DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.cipsrv_index';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.cipsrv_index(
    IN  p_points                         JSONB
   ,IN  p_lines                          JSONB
   ,IN  p_areas                          JSONB
   ,IN  p_geometry                       JSONB 
   ,IN  p_geometry_clip                  VARCHAR[]
   ,IN  p_geometry_clip_stage            VARCHAR
   ,IN  p_catchment_filter               VARCHAR[]
   ,IN  p_nhdplus_version                VARCHAR
   ,IN  p_wbd_version                    VARCHAR
   
   ,IN  p_default_point_indexing_method  VARCHAR
   
   ,IN  p_default_line_indexing_method   VARCHAR
   ,IN  p_default_line_threshold         NUMERIC
   
   ,IN  p_default_ring_indexing_method   VARCHAR
   ,IN  p_default_ring_areacat_threshold NUMERIC
   ,IN  p_default_ring_areaevt_threshold NUMERIC
   
   ,IN  p_default_area_indexing_method   VARCHAR
   ,IN  p_default_areacat_threshold      NUMERIC
   ,IN  p_default_areaevt_threshold      NUMERIC
   
   ,IN  p_known_region                   VARCHAR
   ,IN  p_return_indexed_features        BOOLEAN
   ,IN  p_return_indexed_collection      BOOLEAN
   ,IN  p_return_catchment_geometry      BOOLEAN
   ,IN  p_return_indexing_summary        BOOLEAN
   ,IN  p_return_full_catchments         BOOLEAN
   ,IN  p_limit_to_us_catchments         BOOLEAN
   
   ,OUT out_indexed_points               JSONB
   ,OUT out_indexed_lines                JSONB
   ,OUT out_indexed_areas                JSONB
   ,OUT out_indexed_collection           GEOMETRY
   ,OUT out_indexing_summary             JSONB
   ,OUT out_catchment_count              INTEGER
   ,OUT out_catchment_areasqkm           NUMERIC
   ,OUT out_return_code                  INTEGER
   ,OUT out_status_message               VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                                RECORD;
   int_srid                           INTEGER;
   ary_features                       cipsrv_engine.cip_feature[];   
   boo_return_geometry                BOOLEAN;
   
   str_default_point_indexing_method  VARCHAR;
   
   str_default_line_indexing_method   VARCHAR;
   num_default_line_threshold         NUMERIC;
   
   str_default_ring_indexing_method   VARCHAR;
   num_default_ring_areacat_threshold NUMERIC;
   num_default_ring_areaevt_threshold NUMERIC;
   
   str_default_area_indexing_method   VARCHAR;
   num_default_areacat_threshold      NUMERIC;
   num_default_areaevt_threshold      NUMERIC;
   
   str_known_region                   VARCHAR;
   str_geometry_clip_stage            VARCHAR;
   boo_filter_by_state                BOOLEAN;
   boo_return_indexed_features        BOOLEAN;
   boo_return_indexing_summary        BOOLEAN;
   ary_state_filters                  VARCHAR[];
   boo_filter_by_tribal               BOOLEAN;
   boo_filter_by_notribal             BOOLEAN;
   boo_limit_to_us_catchments         BOOLEAN;
   
   str_nhdplus_version                VARCHAR;
   str_wbd_version                    VARCHAR;
   int_splitselector                  INTEGER;
   str_schema                         VARCHAR;
   str_sql                            VARCHAR;
   
BEGIN

   out_return_code := cipsrv_engine.create_cip_temp_tables();
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  p_points      IS NULL
   AND p_lines       IS NULL
   AND p_areas       IS NULL
   AND p_geometry    IS NULL
   THEN
      out_return_code    := -10;
      out_status_message := 'input geometry cannot be null';
      RETURN;
   
   END IF;
   
   IF p_geometry_clip_stage IS NULL
   THEN
      str_geometry_clip_stage := 'AFTER';
      
   ELSIF UPPER(p_geometry_clip_stage) IN ('BEFORE','AFTER')
   THEN
      str_geometry_clip_stage := UPPER(p_geometry_clip_stage);
   
   END IF;
   
   IF p_return_full_catchments IS NULL
   OR NOT p_return_full_catchments
   THEN
      -- Will return state-split catchments
      int_splitselector := 1;
      
   ELSE
      -- Will return full catchments
      int_splitselector := 2;
      
   END IF;
   
   IF p_limit_to_us_catchments IS NULL
   OR p_limit_to_us_catchments
   THEN
      boo_limit_to_us_catchments := TRUE;
      
   ELSE
      boo_limit_to_us_catchments := FALSE;
   
   END IF;
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.parse_catchment_filter(
      p_catchment_filter := p_catchment_filter
   );
   boo_filter_by_state    := rec.out_filter_by_state;
   ary_state_filters      := rec.out_state_filters;
   boo_filter_by_tribal   := rec.out_filter_by_tribal;
   boo_filter_by_notribal := rec.out_filter_by_notribal;
   
   IF UPPER(p_nhdplus_version) IN ('MR')
   THEN
      str_nhdplus_version := 'nhdplus_m';
   
   ELSIF UPPER(p_nhdplus_version) IN ('HR')
   THEN
      str_nhdplus_version := 'nhdplus_h';
   
   ELSIF p_nhdplus_version IS NULL
   THEN
      out_return_code    := -10;
      out_status_message := 'nhdplus version cannot be null';
      RETURN;
      
   ELSIF p_nhdplus_version NOT IN ('nhdplus_m','nhdplus_h') 
   THEN
      out_return_code    := -10;
      out_status_message := 'invalid nhdplus version';
      RETURN;
   
   ELSE
      str_nhdplus_version := p_nhdplus_version;
   
   END IF;
   
   ----------------------------------------------------------------------------
   str_wbd_version := UPPER(p_wbd_version);
   IF str_wbd_version NOT IN ('NP21','NPHR','F3')
   THEN
      str_wbd_version := NULL;
      
   END IF;
   
   --########################################################################--
   
   str_default_point_indexing_method  := p_default_point_indexing_method;
   IF str_default_point_indexing_method IS NULL
   THEN
      str_default_point_indexing_method := 'point_simple';
      
   ELSIF str_default_point_indexing_method NOT IN ('point_simple')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP point indexing method';
      RETURN;
    
   END IF;
   
   --########################################################################--
   
   str_default_line_indexing_method  := p_default_line_indexing_method;
   IF str_default_line_indexing_method IS NULL
   THEN
      str_default_line_indexing_method := 'line_simple';
      
   ELSIF str_default_line_indexing_method NOT IN ('line_simple','line_levelpath')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP line indexing method';
      RETURN;
    
   END IF;
   
   num_default_line_threshold := p_default_line_threshold;
   IF num_default_line_threshold IS NULL
   THEN
      num_default_line_threshold := 10;
      
   END IF;

   --########################################################################--
   
   str_default_ring_indexing_method  := p_default_ring_indexing_method;
   IF str_default_ring_indexing_method IS NULL
   THEN
      str_default_ring_indexing_method := 'area_simple';
      
   ELSIF str_default_ring_indexing_method NOT IN ('treat_as_lines','area_simple','area_centroid','area_artpath')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP ring indexing method';
      RETURN;
    
   END IF;
   
   num_default_ring_areacat_threshold := p_default_ring_areacat_threshold;
   IF num_default_ring_areacat_threshold IS NULL
   THEN
      num_default_ring_areacat_threshold := 10;
      
   END IF;
   
   num_default_ring_areaevt_threshold := p_default_ring_areaevt_threshold;
   IF num_default_ring_areaevt_threshold IS NULL
   THEN
      num_default_ring_areaevt_threshold := 10;
      
   END IF;
   
   --########################################################################--
   
   str_default_area_indexing_method  := p_default_area_indexing_method;
   IF str_default_area_indexing_method IS NULL
   THEN
      str_default_area_indexing_method := 'area_simple';
      
   ELSIF str_default_area_indexing_method NOT IN ('area_simple','area_centroid','area_artpath')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP area indexing method';
      RETURN;
    
   END IF;
   
   num_default_areacat_threshold := p_default_areacat_threshold;
   IF num_default_areacat_threshold IS NULL
   THEN
      num_default_areacat_threshold := 10;
      
   END IF;
   
   num_default_areaevt_threshold := p_default_areaevt_threshold;
   IF num_default_areaevt_threshold IS NULL
   THEN
      num_default_areaevt_threshold := 10;
      
   END IF;
   
   --########################################################################--
   
   boo_return_indexed_features := p_return_indexed_features;
   IF boo_return_indexed_features IS NULL
   THEN
      boo_return_indexed_features := TRUE;
      
   END IF;
   
   boo_return_indexing_summary := p_return_indexing_summary;
   IF boo_return_indexing_summary IS NULL
   THEN
      boo_return_indexing_summary := TRUE;
      
   END IF;

   boo_return_geometry := p_return_catchment_geometry;
   IF boo_return_geometry IS NULL
   THEN
      boo_return_geometry := FALSE;
      
   END IF;

   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   str_known_region := p_known_region;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Build features from JSONB inputs
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.unpackjsonb(
       p_points                         := p_points
      ,p_lines                          := p_lines
      ,p_areas                          := p_areas
      ,p_geometry                       := p_geometry 
      ,p_nhdplus_version                := str_nhdplus_version
      ,p_known_region                   := str_known_region
      ,p_int_srid                       := NULL
      
      ,p_default_point_indexing_method  := str_default_point_indexing_method
      
      ,p_default_line_indexing_method   := str_default_line_indexing_method
      ,p_default_line_threshold         := num_default_line_threshold
      
      ,p_default_ring_indexing_method   := str_default_ring_indexing_method
      ,p_default_ring_areacat_threshold := num_default_ring_areacat_threshold
      ,p_default_ring_areaevt_threshold := num_default_ring_areaevt_threshold
      
      ,p_default_area_indexing_method   := str_default_area_indexing_method
      ,p_default_areacat_threshold      := num_default_areacat_threshold
      ,p_default_areaevt_threshold      := num_default_areaevt_threshold
   );
   out_return_code     := rec.out_return_code;
   out_status_message  := rec.out_status_message;
   ary_features        := rec.out_features;
   str_known_region    := rec.out_known_region;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- Harvest summary data before processing
   ----------------------------------------------------------------------------
   IF boo_return_indexing_summary
   THEN
      out_indexing_summary := cipsrv_engine.preprocess2summary(
         p_features          := ary_features
      );

   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Before Clip over each feature
   ----------------------------------------------------------------------------
   IF str_geometry_clip_stage = 'BEFORE'
   AND p_geometry_clip IS NOT NULL
   AND array_length(p_geometry_clip,1) > 0
   THEN
      rec := cipsrv_engine.feature_clip(
          p_features           := ary_features
         ,p_clippers           := p_geometry_clip
         ,p_known_region       := str_known_region
      );
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      ary_features       := rec.out_features;

   END IF;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Loop over and index each feature
   ----------------------------------------------------------------------------
   IF array_length(ary_features,1) > 0
   THEN
      FOR i IN 1 .. array_length(ary_features,1)
      LOOP
         IF (ary_features[i]).gtype IN ('ST_Point','ST_MultiPoint')
         AND (ary_features[i]).point_indexing_method = 'point_simple'
         THEN
            ary_features[i].indexing_method_used := 'point_simple';
            
            IF str_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_point_simple(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_known_region           := str_known_region
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
                        
            ELSIF str_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_point_simple(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_known_region           := str_known_region
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
         ELSIF (ary_features[i]).gtype IN ('ST_LineString','ST_MultiLineString')
         AND (ary_features[i]).line_indexing_method = 'line_simple'
         THEN
            ary_features[i].indexing_method_used := 'line_simple';
            
            IF str_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_line_simple(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_lengthkm      := (ary_features[i]).lengthkm
                  ,p_known_region           := str_known_region
                  ,p_line_threshold_perc    := (ary_features[i]).line_threshold
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            ELSIF str_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_line_simple(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_lengthkm      := (ary_features[i]).lengthkm
                  ,p_known_region           := str_known_region
                  ,p_line_threshold_perc    := (ary_features[i]).line_threshold
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
            ary_features[i].line_threshold_used := (ary_features[i]).line_threshold;
            
         ELSIF (ary_features[i]).gtype IN ('ST_LineString','ST_MultiLineString')
         AND (ary_features[i]).line_indexing_method = 'line_levelpath'
         THEN
            ary_features[i].indexing_method_used := 'line_levelpath';
            
            IF str_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_line_levelpath(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_lengthkm      := (ary_features[i]).lengthkm
                  ,p_known_region           := str_known_region
                  ,p_line_threshold_perc    := (ary_features[i]).line_threshold
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            ELSIF str_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_line_levelpath(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_lengthkm      := (ary_features[i]).lengthkm
                  ,p_known_region           := str_known_region
                  ,p_line_threshold_perc    := (ary_features[i]).line_threshold
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
            ary_features[i].line_threshold_used := (ary_features[i]).line_threshold;
            
         ELSIF (ary_features[i]).gtype IN ('ST_Polygon','ST_MultiPolygon')
         AND (ary_features[i]).area_indexing_method = 'area_artpath'
         THEN
            ary_features[i].indexing_method_used := 'area_artpath';
            
            IF str_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_area_artpath(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_areasqkm      := (ary_features[i]).areasqkm
                  ,p_known_region           := str_known_region
                  ,p_cat_threshold_perc     := (ary_features[i]).areacat_threshold
                  ,p_evt_threshold_perc     := (ary_features[i]).areaevt_threshold
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
            
            ELSIF str_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_area_artpath(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_areasqkm      := (ary_features[i]).areasqkm
                  ,p_known_region           := str_known_region
                  ,p_cat_threshold_perc     := (ary_features[i]).areacat_threshold
                  ,p_evt_threshold_perc     := (ary_features[i]).areaevt_threshold
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
            ary_features[i].areacat_threshold_used := (ary_features[i]).areacat_threshold;
            ary_features[i].areaevt_threshold_used := (ary_features[i]).areaevt_threshold;
            
         ELSIF (ary_features[i]).gtype IN ('ST_Polygon','ST_MultiPolygon')
         AND (ary_features[i]).area_indexing_method = 'area_simple'
         THEN
            ary_features[i].indexing_method_used := 'area_simple';
            
            IF str_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_area_simple(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_areasqkm      := (ary_features[i]).areasqkm
                  ,p_known_region           := str_known_region
                  ,p_cat_threshold_perc     := (ary_features[i]).areacat_threshold
                  ,p_evt_threshold_perc     := (ary_features[i]).areaevt_threshold
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;

            ELSIF str_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_area_simple(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_areasqkm      := (ary_features[i]).areasqkm
                  ,p_known_region           := str_known_region
                  ,p_cat_threshold_perc     := (ary_features[i]).areacat_threshold
                  ,p_evt_threshold_perc     := (ary_features[i]).areaevt_threshold
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
            ary_features[i].areacat_threshold_used := (ary_features[i]).areacat_threshold;
            ary_features[i].areaevt_threshold_used := (ary_features[i]).areaevt_threshold;
            
         ELSIF (ary_features[i]).gtype IN ('ST_Polygon','ST_MultiPolygon')
         AND (ary_features[i]).area_indexing_method = 'area_centroid'
         THEN
            ary_features[i].indexing_method_used := 'area_centroid';
            
            IF str_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_area_centroid(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_areasqkm      := (ary_features[i]).areasqkm
                  ,p_known_region           := str_known_region
                  ,p_cat_threshold_perc     := (ary_features[i]).areacat_threshold
                  ,p_evt_threshold_perc     := (ary_features[i]).areaevt_threshold
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            ELSIF str_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_area_centroid(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_areasqkm      := (ary_features[i]).areasqkm
                  ,p_known_region           := str_known_region
                  ,p_cat_threshold_perc     := (ary_features[i]).areacat_threshold
                  ,p_evt_threshold_perc     := (ary_features[i]).areaevt_threshold
                  ,p_permid_joinkey         := NULL
                  ,p_permid_geometry        := (ary_features[i]).geometry
                  ,p_statesplit             := int_splitselector
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
            ary_features[i].areacat_threshold_used := (ary_features[i]).areacat_threshold;
            ary_features[i].areaevt_threshold_used := (ary_features[i]).areaevt_threshold;
         
         ELSE
            RAISE EXCEPTION 'err %',(ary_features[i]).gtype;
            
         END IF;

         IF out_return_code != 0
         THEN
            RETURN;
            
         END IF;      
         
      END LOOP;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Clip AFTER indexing if requested
   ----------------------------------------------------------------------------
   IF array_length(ary_features,1) > 0
   THEN
      IF str_geometry_clip_stage = 'AFTER'
      AND p_geometry_clip IS NOT NULL
      AND array_length(p_geometry_clip,1) > 0
      THEN
         rec := cipsrv_engine.feature_clip(
             p_features           := ary_features
            ,p_clippers           := p_geometry_clip
            ,p_known_region       := str_known_region
         );
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         ary_features       := rec.out_features;
      
      END IF;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- Return catchment results
   ----------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_h'
   THEN
      str_schema := 'h';
      
   ELSIF str_nhdplus_version = 'nhdplus_m'
   THEN
      str_schema := 'm';
      
   END IF;
   
   str_sql := '
      INSERT INTO tmp_cip_out(
          nhdplusid
         ,catchmentstatecode
         ,xwalk_huc12
         ,areasqkm
         ,istribal
         ,istribal_areasqkm
         ,isnavigable
         ,shape
      )
      SELECT
       a.nhdplusid
      ,CASE 
       WHEN $1 = 2
       THEN
         NULL
       ELSE
         a.catchmentstatecodes[1]
       END AS catchmentstatecode 
    ';
       
    IF str_wbd_version IS NOT NULL
    THEN
       str_sql := str_sql || ',b.xwalk_huc12 ';
       
    ELSE
       str_sql := str_sql || ',CAST(NULL AS VARCHAR(12)) AS xwalk_huc12 ';
    
    END IF;
    
    str_sql := str_sql || '
      ,a.areasqkm
      ,a.istribal
      ,a.istribal_areasqkm
      ,CASE
       WHEN a.isnavigable
       THEN
         ''Y''
       ELSE
         ''N''
       END AS isnavigable
      ,CASE
       WHEN $2
       THEN
         ST_TRANSFORM(a.shape,4269)
       ELSE
         CAST(NULL AS GEOMETRY)       
       END AS shape
      FROM
      cipsrv_nhdplus_' || str_schema || '.catchment_' || str_known_region || ' a ';

   IF str_wbd_version IS NOT NULL
   THEN
      str_sql := str_sql || '
      LEFT JOIN (
         SELECT
          bb.nhdplusid
         ,bb.xwalk_huc12
         FROM
         cipsrv_epageofab_' || str_schema || '.catchment_fabric_xwalk bb
         WHERE
         bb.xwalk_huc12_version = ''' || str_wbd_version || '''
      ) b
      ON
      a.nhdplusid = b.nhdplusid 
      ';

   END IF;
   
   str_sql := str_sql || '
      WHERE
          EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
      AND a.statesplit IN (0,$3) 
   ';
   
   IF boo_filter_by_state
   THEN
      str_sql := str_sql || ' AND a.catchmentstatecodes && ary_state_filters ';
      
   END IF;
   
   IF boo_filter_by_tribal
   THEN
      str_sql := str_sql || ' AND a.istribal IN (''F'',''P'') ';
      
   END IF;
   
   IF boo_filter_by_notribal
   THEN
      str_sql := str_sql || ' AND a.istribal = ''N'' ';
      
   END IF;
   
   IF boo_limit_to_us_catchments
   THEN
      str_sql := str_sql || ' AND a.border_status IN (''I'',''B'') ';
      
      IF int_splitselector = 1
      THEN
         str_sql := str_sql || ' AND a.catchmentstatecodes[1] NOT IN (''MX'',''CN'',''OW'') ';
         
      END IF;
      
   END IF;
   
   --RAISE WARNING 'check %',str_sql;
   
   EXECUTE str_sql
   USING 
    int_splitselector
   ,boo_return_geometry
   ,int_splitselector;
   
   SELECT
    COUNT(*)
   ,SUM(a.areasqkm)
   INTO
    out_catchment_count
   ,out_catchment_areasqkm
   FROM
   tmp_cip_out a;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Build the output results
   ----------------------------------------------------------------------------
   IF array_length(ary_features,1) > 0
   THEN
      IF boo_return_indexed_features
      THEN
         out_indexed_points := cipsrv_engine.features2jsonb(
             p_features         := ary_features
            ,p_geometry_type    := 'P'
            ,p_empty_collection := FALSE
         );
         
         out_indexed_lines := cipsrv_engine.features2jsonb(
             p_features         := ary_features
            ,p_geometry_type    := 'L'
            ,p_empty_collection := FALSE
         );
         
         out_indexed_areas := cipsrv_engine.features2jsonb(
             p_features         := ary_features
            ,p_geometry_type    := 'A'
            ,p_empty_collection := FALSE
         );
      
      END IF;
      
      IF p_return_indexed_collection
      THEN
         out_indexed_collection := cipsrv_engine.features2geomcollection(
            p_features          := ary_features
         );
      
      END IF;
      
   END IF;  
   
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.cipsrv_index';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;
