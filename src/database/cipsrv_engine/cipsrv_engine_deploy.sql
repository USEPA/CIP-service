--******************************--
----- types/cip_feature.sql 

DROP TYPE IF EXISTS cipsrv_engine.cip_feature CASCADE;

CREATE TYPE cipsrv_engine.cip_feature
AS (
    globalid                    VARCHAR
   ,gtype                       VARCHAR
   ,geometry                    GEOMETRY
   ,lengthkm                    NUMERIC
   ,areasqkm                    NUMERIC
   ,isring                      BOOLEAN
   ,properties                  JSONB
   ,source_featureid            VARCHAR
   ,permid_joinkey              VARCHAR
   ,nhdplus_version             VARCHAR
   ,known_region                VARCHAR
   ,int_srid                    INTEGER
   ,indexing_method_used        VARCHAR
   ,converted_to_ring           BOOLEAN
   
   ,point_indexing_method       VARCHAR
   
   ,line_indexing_method        VARCHAR
   ,line_threshold              NUMERIC
   ,line_threshold_used         NUMERIC
   
   ,ring_indexing_method        VARCHAR
   ,ring_areacat_threshold      NUMERIC
   ,ring_areacat_threshold_used NUMERIC
   ,ring_areaevt_threshold      NUMERIC
   ,ring_areaevt_threshold_used NUMERIC
   
   ,area_indexing_method        VARCHAR
   ,areacat_threshold           NUMERIC
   ,areacat_threshold_used      NUMERIC
   ,areaevt_threshold           NUMERIC
   ,areaevt_threshold_used      NUMERIC
);

GRANT USAGE ON TYPE cipsrv_engine.cip_feature TO public;

--******************************--
----- functions/adjust_point_extent.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.adjust_point_extent';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.adjust_point_extent(
    IN  p_extent_value      NUMERIC
   ,IN  p_direction         VARCHAR
   ,IN  p_flowline_amount   NUMERIC
   ,IN  p_flowline_fmeasure NUMERIC
   ,IN  p_flowline_tmeasure NUMERIC
   ,IN  p_event_measure     NUMERIC
) RETURNS NUMERIC 
IMMUTABLE 
AS $BODY$
DECLARE
   str_direction VARCHAR := UPPER(p_direction);
   num_amount    NUMERIC;

BEGIN
   
   IF str_direction IN ('UT','UM','U')
   THEN
      IF p_flowline_tmeasure = p_event_measure
      THEN
         RETURN p_extent_value;

      END IF;
      
      num_amount := p_flowline_amount * ((p_flowline_tmeasure - p_event_measure) / (p_flowline_tmeasure - p_flowline_fmeasure));
      
   ELSIF str_direction IN ('DM','DD','PP','PPALL','D')
   THEN
      IF p_flowline_fmeasure = p_event_measure
      THEN
         RETURN p_extent_value;

      END IF;
      
      num_amount := p_flowline_amount * ((p_event_measure - p_flowline_fmeasure) / (p_flowline_tmeasure - p_flowline_fmeasure));
      
   ELSE
      RAISE EXCEPTION 'err direction %',str_direction;
      
   END IF;
   
   RETURN p_extent_value - num_amount;

END;
$BODY$ LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.adjust_point_extent(
    NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.adjust_point_extent(
    NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/cipsrv_index.sql 

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
   ,IN  p_return_full_catchment          BOOLEAN
   
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
   
   str_nhdplus_version                VARCHAR;
   str_wbd_version                    VARCHAR;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  p_points    IS NULL
   AND p_lines     IS NULL
   AND p_areas     IS NULL
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
   
   str_known_region := p_known_region;
   out_return_code := cipsrv_engine.create_cip_temp_tables();
   
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
                  ,p_return_full_catchment  := p_return_full_catchment
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
   -- Return filtered catchment results
   ----------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      IF str_wbd_version IS NULL
      THEN
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
         ,a.catchmentstatecode
         ,CAST(NULL AS VARCHAR(12)) AS xwalk_huc12
         ,a.areasqkm
         ,a.istribal
         ,a.istribal_areasqkm
         ,a.isnavigable
         ,CASE
          WHEN boo_return_geometry
          THEN
            a.shape
          ELSE
            CAST(NULL AS GEOMETRY)       
          END AS shape
         FROM
         cipsrv_epageofab_m.catchment_fabric a
         WHERE
         EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
         AND (NOT boo_filter_by_state    OR a.catchmentstatecode = ANY(ary_state_filters) )
         AND (NOT boo_filter_by_tribal   OR a.istribal IN ('F','P'))
         AND (NOT boo_filter_by_notribal OR a.istribal = 'N');
         
      ELSE
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
         ,a.catchmentstatecode
         ,b.xwalk_huc12
         ,a.areasqkm
         ,a.istribal
         ,a.istribal_areasqkm
         ,a.isnavigable
         ,CASE
          WHEN boo_return_geometry
          THEN
            a.shape
          ELSE
            CAST(NULL AS GEOMETRY)       
          END AS shape
         FROM
         cipsrv_epageofab_m.catchment_fabric a
         LEFT JOIN (
            SELECT
             bb.nhdplusid
            ,bb.xwalk_huc12
            FROM
            cipsrv_epageofab_m.catchment_fabric_xwalk bb
            WHERE
            bb.xwalk_huc12_version = str_wbd_version
         ) b
		   ON
         a.nhdplusid = b.nhdplusid         
         WHERE
         EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
         AND (NOT boo_filter_by_state    OR a.catchmentstatecode = ANY(ary_state_filters) )
         AND (NOT boo_filter_by_tribal   OR a.istribal IN ('F','P'))
         AND (NOT boo_filter_by_notribal OR a.istribal = 'N');
      
      END IF;
   
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      IF str_wbd_version IS NULL
      THEN
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
         ,a.catchmentstatecode
         ,CAST(NULL AS VARCHAR(12)) AS xwalk_huc12
         ,a.areasqkm
         ,a.istribal
         ,a.istribal_areasqkm
         ,a.isnavigable
         ,CASE
          WHEN boo_return_geometry
          THEN
            a.shape
          ELSE
            CAST(NULL AS GEOMETRY)       
          END AS shape
         FROM
         cipsrv_epageofab_h.catchment_fabric a
         WHERE
         EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
         AND (NOT boo_filter_by_state    OR a.catchmentstatecode = ANY(ary_state_filters) )
         AND (NOT boo_filter_by_tribal   OR a.istribal IN ('F','P'))
         AND (NOT boo_filter_by_notribal OR a.istribal = 'N');
         
      ELSE
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
         ,a.catchmentstatecode
         ,b.xwalk_huc12
         ,a.areasqkm
         ,a.istribal
         ,a.istribal_areasqkm
         ,a.isnavigable
         ,CASE
          WHEN boo_return_geometry
          THEN
            a.shape
          ELSE
            CAST(NULL AS GEOMETRY)       
          END AS shape
         FROM
         cipsrv_epageofab_h.catchment_fabric a
         LEFT JOIN (
            SELECT
             bb.nhdplusid
            ,bb.xwalk_huc12
            FROM
            cipsrv_epageofab_h.catchment_fabric_xwalk bb
            WHERE
            bb.xwalk_huc12_version = str_wbd_version
         ) b
         ON
         a.nhdplusid = b.nhdplusid 
         WHERE
         EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
         AND (NOT boo_filter_by_state    OR a.catchmentstatecode = ANY(ary_state_filters) )
         AND (NOT boo_filter_by_tribal   OR a.istribal IN ('F','P'))
         AND (NOT boo_filter_by_notribal OR a.istribal = 'N');
      
      END IF;
   
   ELSE
      RAISE EXCEPTION 'err';
   
   END IF;
   
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
--******************************--
----- functions/cipsrv_version.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.cipsrv_version';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.cipsrv_version()
RETURNS VARCHAR
STABLE
AS $BODY$
DECLARE
BEGIN

   RETURN '1.0';

END;
$BODY$ LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.cipsrv_version()
OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.cipsrv_version()
TO PUBLIC;

--******************************--
----- functions/clean_geometry.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.clean_geometry';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.clean_geometry(
    IN  p_geometry          GEOMETRY
   ,IN  p_colletiontype     INTEGER DEFAULT NULL
) RETURNS GEOMETRY
IMMUTABLE
AS $BODY$
DECLARE
   str_geometry VARCHAR;
   int_numgeoms INTEGER;
   rez          GEOMETRY;
   
BEGIN

   IF p_geometry IS NULL
   OR ST_ISEMPTY(p_geometry)
   THEN
      RETURN NULL;
      
   END IF;
   
   str_geometry := ST_GeometryType(p_geometry);
   int_numgeoms := ST_NumGeometries(p_geometry);
   
   IF  str_geometry IN ('ST_MultiPoint','ST_MultiLineString','ST_MultiPolygon') 
   AND int_numgeoms = 1
   THEN
      RETURN ST_GeometryN(p_geometry,1);
   
   ELSIF  str_geometry = 'ST_GeometryCollection' 
   AND p_colletiontype IS NOT NULL
   THEN
      rez := ST_COLLECTIONEXTRACT(p_geometry,p_colletiontype);
      
      str_geometry := ST_GeometryType(rez);
      int_numgeoms := ST_NumGeometries(rez);
      
      IF  str_geometry IN ('ST_MultiPoint','ST_MultiLineString','ST_MultiPolygon') 
      AND int_numgeoms = 1
      THEN
         RETURN ST_GeometryN(rez,1);
         
      ELSE
         RETURN rez;
         
      END IF;
      
   ELSE
      RETURN p_geometry;
   
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.clean_geometry';
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
--******************************--
----- functions/column_has_single_index.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.column_has_single_index';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.column_has_single_index(
    IN  p_schema_name  VARCHAR
   ,IN  p_table_name   VARCHAR
   ,IN  p_column_name  VARCHAR
   ,IN  p_unique       BOOLEAN DEFAULT FALSE
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   boo_results BOOLEAN;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for table
   ----------------------------------------------------------------------------
   SELECT
   TRUE
   INTO boo_results
   FROM 
   pg_class t
   JOIN
   pg_namespace ns
   ON
   t.relnamespace = ns.oid
   JOIN
   pg_index ix
   ON
   t.oid = ix.indrelid
   JOIN
   pg_class i
   ON
   i.oid = ix.indexrelid
   JOIN
   pg_attribute a
   ON
       a.attrelid = t.oid
   AND a.attnum = ANY(ix.indkey)
   WHERE 
       t.relkind  = 'r'
   AND ns.nspname = p_schema_name
   AND t.relname  = p_table_name
   AND attname    = p_column_name
   AND (NOT p_unique OR ix.indisunique)
   GROUP BY
    a.attname
   ,i.relname
   HAVING 
   COUNT(*) = 1;

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   RETURN COALESCE(boo_results,FALSE);

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.column_has_single_index(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.column_has_single_index(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
) TO PUBLIC;

--******************************--
----- functions/create_cip_batch_temp_tables.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.create_cip_batch_temp_tables()
RETURNS INT4
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_cip temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_cip')
   THEN
      TRUNCATE TABLE tmp_cip;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_cip(
          permid_joinkey       UUID
         ,catchmentstatecodes  VARCHAR[]
         ,nhdplusid            BIGINT    NOT NULL
         ,overlap_measure      NUMERIC
      );

      CREATE UNIQUE INDEX tmp_cip_pk
      ON tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
      ) NULLS NOT DISTINCT;
      
      CREATE INDEX tmp_cip_i01
      ON tmp_cip(
         permid_joinkey
      );
      
      CREATE INDEX tmp_cip_i02
      ON tmp_cip(
         nhdplusid
      );

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_src2cip temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_src2cip')
   THEN
      TRUNCATE TABLE tmp_src2cip;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_src2cip(
          permid_joinkey       UUID
         ,nhdplusid            BIGINT
         ,cip_method           VARCHAR(255)
         ,cip_parms            VARCHAR(255)
         ,overlap_measure      NUMERIC
      );

      CREATE UNIQUE INDEX tmp_src2cip_pk 
      ON tmp_src2cip(
          permid_joinkey
         ,nhdplusid
      );

   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.create_cip_batch_temp_tables() OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.create_cip_batch_temp_tables() TO PUBLIC;

--******************************--
----- functions/create_cip_temp_tables.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.create_cip_temp_tables()
RETURNS INT4
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_cip temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_cip')
   THEN
      TRUNCATE TABLE tmp_cip;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_cip(
          permid_joinkey       UUID
         ,catchmentstatecodes  VARCHAR[]
         ,nhdplusid            BIGINT    NOT NULL
         ,overlap_measure      NUMERIC
      );

      CREATE UNIQUE INDEX tmp_cip_pk
      ON tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
      ) NULLS NOT DISTINCT;
      
      CREATE INDEX tmp_cip_i01
      ON tmp_cip(
         permid_joinkey
      );
      
      CREATE INDEX tmp_cip_i02
      ON tmp_cip(
         nhdplusid
      );

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_cip_out temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_cip_out')
   THEN
      TRUNCATE TABLE tmp_cip_out;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_cip_out(
          nhdplusid            BIGINT      NOT NULL
         ,catchmentstatecode   VARCHAR(2)  NOT NULL
         ,xwalk_huc12          VARCHAR(12)
         ,areasqkm             NUMERIC
         ,istribal             VARCHAR(1)  NOT NULL
         ,istribal_areasqkm    NUMERIC
         ,isnavigable          VARCHAR(1)  NOT NULL
         ,shape                GEOMETRY
      );

      CREATE UNIQUE INDEX tmp_cip_out_pk 
      ON tmp_cip_out(catchmentstatecode,nhdplusid);
      
      CREATE INDEX tmp_cip_out_01i
      ON tmp_cip_out(nhdplusid);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.create_cip_temp_tables() OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.create_cip_temp_tables() TO PUBLIC;

--******************************--
----- functions/create_delineation_temp_tables.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.create_delineation_temp_tables()
RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_catchments temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_catchments')
   THEN
      TRUNCATE TABLE tmp_catchments;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_catchments(
          nhdplusid                   BIGINT
         ,sourcefc                    VARCHAR
         ,areasqkm                    NUMERIC
         ,hydroseq                    BIGINT
         ,orderingkey                 NUMERIC
         ,shape                       GEOMETRY
         ,shape_3338                  GEOMETRY
         ,shape_5070                  GEOMETRY
         ,shape_26904                 GEOMETRY
         ,shape_32161                 GEOMETRY
         ,shape_32655                 GEOMETRY
         ,shape_32702                 GEOMETRY
      );

      CREATE UNIQUE INDEX tmp_catchments_pk
      ON tmp_catchments(nhdplusid);
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.create_delineation_temp_tables()
OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.create_delineation_temp_tables()
TO PUBLIC;
--******************************--
----- functions/create_line_temp_tables.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.create_line_temp_tables()
RETURNS INT4
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_line temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_line')
   THEN
      TRUNCATE TABLE tmp_line;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_line(
          nhdplusid               BIGINT
         ,areasqkm                NUMERIC
         ,overlapmeasure          NUMERIC
         ,eventpercentage         NUMERIC
         ,nhdpercentage           NUMERIC
         ,hydroseq                BIGINT
         ,levelpathi              BIGINT
         ,fromnode                BIGINT
         ,tonode                  BIGINT
         ,connector_fromnode      BIGINT
         ,connector_tonode        BIGINT
         ,fcode                   INTEGER
         ,isnavigable             BOOLEAN
      );

      CREATE UNIQUE INDEX tmp_line_pk 
      ON tmp_line(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_line_pk2
      ON tmp_line(hydroseq);
      
      CREATE INDEX tmp_line_01i
      ON tmp_line(levelpathi);
      
      CREATE INDEX tmp_line_02i
      ON tmp_line(fcode);
      
      CREATE INDEX tmp_line_03i
      ON tmp_line(isnavigable);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_line_dd temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_line_levelpathi')
   THEN
      TRUNCATE TABLE tmp_line_levelpathi;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_line_levelpathi(
          levelpathi              BIGINT
         ,max_hydroseq            BIGINT
         ,min_hydroseq            BIGINT
         ,totaleventpercentage    NUMERIC
         ,totaloverlapmeasure     NUMERIC
         ,levelpathilengthkm      NUMERIC
         ,fromnode                BIGINT
         ,tonode                  BIGINT
         ,connector_fromnode      BIGINT
         ,connector_tonode        BIGINT
      );

      CREATE UNIQUE INDEX tmp_line_levelpathi_pk 
      ON tmp_line_levelpathi(levelpathi);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.create_line_temp_tables() 
OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.create_line_temp_tables() 
TO PUBLIC;

--******************************--
----- functions/create_navigation_temp_tables.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.create_navigation_temp_tables()
RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_navigation_working30 temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_navigation_working30')
   THEN
      TRUNCATE TABLE tmp_navigation_working30;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_navigation_working30(
          nhdplusid                   BIGINT
         ,hydroseq                    BIGINT
         ,fmeasure                    NUMERIC
         ,tmeasure                    NUMERIC
         ,lengthkm                    NUMERIC
         ,flowtimeday                 NUMERIC
         ,network_distancekm          NUMERIC
         ,network_flowtimeday         NUMERIC
         ,levelpathi                  BIGINT
         ,terminalpa                  BIGINT
         ,uphydroseq                  BIGINT
         ,dnhydroseq                  BIGINT
         ,navtermination_flag         INTEGER
         ,nav_order                   INTEGER
         ,selected                    BOOLEAN
      );

      CREATE UNIQUE INDEX tmp_navigation_working30_pk
      ON tmp_navigation_working30(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_navigation_working30_1u
      ON tmp_navigation_working30(hydroseq);
      
      CREATE INDEX tmp_navigation_working30_01i
      ON tmp_navigation_working30(network_distancekm);
            
      CREATE INDEX tmp_navigation_working30_02i
      ON tmp_navigation_working30(network_flowtimeday);
      
      CREATE INDEX tmp_navigation_working30_03i
      ON tmp_navigation_working30(dnhydroseq);
      
      CREATE INDEX tmp_navigation_working30_04i
      ON tmp_navigation_working30(nav_order);
      
      CREATE INDEX tmp_navigation_working30_05i
      ON tmp_navigation_working30(selected);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_navigation_results temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_navigation_results')
   THEN
      TRUNCATE TABLE tmp_navigation_results;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_navigation_results(
          nhdplusid                   BIGINT
         ,hydroseq                    BIGINT
         ,fmeasure                    NUMERIC
         ,tmeasure                    NUMERIC
         ,levelpathi                  BIGINT
         ,terminalpa                  BIGINT
         ,uphydroseq                  BIGINT
         ,dnhydroseq                  BIGINT
         ,lengthkm                    NUMERIC
         ,flowtimeday                 NUMERIC
         /* ++++++++++ */
         ,network_distancekm          NUMERIC
         ,network_flowtimeday         NUMERIC
         /* ++++++++++ */
         ,permanent_identifier        VARCHAR(40)
         ,reachcode                   VARCHAR(14)
         ,fcode                       INTEGER
         ,gnis_id                     VARCHAR(10)
         ,gnis_name                   VARCHAR(65)
         ,wbarea_permanent_identifier VARCHAR(40)
         /* ++++++++++ */
         ,quality_marker              INTEGER
         ,navtermination_flag         INTEGER
         ,shape                       GEOMETRY
         ,nav_order                   INTEGER
      );

      CREATE UNIQUE INDEX tmp_navigation_results_pk
      ON tmp_navigation_results(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_navigation_results_1u
      ON tmp_navigation_results(hydroseq);
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.create_navigation_temp_tables() 
OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.create_navigation_temp_tables() 
TO PUBLIC;

--******************************--
----- functions/deepest_cell.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.deepest_cell';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.deepest_cell(
    IN  p_input               GEOMETRY
   ,IN  p_FDR                 RASTER
   ,IN  p_FAC                 RASTER 
   ,OUT out_columnX           INTEGER
   ,OUT out_rowY              INTEGER
)
IMMUTABLE
AS
$BODY$ 
DECLARE
   rec               RECORD;
   sdo_input         GEOMETRY := p_input;
   int_column_x_fdr  INTEGER;
   int_row_y_fdr     INTEGER;
   int_column_x_fac  INTEGER;
   int_row_y_fac     INTEGER;
   int_orig_column_x INTEGER;
   int_orig_row_y    INTEGER;
   mat_values_fdr    INTEGER[][];
   mat_values_fac    INTEGER[][];
   int_largest_accum INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF sdo_input IS NULL
   THEN
      RAISE EXCEPTION 'input point cannot by null';
      
   END IF;
   
   IF p_FDR IS NULL
   OR p_FAC IS NULL
   THEN
      RAISE EXCEPTION 'FDR and FAC rasters required';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Force inputs to match
   ----------------------------------------------------------------------------
   IF ST_SRID(sdo_input) <> ST_SRID(p_FDR)
   THEN
      sdo_input := ST_Transform(sdo_input,ST_SRID(p_FDR));
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Get initial start columnx and rowy from raster fdr
   ----------------------------------------------------------------------------
   rec := ST_WorldToRasterCoord(
       p_FDR
      ,sdo_input
   );
   int_column_x_fdr := rec.columnx;
   int_row_y_fdr    := rec.rowy;
   --RAISE WARNING '% %', int_column_x_fdr, int_row_y_fdr;
   --RAISE WARNING '%',st_astext(st_transform(ST_PixelAsCentroid(p_FDR,int_column_x,int_row_y),4269));

   ----------------------------------------------------------------------------
   -- Step 40
   -- Get initial start columnx and rowy from raster fac
   ----------------------------------------------------------------------------
   rec := ST_WorldToRasterCoord(
       p_FAC
      ,sdo_input
   );
   int_column_x_fac := rec.columnx;
   int_row_y_fac    := rec.rowy;
   --RAISE WARNING '% %', int_column_x_fac, int_row_y_fac;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Pull the fdr 9 cell matrix around this top
   --------------------------------------------------------------------------
   mat_values_fdr := ST_Neighborhood(
       p_FDR
      ,1
      ,int_column_x_fdr
      ,int_row_y_fdr
      ,1
      ,1
   );
   --RAISE WARNING '%', mat_values_fdr;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Pull the fac 9 cell matrix around this top
   --------------------------------------------------------------------------
   mat_values_fac := ST_Neighborhood(
       p_FAC
      ,1
      ,int_column_x_fac
      ,int_row_y_fac
      ,1
      ,1
   );
   --RAISE WARNING '%', mat_values_fac;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Decide whether to move over into neighboring cell that is deeper
   --------------------------------------------------------------------------
   int_orig_column_x := int_column_x_fdr;
   int_orig_row_y    := int_row_y_fdr;
   int_largest_accum := mat_values_fac[2][2];
   
   -- 1,1
   IF  mat_values_fac[1][1] > int_largest_accum
   AND mat_values_fdr[2][2] <> 32
   THEN
      IF mat_values_fdr[2][1] <> 64
      AND mat_values_fdr[1][2] <> 16
      THEN
         int_largest_accum := mat_values_fac[1][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[2][1] = 64
      AND   mat_values_fac[2][1] + 7 < mat_values_fac[1][1]
      THEN
         int_largest_accum := mat_values_fac[1][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[1][2] = 16
      AND   mat_values_fac[1][2] + 7 < mat_values_fac[1][1]
      THEN
         int_largest_accum := mat_values_fac[1][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y - 1;
         
      END IF;
      
   END IF;
   
   -- 1,2
   IF  mat_values_fac[1][2] > int_largest_accum
   AND mat_values_fdr[2][2] <> 64
   THEN
      IF mat_values_fdr[1][1] <> 1
      AND mat_values_fdr[1][3] <> 16
      THEN
         int_largest_accum := mat_values_fac[1][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[1][1] = 1
      AND   mat_values_fac[1][1] + 7 < mat_values_fac[1][2]
      THEN
         int_largest_accum := mat_values_fac[1][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[1][3] = 16
      AND   mat_values_fac[1][3] + 7 < mat_values_fac[1][2]
      THEN
         int_largest_accum := mat_values_fac[1][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y - 1;
         
      END IF;
      
   END IF;
   
   -- 1,3
   IF  mat_values_fac[1][3] > int_largest_accum
   AND mat_values_fdr[2][2] <> 128
   THEN
      IF mat_values_fdr[1][2] <> 1
      AND mat_values_fdr[2][3] <> 64
      THEN
         int_largest_accum := mat_values_fac[1][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[1][2] = 1
      AND   mat_values_fac[1][2] + 7 < mat_values_fac[1][3]
      THEN
         int_largest_accum := mat_values_fac[1][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[2][3] = 64
      AND   mat_values_fac[2][3] + 7 < mat_values_fac[1][3]
      THEN
         int_largest_accum := mat_values_fac[1][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y - 1;
         
      END IF;
      
   END IF;
   
   -- 2,1
   IF  mat_values_fac[2][1] > int_largest_accum
   AND mat_values_fdr[2][2] <> 16
   THEN
      IF mat_values_fdr[1][1] <> 4
      AND mat_values_fdr[3][1] <> 64
      THEN
         int_largest_accum := mat_values_fac[2][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y;
      
      ELSIF mat_values_fdr[1][1] = 4
      AND   mat_values_fac[1][1] + 7 < mat_values_fac[2][1]
      THEN
         int_largest_accum := mat_values_fac[2][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y;
      
      ELSIF mat_values_fdr[3][1] = 64
      AND   mat_values_fac[3][1] + 7 < mat_values_fac[2][1]
      THEN
         int_largest_accum := mat_values_fac[2][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y;
         
      END IF;
      
   END IF;
   
   -- 2,3
   IF  mat_values_fac[2][3] > int_largest_accum
   AND mat_values_fdr[2][2] <> 1
   THEN
      IF mat_values_fdr[1][3] <> 4
      AND mat_values_fdr[3][3] <> 64
      THEN
         int_largest_accum := mat_values_fac[2][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y;
      
      ELSIF mat_values_fdr[1][3] = 4
      AND   mat_values_fac[1][3] + 7 < mat_values_fac[2][3]
      THEN
         int_largest_accum := mat_values_fac[2][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y;
      
      ELSIF mat_values_fdr[3][3] = 64
      AND   mat_values_fac[3][3] + 7 < mat_values_fac[2][3]
      THEN
         int_largest_accum := mat_values_fac[2][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y;
         
      END IF;
      
   END IF;
   
   -- 3,1
   IF  mat_values_fac[3][1] > int_largest_accum
   AND mat_values_fdr[2][2] <> 8
   THEN
      IF mat_values_fdr[2][1] <> 4
      AND mat_values_fdr[3][2] <> 16
      THEN
         int_largest_accum := mat_values_fac[3][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[2][1] = 4
      AND   mat_values_fac[2][1] + 7 < mat_values_fac[3][1]
      THEN
         int_largest_accum := mat_values_fac[3][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[3][2] = 16
      AND   mat_values_fac[3][2] + 7 < mat_values_fac[3][1]
      THEN
         int_largest_accum := mat_values_fac[3][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y + 1;
         
      END IF;
      
   END IF;
   
   -- 3,2
   IF  mat_values_fac[3][2] > int_largest_accum
   AND mat_values_fdr[2][2] <> 4
   THEN
      IF  mat_values_fdr[3][1] <> 1
      AND mat_values_fdr[3][3] <> 16
      THEN
         int_largest_accum := mat_values_fac[3][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[3][1] = 1
      AND   mat_values_fac[3][1] + 7 < mat_values_fac[3][2]
      THEN
         int_largest_accum := mat_values_fac[3][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[3][3] = 16
      AND   mat_values_fac[3][3] + 7 < mat_values_fac[3][2]
      THEN
         int_largest_accum := mat_values_fac[3][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y + 1;
         
      END IF;
      
   END IF;
   
   -- 3,3
   IF  mat_values_fac[3][3] > int_largest_accum
   AND mat_values_fdr[2][2] <> 2
   THEN
      IF mat_values_fdr[3][2] <> 1
      AND mat_values_fdr[2][3] <> 4
      THEN
         int_largest_accum := mat_values_fac[3][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[3][2] = 1
      AND   mat_values_fac[3][2] + 7 < mat_values_fac[3][3]
      THEN
         int_largest_accum := mat_values_fac[3][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[2][3] = 4
      AND   mat_values_fac[2][3] + 7 < mat_values_fac[3][3]
      THEN
         int_largest_accum := mat_values_fac[3][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y + 1;
         
      END IF;
      
   END IF;
   --RAISE WARNING '% %', int_column_x_fdr, int_row_y_fdr;
   
   --------------------------------------------------------------------------
   -- Step 80
   -- Return results
   --------------------------------------------------------------------------
   out_columnX := int_column_x_fdr;
   out_rowY    := int_row_y_fdr;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.deepest_cell(
    GEOMETRY
   ,RASTER
   ,RASTER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.deepest_cell(
    GEOMETRY
   ,RASTER
   ,RASTER
) TO PUBLIC;
--******************************--
----- functions/determine_grid_srid.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.determine_grid_srid';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.determine_grid_srid(
    IN  p_geometry             GEOMETRY
   ,IN  p_nhdplus_version      VARCHAR
   ,IN  p_known_region         VARCHAR
   ,OUT out_srid               INTEGER
   ,OUT out_grid_size          NUMERIC
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec RECORD;
   
BEGIN

   IF p_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cipsrv_nhdplus_m.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      out_srid           := rec.out_srid;
      out_grid_size      := rec.out_grid_size;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
   
   ELSIF p_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cipsrv_nhdplus_h.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      out_srid           := rec.out_srid;
      out_grid_size      := rec.out_grid_size;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;

   ELSE
      RAISE EXCEPTION 'err %',p_nhdplus_version;

   END IF;   
   
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.determine_grid_srid';
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

--******************************--
----- functions/edges2rings.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.edges2rings';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.edges2rings()
RETURNS BOOLEAN
VOLATILE
AS $BODY$ 
DECLARE
   int_edge_id        INTEGER;
   ary_edge_id        INTEGER[];
   ary_interior_side  VARCHAR(1)[];
   ary_start_node_id  INTEGER[];
   ary_end_node_id    INTEGER[];
   ary_shape          GEOMETRY[];
   int_touch_count    INTEGER;
   
   str_snake_interior VARCHAR(1);
   int_snake_start_id INTEGER;
   int_snake_end_id   INTEGER;
   sdo_snake_shape    GEOMETRY;
   ary_snake_body     INTEGER[];
   
   boo_forwards       BOOLEAN;
   boo_backwards      BOOLEAN;
   boo_ccw            BOOLEAN;
   str_ring_type      VARCHAR(1);
   int_sanity         INTEGER;
   
BEGIN

   -----------------------------------------------------------------------------
   -- Step 10
   -- Pick out a random edge
   -----------------------------------------------------------------------------
   SELECT
    a.edge_id
   ,a.interior_side
   ,a.start_node_id
   ,a.end_node_id
   ,a.shape
   ,a.touch_count
   INTO
    int_edge_id
   ,str_snake_interior
   ,int_snake_start_id
   ,int_snake_end_id
   ,sdo_snake_shape
   ,int_touch_count
   FROM
   tmp_delineation_edges a
   ORDER BY
   a.touch_count ASC
   LIMIT 1;
      
   IF int_edge_id IS NULL
   THEN  
      RETURN FALSE;
      
   END IF;

   ary_snake_body := ARRAY[int_edge_id];
   
   -----------------------------------------------------------------------------
   -- Step 20
   -- Grow the search snake
   -----------------------------------------------------------------------------
   boo_forwards  := TRUE;
   boo_backwards := TRUE;
   
   WHILE boo_forwards AND boo_backwards
   LOOP
      SELECT
       array_agg(a.edge_id)
      ,array_agg(a.interior_side)
      ,array_agg(a.start_node_id)
      ,array_agg(a.end_node_id)
      ,array_agg(a.shape)
      INTO
       ary_edge_id
      ,ary_interior_side
      ,ary_start_node_id
      ,ary_end_node_id
      ,ary_shape
      FROM (
         SELECT
          aa.edge_id
         ,CASE
          WHEN int_snake_end_id = aa.start_node_id
          THEN
            aa.interior_side
          ELSE
            CASE
            WHEN aa.interior_side = 'R'
            THEN
              'L'::VARCHAR(1)
            ELSE
              'R'::VARCHAR(1)
            END
          END AS interior_side
         ,CASE
          WHEN int_snake_end_id = aa.start_node_id
          THEN
            aa.start_node_id
          ELSE
            aa.end_node_id
          END AS start_node_id
         ,CASE
          WHEN int_snake_end_id = aa.start_node_id
          THEN
            aa.end_node_id
          ELSE
            aa.start_node_id
          END AS end_node_id
         ,CASE
          WHEN int_snake_end_id = aa.start_node_id
          THEN
            aa.shape
          ELSE
            ST_Reverse(aa.shape)
          END AS shape
         FROM
         tmp_delineation_edges aa
         WHERE (
               ( int_snake_end_id = aa.start_node_id AND aa.interior_side  = str_snake_interior )
            OR ( int_snake_end_id = aa.end_node_id   AND aa.interior_side <> str_snake_interior )
         ) 
         AND aa.edge_id <> ALL(ary_snake_body)            
      ) a;
      
      IF array_length(ary_edge_id,1) = 1
      THEN
         int_snake_end_id := ary_end_node_id[1];
         sdo_snake_shape  := ST_MakeLine(sdo_snake_shape,ary_shape[1]);
         ary_snake_body   := array_append(ary_snake_body,ary_edge_id[1]);

      ELSE
         boo_forwards := FALSE;
      
      END IF;
      
      SELECT
       array_agg(a.edge_id)
      ,array_agg(a.interior_side)
      ,array_agg(a.start_node_id)
      ,array_agg(a.end_node_id)
      ,array_agg(a.shape)
      INTO
       ary_edge_id
      ,ary_interior_side
      ,ary_start_node_id
      ,ary_end_node_id
      ,ary_shape
      FROM (
         SELECT
          aa.edge_id
         ,CASE
          WHEN int_snake_start_id = aa.end_node_id
          THEN
            aa.interior_side
          ELSE
            CASE
            WHEN aa.interior_side = 'R'
            THEN
              'L'::VARCHAR(1)
            ELSE
              'R'::VARCHAR(1)
            END
          END AS interior_side
         ,CASE
          WHEN int_snake_start_id = aa.end_node_id
          THEN
            aa.start_node_id
          ELSE
            aa.end_node_id
          END AS start_node_id
         ,CASE
          WHEN int_snake_start_id = aa.end_node_id
          THEN
            aa.end_node_id
          ELSE
            aa.start_node_id
          END AS end_node_id
         ,CASE
          WHEN int_snake_start_id = aa.end_node_id
          THEN
            aa.shape
          ELSE
            ST_Reverse(aa.shape)
          END AS shape
         FROM
         tmp_delineation_edges aa
         WHERE (
               ( int_snake_start_id = aa.end_node_id   AND aa.interior_side  = str_snake_interior )
            OR ( int_snake_start_id = aa.start_node_id AND aa.interior_side <> str_snake_interior )
         ) 
         AND aa.edge_id <> ALL(ary_snake_body)            
      ) a;
      
      IF array_length(ary_edge_id,1) = 1
      THEN
         int_snake_start_id := ary_start_node_id[1];
         sdo_snake_shape    := ST_MakeLine(ary_shape[1],sdo_snake_shape);
         ary_snake_body     := array_append(ary_snake_body,ary_edge_id[1]);

      ELSE
         boo_backwards := FALSE;
      
      END IF;
      
      int_sanity := int_sanity + 1;
      
      IF int_sanity > 50000
      THEN
         RAISE EXCEPTION 'sanity check';
      
      END IF;
      
   END LOOP;
   
   -----------------------------------------------------------------------------
   -- Test for completed ring
   -----------------------------------------------------------------------------
   IF int_snake_start_id = int_snake_end_id
   THEN
      boo_ccw := ST_IsPolygonCCW(ST_MakePolygon(sdo_snake_shape));
      
      IF boo_ccw AND str_snake_interior = 'L'
      OR NOT boo_ccw AND str_snake_interior = 'R'
      THEN
         str_ring_type := 'E';
         
      ELSE
         str_ring_type := 'I';
         
      END IF;
      
      INSERT INTO tmp_delineation_rings(
          ring_id
         ,ring_type
         ,shape
      ) VALUES (
          int_edge_id
         ,str_ring_type
         ,sdo_snake_shape
      );
      
      DELETE FROM tmp_delineation_edges
      WHERE edge_id = ANY(ary_snake_body);
   
   -----------------------------------------------------------------------------
   -- Scrunch the edges
   -----------------------------------------------------------------------------
   ELSE
      DELETE FROM tmp_delineation_edges
      WHERE edge_id = ANY(ary_snake_body);
      
      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape 
         ,touch_count
      ) VALUES (
          int_edge_id
         ,str_snake_interior
         ,int_snake_start_id
         ,int_snake_end_id
         ,sdo_snake_shape
         ,int_touch_count + 1
      );
   
   END IF;
   
   RETURN TRUE;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.edges2rings
OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.edges2rings
TO PUBLIC;

--******************************--
----- functions/fdr_upstream_norecursion.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.fdr_upstream_norecursion';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.fdr_upstream_norecursion(
    IN p_column_x             INTEGER
   ,IN p_row_y                INTEGER
   ,IN OUT iout_rast          RASTER
   ,OUT out_depth             INTEGER 
)
IMMUTABLE
AS $BODY$
DECLARE
   boo_continue     BOOLEAN;
   int_depth_charge INTEGER := 100000;
   mat_values       INTEGER[][];
   int_working_x    INTEGER[];
   int_working_y    INTEGER[];
   int_increment_x  INTEGER[];
   int_increment_y  INTEGER[];
   int_index        INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF iout_rast IS NULL
   THEN
      RAISE EXCEPTION 'Input raster is null';
      
   END IF;
   --RAISE WARNING 'raster % by %',ST_Height(iout_rast),ST_Width(iout_rast);
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Seed the start location
   ----------------------------------------------------------------------------
   int_working_x[1] := p_column_x;
   int_working_y[1] := p_row_y;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Set up outer loop to allow starting over and exit condition
   ----------------------------------------------------------------------------
   out_depth := 1;
   boo_continue := TRUE;
   <<outer_loop>>
   WHILE boo_continue
   LOOP
      
   ----------------------------------------------------------------------------
   -- Step 40
   -- Abend if stuck in ceaseless loop
   ----------------------------------------------------------------------------
      out_depth := out_depth + 1;
      IF out_depth > int_depth_charge
      THEN
         RAISE EXCEPTION 'depth charge';
         
      END IF;
      
   ----------------------------------------------------------------------------
   -- Step 50
   -- Initialize increment queue
   ----------------------------------------------------------------------------
      int_index       := 1;
      int_increment_x := NULL;
      int_increment_y := NULL;
      
   ----------------------------------------------------------------------------
   -- Step 60
   -- Loop through the working stack
   ----------------------------------------------------------------------------
      --RAISE WARNING '%', array_length(int_working_x,1);
      FOR i IN 1 .. array_length(int_working_x,1)
      LOOP
         
   ----------------------------------------------------------------------------
   -- Step 70
   -- Pull 3x3 grid
   ----------------------------------------------------------------------------   
         mat_values := ST_Neighborhood(
             iout_rast
            ,1
            ,int_working_x[i]
            ,int_working_y[i]
            ,1
            ,1
         );
         --raise warning '%', mat_values;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Set center grid to be 99
   ----------------------------------------------------------------------------
         IF mat_values[2][2] <> 99
         THEN
            
            iout_rast := ST_SetValue(
                iout_rast
               ,int_working_x[i]
               ,int_working_y[i]
               ,99
            );
            
   ----------------------------------------------------------------------------
   -- Step 90
   -- Examine surrounding cells to define the upstream area
   ----------------------------------------------------------------------------         
            IF mat_values[1][1] = 2
            THEN
               int_increment_x[int_index] := int_working_x[i] - 1;
               int_increment_y[int_index] := int_working_y[i] - 1;
               int_index := int_index + 1;

            END IF;

            IF mat_values[1][2] = 4
            THEN
               int_increment_x[int_index] := int_working_x[i];
               int_increment_y[int_index] := int_working_y[i] - 1;
               int_index := int_index + 1;

            END IF;

            IF mat_values[1][3] = 8
            THEN
               int_increment_x[int_index] := int_working_x[i] + 1;
               int_increment_y[int_index] := int_working_y[i] - 1;
               int_index := int_index + 1;
              
            END IF;

            IF mat_values[2][1] = 1
            THEN
               int_increment_x[int_index] := int_working_x[i] - 1;
               int_increment_y[int_index] := int_working_y[i];
               int_index := int_index + 1;

            END IF;

            IF mat_values[2][3] = 16
            THEN
               int_increment_x[int_index] := int_working_x[i] + 1;
               int_increment_y[int_index] := int_working_y[i];
               int_index := int_index + 1;

            END IF;

            IF mat_values[3][1] = 128
            THEN
               int_increment_x[int_index] := int_working_x[i] - 1;
               int_increment_y[int_index] := int_working_y[i] + 1;
               int_index := int_index + 1;
               
            END IF;

            IF mat_values[3][2] = 64
            THEN
               int_increment_x[int_index] := int_working_x[i];
               int_increment_y[int_index] := int_working_y[i] + 1;
               int_index := int_index + 1;

            END IF;

            IF mat_values[3][3] = 32
            THEN
               int_increment_x[int_index] := int_working_x[i] + 1;
               int_increment_y[int_index] := int_working_y[i] + 1;
               int_index := int_index + 1;
               
            END IF;

         END IF;

      END LOOP;
      
   ----------------------------------------------------------------------------
   -- Step 100
   -- Continue until done
   ----------------------------------------------------------------------------
      IF int_increment_x IS NULL
      OR array_length(int_increment_x,1) = 0
      THEN
         boo_continue := FALSE;
         
      END IF;
      
      int_working_x := int_increment_x;
      int_working_y := int_increment_y;
      
   ----------------------------------------------------------------------------
   -- Step 110
   -- Continue until done
   ----------------------------------------------------------------------------
   END LOOP outer_loop;

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.fdr_upstream_norecursion(
    INTEGER
   ,INTEGER
   ,RASTER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.fdr_upstream_norecursion(
    INTEGER
   ,INTEGER
   ,RASTER
) TO PUBLIC;
--******************************--
----- functions/feature_batch_clip.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.feature_batch_clip';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.feature_batch_clip(
    IN  p_keyword                       VARCHAR
   ,IN  p_clippers                      VARCHAR[]
   ,IN  p_known_region                  VARCHAR
   ,OUT out_return_code                 INTEGER
   ,OUT out_status_message              VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                  RECORD;
   str_sql              VARCHAR;
   
BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_clippers IS NULL
   OR array_length(p_clippers,1) = 0
   THEN
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Clip the point features
   ----------------------------------------------------------------------------
   str_sql := 'UPDATE cipsrv_upload.' || p_keyword || '_points a '
           || 'SET shape = ( '
           || '   SELECT '
           || '   b.out_clipped_geometry '
           || '   FROM '
           || '   cipsrv_support.geometry_clip( '
           || '       p_geometry      := a.shape '
           || '      ,p_clippers      := ? '
           || '      ,p_known_region  := ? '
           || '   ) '
           || ') ';
           
   EXECUTE str_sql USING p_clippers,p_known_region;
   
   str_sql := 'DELETE FROM cipsrv_upload.' || p_keyword || '_points a '
           || 'WHERE '
           || 'a.shape IS NULL ';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Clip the line features
   ----------------------------------------------------------------------------
   str_sql := 'UPDATE cipsrv_upload.' || p_keyword || '_lines a '
           || 'SET shape = ( '
           || '   SELECT '
           || '   b.out_clipped_geometry '
           || '   FROM '
           || '   cipsrv_support.geometry_clip( '
           || '       p_geometry      := a.shape '
           || '      ,p_clippers      := ? '
           || '      ,p_known_region  := ? '
           || '   ) '
           || ') ';
           
   EXECUTE str_sql USING p_clippers,p_known_region;
   
   str_sql := 'DELETE FROM cipsrv_upload.' || p_keyword || '_lines a '
           || 'WHERE '
           || 'a.shape IS NULL ';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Clip the area features
   ----------------------------------------------------------------------------
   str_sql := 'UPDATE cipsrv_upload.' || p_keyword || '_areas a '
           || 'SET shape = ( '
           || '   SELECT '
           || '   b.out_clipped_geometry '
           || '   FROM '
           || '   cipsrv_support.geometry_clip( '
           || '       p_geometry      := a.shape '
           || '      ,p_clippers      := ? '
           || '      ,p_known_region  := ? '
           || '   ) '
           || ') ';
           
   EXECUTE str_sql USING p_clippers,p_known_region;
   
   str_sql := 'DELETE FROM cipsrv_upload.' || p_keyword || '_areas a '
           || 'WHERE '
           || 'a.shape IS NULL ';
           
   EXECUTE str_sql;
   
   RETURN;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.feature_batch_clip(
    VARCHAR
   ,VARCHAR[]
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.feature_batch_clip(
    VARCHAR
   ,VARCHAR[]
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/feature_clip.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.feature_clip';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.feature_clip(
    IN  p_features                      cipsrv_engine.cip_feature[]
   ,IN  p_clippers                      VARCHAR[]
   ,IN  p_known_region                  VARCHAR
   ,OUT out_return_code                 INTEGER
   ,OUT out_status_message              VARCHAR
   ,OUT out_features                    cipsrv_engine.cip_feature[]
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                  RECORD;
   obj_rez cipsrv_engine.cip_feature;
   ary_rez cipsrv_engine.cip_feature[];
   str_known_region     VARCHAR := p_known_region;
   sdo_output           GEOMETRY;
   
BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_features IS NULL
   OR array_length(p_features,1) = 0
   THEN
      RETURN;
      
   END IF;
   
   IF p_clippers IS NULL
   OR array_length(p_clippers,1) = 0
   THEN
      out_features := p_features;
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Loop over the features
   ----------------------------------------------------------------------------
   FOR i IN 1 .. array_length(p_features,1)
   LOOP
      obj_rez := p_features[i];
      
      rec := cipsrv_support.geometry_clip(
          p_geometry      := (obj_rez).geometry
         ,p_clippers      := p_clippers
         ,p_known_region  := str_known_region
      );
      sdo_output         := rec.out_clipped_geometry;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
         
      END IF;
      
      IF sdo_output IS NULL
      OR ST_IsEmpty(sdo_output)
      THEN
         NULL;
         
      ELSE
         obj_rez.geometry := sdo_output;
         out_features     := array_append(out_features,obj_rez);
         
      END IF;
         
   END LOOP;
   
   RETURN;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.feature_clip(
    cipsrv_engine.cip_feature[]
   ,VARCHAR[]
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.feature_clip(
    cipsrv_engine.cip_feature[]
   ,VARCHAR[]
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/featurecat.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.featurecat';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.featurecat(
    IN  p_features           cipsrv_engine.cip_feature[]
   ,IN  p_cat                cipsrv_engine.cip_feature[]
) RETURNS cipsrv_engine.cip_feature[]
IMMUTABLE
AS $BODY$ 
DECLARE
   ary_features cipsrv_engine.cip_feature[];
   ary_cat      cipsrv_engine.cip_feature[];
   ary_rez      cipsrv_engine.cip_feature[];
   
BEGIN

   ary_features := array_remove(p_features,NULL);
   ary_cat      := array_remove(p_cat,NULL);
   ary_rez      := NULL::cipsrv_engine.cip_feature[];
   
   IF ary_features IS NOT NULL
   AND array_length(ary_features,1) > 0
   THEN
      FOR i IN 1 .. array_length(ary_features,1)
      LOOP
         ary_rez := array_append(ary_rez,ary_features[i]);
         
      END LOOP;
      
   END IF;
   
   IF ary_cat IS NOT NULL 
   AND array_length(ary_cat,1) > 0
   THEN
      FOR i IN 1 .. array_length(ary_cat,1)
      LOOP
         ary_rez := array_append(ary_rez,ary_cat[i]);
         
      END LOOP;
   
   END IF;

   RETURN ary_rez;   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.featurecat(
    cipsrv_engine.cip_feature[]
   ,cipsrv_engine.cip_feature[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.featurecat(
    cipsrv_engine.cip_feature[]
   ,cipsrv_engine.cip_feature[]
) TO PUBLIC;

--******************************--
----- functions/feature2jsonb.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.feature2jsonb';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.feature2jsonb(
   IN  p_feature      cipsrv_engine.cip_feature
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE
   json_rez        JSONB;
   json_properties JSONB;
   
BEGIN

   IF p_feature IS NULL
   THEN
      RETURN json_rez;
      
   END IF;
   
   json_properties := (p_feature).properties;
   
   IF json_properties IS NULL
   OR JSONB_TYPEOF(json_properties) = 'null'
   THEN
      json_properties := '{}'::JSONB;
      
   END IF;
   
   IF (p_feature).globalid IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['globalid']
         ,replacement       := TO_JSONB((p_feature).globalid)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).lengthkm IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['lengthkm']
         ,replacement       := TO_JSONB((p_feature).lengthkm)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).areasqkm IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['areasqkm']
         ,replacement       := TO_JSONB((p_feature).areasqkm)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).converted_to_ring IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['converted_to_ring']
         ,replacement       := TO_JSONB((p_feature).converted_to_ring)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).indexing_method_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['indexing_method_used']
         ,replacement       := TO_JSONB((p_feature).indexing_method_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).line_threshold_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['line_threshold_used']
         ,replacement       := TO_JSONB((p_feature).line_threshold_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).areacat_threshold_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['areacat_threshold_used']
         ,replacement       := TO_JSONB((p_feature).areacat_threshold_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).areaevt_threshold_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['areaevt_threshold_used']
         ,replacement       := TO_JSONB((p_feature).areaevt_threshold_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   
   
   IF (p_feature).ring_areacat_threshold_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['ring_areacat_threshold_used']
         ,replacement       := TO_JSONB((p_feature).ring_areacat_threshold_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).ring_areaevt_threshold_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['ring_areaevt_threshold_used']
         ,replacement       := TO_JSONB((p_feature).ring_areaevt_threshold_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   json_rez := JSONB_BUILD_OBJECT(
       'type'       ,'Feature'
      ,'geometry'   ,ST_AsGeoJSON(ST_Transform((p_feature).geometry,4326))::JSONB
      ,'obj_type'   ,'event_feature_properties'
      ,'properties' ,json_properties
   );

   RETURN json_rez;   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.feature2jsonb(
   cipsrv_engine.cip_feature
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.feature2jsonb(
   cipsrv_engine.cip_feature
) TO PUBLIC;

--******************************--
----- functions/features2geomcollection.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.features2geomcollection';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.features2geomcollection(
    IN  p_features           cipsrv_engine.cip_feature[]
) RETURNS GEOMETRY
IMMUTABLE
AS $BODY$ 
DECLARE
   sdo_rez GEOMETRY;
   
BEGIN

   IF p_features IS NULL
   OR array_length(p_features,1) = 0
   THEN
      RETURN NULL;
      
   END IF;
   
   FOR i IN 1 .. array_length(p_features,1)
   LOOP
      IF sdo_rez IS NULL
      THEN
         sdo_rez := ST_Transform(p_features[i].geometry,4326);
         
      ELSE
         sdo_rez := ST_Collect(sdo_rez,ST_Transform(p_features[i].geometry,4326));
      
      END IF;
   
   END LOOP;
   
   RETURN sdo_rez;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.features2geomcollection(
    cipsrv_engine.cip_feature[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.features2geomcollection(
    cipsrv_engine.cip_feature[]
) TO PUBLIC;

--******************************--
----- functions/features2jsonb.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.features2jsonb';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.features2jsonb(
    IN  p_features           cipsrv_engine.cip_feature[]
   ,IN  p_geometry_type      VARCHAR DEFAULT NULL
   ,IN  p_empty_collection   BOOLEAN DEFAULT FALSE
) RETURNS JSONB
IMMUTABLE
AS $BODY$
DECLARE
   obj_rez JSONB;
   ary_rez JSONB;

BEGIN

   IF p_features IS NULL
   OR array_length(p_features,1) = 0
   THEN
      IF p_empty_collection
      THEN
         RETURN JSON_BUILD_OBJECT(
             'type'    , 'FeatureCollection'
            ,'features', '[]'::JSONB
         );

      ELSE
         RETURN NULL;

      END IF;

   END IF;

   FOR i IN 1 .. array_length(p_features,1)
   LOOP
      IF p_geometry_type IS NULL
      OR ( p_geometry_type IN ('P')
         AND p_features[i].gtype IN ('ST_Point','ST_MultiPoint')
      )
      OR ( p_geometry_type IN ('L')
         AND p_features[i].gtype IN ('ST_LineString','ST_MultiLineString')
      )
      OR ( p_geometry_type IN ('A')
         AND p_features[i].gtype IN ('ST_Polygon','ST_MultiPolygon')
      )
      THEN
         obj_rez := cipsrv_engine.feature2jsonb(
            p_feature := p_features[i]
         );

         IF ary_rez IS NULL
         THEN
            ary_rez := JSON_BUILD_ARRAY(obj_rez);

         ELSE
            ary_rez := ary_rez || obj_rez;

         END IF;

      END IF;

   END LOOP;

   IF ary_rez IS NULL
   OR JSONB_ARRAY_LENGTH(ary_rez) = 0
   THEN
      IF p_empty_collection
      THEN
         RETURN JSON_BUILD_OBJECT(
             'type'    , 'FeatureCollection'
            ,'features', '[]'::JSONB
         );

      ELSE
         RETURN NULL;

      END IF;

   ELSE
      RETURN JSON_BUILD_OBJECT(
          'type'    , 'FeatureCollection'
         ,'features', ary_rez
      );

   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.features2jsonb(
    cipsrv_engine.cip_feature[]
   ,VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.features2jsonb(
    cipsrv_engine.cip_feature[]
   ,VARCHAR
   ,BOOLEAN
) TO PUBLIC;

--******************************--
----- functions/field_exists.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.field_exists';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.field_exists(
    IN  p_schema_name VARCHAR
   ,IN  p_table_name  VARCHAR
   ,IN  p_field_name  VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   int_oid        INTEGER;
   str_table_name VARCHAR(255);
   str_field_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for table
   ----------------------------------------------------------------------------
   SELECT 
    c.oid
   INTO int_oid
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   WHERE  
       n.nspname = p_schema_name
   AND c.relname = p_table_name
   AND c.relkind = 'r';
   
   IF int_oid IS NULL 
   THEN
      RETURN FALSE;
      
   END IF;
   
   SELECT 
    a.attname
   INTO str_field_name
   FROM 
   pg_catalog.pg_attribute a 
   WHERE 
       a.attrelid = int_oid
   AND a.attname  = p_field_name
   AND NOT attisdropped
   AND attnum > 0; 

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_field_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.field_exists(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.field_exists(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/get_measure.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.get_measure';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.get_measure(
    IN  p_nhdplus_version              VARCHAR
   ,IN  p_direction                    VARCHAR DEFAULT NULL
   ,IN  p_nhdplusid                    BIGINT  DEFAULT NULL
   ,IN  p_permanent_identifier         VARCHAR DEFAULT NULL
   ,IN  p_reachcode                    VARCHAR DEFAULT NULL
   ,IN  p_hydroseq                     BIGINT  DEFAULT NULL
   ,IN  p_measure                      NUMERIC DEFAULT NULL
   ,OUT out_measure                    NUMERIC
   ,OUT out_nhdplusid                  BIGINT
   ,OUT out_permanent_identifier       VARCHAR
   ,OUT out_reachcode                  VARCHAR
   ,OUT out_fmeasure                   NUMERIC
   ,OUT out_tmeasure                   NUMERIC
   ,OUT out_hydroseq                   BIGINT
   ,OUT out_uphydroseq                 BIGINT
   ,OUT out_terminalpa                 BIGINT
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                RECORD;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   IF UPPER(p_nhdplus_version) IN ('MR','NHDPLUS_M')
   THEN
      rec := cipsrv_nhdplus_m.get_measure(
          p_direction             := p_direction
         ,p_nhdplusid             := p_nhdplusid
         ,p_permanent_identifier  := p_permanent_identifier
         ,p_reachcode             := p_reachcode
         ,p_hydroseq              := p_hydroseq
         ,p_measure               := p_measure
      );
      
   ELSIF UPPER(p_nhdplus_version) IN ('HR','NHDPLUS_H')
   THEN
      rec := cipsrv_nhdplus_h.get_measure(
          p_direction             := p_direction
         ,p_nhdplusid             := p_nhdplusid
         ,p_permanent_identifier  := p_permanent_identifier
         ,p_reachcode             := p_reachcode
         ,p_hydroseq              := p_hydroseq
         ,p_measure               := p_measure
      );
   
   ELSE
      RAISE EXCEPTION 'unknown nhdplus version %',p_nhdplus_version;
   
   END IF;
   
   out_measure              := rec.out_measure;
   out_nhdplusid            := rec.out_nhdplusid;
   out_permanent_identifier := rec.out_permanent_identifier;
   out_reachcode            := rec.out_reachcode;
   out_fmeasure             := rec.out_fmeasure;
   out_tmeasure             := rec.out_tmeasure;
   out_hydroseq             := rec.out_hydroseq;
   out_uphydroseq           := rec.out_uphydroseq;
   out_terminalpa           := rec.out_terminalpa;
   out_return_code          := rec.out_return_code;
   out_status_message       := rec.out_status_message;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.get_measure';
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

--******************************--
----- functions/index_exists.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.index_exists';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.index_exists(
    IN  p_schema_name VARCHAR
   ,IN  p_table_name  VARCHAR
   ,IN  p_index_name  VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_index_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for table
   ----------------------------------------------------------------------------
   SELECT 
   a.indexname
   INTO str_index_name
   FROM 
   pg_indexes a
   WHERE
       a.schemaname = p_schema_name
   AND a.tablename  = p_table_name
   AND a.indexname  = p_index_name;

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_index_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.index_exists(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.index_exists(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/is_lrs.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.is_lrs';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.is_lrs(
    IN  p_geometry          GEOMETRY
) RETURNS BOOLEAN
AS
$BODY$
DECLARE
BEGIN

   IF ST_M(
      ST_PointN(
          ST_GeometryN(p_geometry,1)
         ,1
      )
   ) IS NULL
   OR ST_M(
      ST_PointN(
          ST_GeometryN(p_geometry,1)
         ,ST_NumPoints(ST_GeometryN(p_geometry,1))
      )
   ) IS NULL
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.is_lrs(
   GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.is_lrs(
   GEOMETRY
) TO PUBLIC;


--******************************--
----- functions/json2geometry.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.json2geometry';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.json2geometry(
    IN  p_in                         JSONB
) RETURNS GEOMETRY
IMMUTABLE
AS $BODY$ 
DECLARE 
   json_feature JSONB;
   
BEGIN

   IF p_in IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF NOT JSONB_PATH_EXISTS(p_in,'$.type')
   THEN
      RETURN NULL;
      
   END IF;
   
   IF p_in->>'type' IN ('Point','LineString','Polygon','MultiPoint','MultiLineString','MultiPolygon','GeometryCollection')
   THEN
      RETURN ST_GeomFromGeoJSON(p_in);
      
   ELSIF p_in->>'type' = 'Feature'
   THEN
      IF NOT JSONB_PATH_EXISTS(p_in,'$.geometry')
      THEN
         RETURN NULL;
         
      END IF;
      
      RETURN ST_GeomFromGeoJSON(p_in->'geometry');
   
   ELSIF p_in->>'type' = 'FeatureCollection'
   THEN
      IF NOT JSONB_PATH_EXISTS(p_in,'$.features')
      OR p_in->'features' IS NULL
      OR JSONB_ARRAY_LENGTH(p_in->'features') = 0
      THEN
         RETURN NULL;
         
      END IF;
      
      json_feature := p_in->'features'->0;
      
      IF NOT JSONB_PATH_EXISTS(json_feature,'$.geometry')
      THEN
         RETURN NULL;
         
      END IF;
      
      RETURN ST_GeomFromGeoJSON(json_feature->'geometry');
   
   END IF;
   
   RETURN NULL;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.json2geometry(
   JSONB
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.json2geometry(
   JSONB
) TO PUBLIC;

--******************************--
----- functions/json2numeric.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.json2numeric';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.json2numeric(
    IN  p_in                         JSONB
) RETURNS NUMERIC
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   IF p_in IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF JSONB_TYPEOF(p_in) = 'string'
   THEN
      IF p_in::VARCHAR IN ('',' ','null','""')
      THEN
         RETURN NULL;

      ELSE            
         RETURN REPLACE(
            p_in::VARCHAR
           ,'"'
           ,''           
         )::NUMERIC;
         
      END IF;
   
   ELSE
      RETURN p_in;

   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.json2numeric(
   JSONB
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.json2numeric(
   JSONB
) TO PUBLIC;

--******************************--
----- functions/json2bigint.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.json2bigint';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.json2bigint(
    IN  p_in                         JSONB
) RETURNS BIGINT
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   IF p_in IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF JSONB_TYPEOF(p_in) = 'string'
   THEN
      IF p_in::VARCHAR IN ('',' ','null','""')
      THEN
         RETURN NULL;

      ELSE            
         RETURN REPLACE(
            p_in::VARCHAR
           ,'"'
           ,''           
         )::BIGINT;
         
      END IF;
   
   ELSE
      RETURN p_in;

   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.json2bigint(
   JSONB
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.json2bigint(
   JSONB
) TO PUBLIC;

--******************************--
----- functions/json2date.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.json2date';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.json2date(
    IN  p_in                         JSONB
) RETURNS DATE
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   IF p_in IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF JSONB_TYPEOF(p_in) = 'string'
   THEN
      IF p_in::VARCHAR IN ('',' ','null','""')
      THEN
         RETURN NULL;

      ELSE            
         RETURN TO_DATE(
            p_in::VARCHAR
           ,'YYYY-MM-DD"T"HH24:MI:SS"Z"'           
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
   WHERE p.oid::regproc::text = 'cipsrv_engine.json2date';
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

--******************************--
----- functions/json2strary.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.json2strary';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.json2strary(
    IN  p_in                         JSONB
) RETURNS VARCHAR[]
IMMUTABLE
AS $BODY$ 
DECLARE
   rec         RECORD;
   int_counter INTEGER;
   ary_out     VARCHAR[];
   
BEGIN

   IF p_in IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF JSONB_TYPEOF(p_in) = 'string'
   THEN
      IF p_in::VARCHAR IN ('',' ','null','""')
      THEN
         RETURN NULL;

      ELSE            
         RETURN NULL;
         
      END IF;
   
   ELSIF JSONB_TYPEOF(p_in) = 'array'
   THEN
      int_counter := 1;
      FOR rec IN SELECT JSONB_ARRAY_ELEMENTS_TEXT(p_in) AS item
      LOOP
         ary_out[int_counter] := rec.item;
         int_counter := int_counter + 1;
      
      END LOOP;
   
   END IF;
   
   RETURN ary_out;
   
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.json2strary';
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

--******************************--
----- functions/jsonb2feature.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.jsonb2feature';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.jsonb2feature(
    IN  p_feature                JSONB
   ,IN  p_geometry_override      GEOMETRY DEFAULT NULL
   ,IN  p_globalid               VARCHAR  DEFAULT NULL
   ,IN  p_source_featureid       VARCHAR  DEFAULT NULL
   ,IN  p_permid_joinkey         VARCHAR  DEFAULT NULL
   ,IN  p_nhdplus_version        VARCHAR  DEFAULT NULL
   ,IN  p_known_region           VARCHAR  DEFAULT NULL
   ,IN  p_int_srid               INTEGER  DEFAULT NULL
   
   ,IN  p_point_indexing_method  VARCHAR  DEFAULT NULL
   
   ,IN  p_line_indexing_method   VARCHAR  DEFAULT NULL
   ,IN  p_line_threshold         NUMERIC  DEFAULT NULL
   
   ,IN  p_ring_indexing_method   VARCHAR  DEFAULT NULL
   ,IN  p_ring_areacat_threshold NUMERIC  DEFAULT NULL
   ,IN  p_ring_areaevt_threshold NUMERIC  DEFAULT NULL
   
   ,IN  p_area_indexing_method   VARCHAR  DEFAULT NULL
   ,IN  p_areacat_threshold      NUMERIC  DEFAULT NULL
   ,IN  p_areaevt_threshold      NUMERIC  DEFAULT NULL
   
) RETURNS cipsrv_engine.cip_feature[]
IMMUTABLE
AS $BODY$ 
DECLARE
   rec                        RECORD;
   obj_rez cipsrv_engine.cip_feature;
   ary_rez cipsrv_engine.cip_feature[];
   has_properties             BOOLEAN;
   boo_isring                 BOOLEAN;
   str_globalid               VARCHAR;
   str_permid_joinkey         VARCHAR;
   str_nhdplus_version        VARCHAR;
   str_known_region           VARCHAR;
   int_srid                   INTEGER;
   str_source_featureid       VARCHAR;
   
   str_point_indexing_method  VARCHAR;
   
   str_line_indexing_method   VARCHAR;
   num_line_threshold         NUMERIC;
   
   str_ring_indexing_method   VARCHAR;
   num_ring_areacat_threshold NUMERIC;
   num_ring_areaevt_threshold NUMERIC;
   
   str_area_indexing_method   VARCHAR;
   num_areacat_threshold      NUMERIC;
   num_areaevt_threshold      NUMERIC;
   
   sdo_geometry               GEOMETRY;
   sdo_geometry2              GEOMETRY;
   json_feature               JSONB := p_feature;
   num_line_lengthkm          NUMERIC;
   num_area_areasqkm          NUMERIC;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF json_feature IS NULL
   THEN
      RETURN ARRAY[obj_rez];
      
   ELSIF JSONB_TYPEOF(json_feature) != 'object'
   OR json_feature->'type' IS NULL
   OR json_feature->>'type' != 'Feature'
   THEN
      RAISE EXCEPTION 'input jsonb is not geojson feature';
   
   ELSE
      IF p_geometry_override IS NOT NULL
      THEN
         json_feature := JSONB_BUILD_OBJECT(
             'type'       ,'Feature'
            ,'geometry'   ,ST_AsGeoJSON(ST_Transform(p_geometry_override,4326))::JSONB
            ,'properties' ,json_feature->'properties'
         );
      
      ELSIF json_feature->>'type' IN (
          'Point'
         ,'LineString'
         ,'Polygon'
         ,'MultiPoint'
         ,'MultiLineString'
         ,'MultiPolygon'
         ,'GeometryCollection'
      )
      THEN
         -- If naked geometry, repack into feature
         json_feature := JSONB_BUILD_OBJECT(
             'type'       ,'Feature'
            ,'geometry'   ,json_feature
         );
         
      END IF;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Get quick boolean if original properties exist
   ----------------------------------------------------------------------------
   IF json_feature->'properties' IS NULL
   THEN
      has_properties := FALSE;

   ELSE
      has_properties := TRUE;

   END IF;
   
   ----------------------------------------------------------------------------
   -- Extract the geometry
   ----------------------------------------------------------------------------
   IF json_feature->'geometry' IS NOT NULL
   THEN
      sdo_geometry := ST_GeomFromGeoJSON(json_feature->'geometry')::JSONB;
      
      -- This fixes a bug in PostGIS 3.1.5
      IF sdo_geometry IS NOT NULL
      AND ( ST_SRID(sdo_geometry) IS NULL OR ST_SRID(sdo_geometry) = 0 )
      THEN
         sdo_geometry := ST_SetSRID(sdo_geometry,4326);
         
      END IF;
      
      -- Break up geometry collections and multilinestrings
      IF ST_GeometryType(sdo_geometry) IN (
          'ST_MultiLineString'
         ,'ST_GeometryCollection'   
      )
      THEN
         FOR i IN 1 .. ST_NumGeometries(sdo_geometry)
         LOOP
            sdo_geometry2 := ST_GeometryN(sdo_geometry,i);
            
            ary_rez := cipsrv_engine.featurecat(ary_rez,
               cipsrv_engine.jsonb2feature(
                   p_feature                := json_feature
                  ,p_geometry_override      := sdo_geometry2
                  ,p_source_featureid       := p_source_featureid
                  ,p_permid_joinkey         := p_permid_joinkey
                  ,p_nhdplus_version        := p_nhdplus_version
                  ,p_known_region           := p_known_region
                  ,p_int_srid               := p_int_srid
                  
                  ,p_point_indexing_method  := p_point_indexing_method
                  
                  ,p_line_indexing_method   := p_line_indexing_method
                  ,p_line_threshold         := p_line_threshold
                  
                  ,p_ring_indexing_method   := p_ring_indexing_method
                  ,p_ring_areacat_threshold := p_ring_areacat_threshold
                  ,p_ring_areaevt_threshold := p_ring_areaevt_threshold
                  
                  ,p_area_indexing_method   := p_area_indexing_method
                  ,p_areacat_threshold      := p_areacat_threshold
                  ,p_areaevt_threshold      := p_areaevt_threshold
               )
            );
            
         END LOOP;
         
         RETURN ary_rez;
 
      END IF;
      
      IF NOT ST_IsValid(sdo_geometry)
      THEN
         sdo_geometry := ST_MakeValid(sdo_geometry);
         
      END IF;

      IF ST_GeometryType(sdo_geometry) = 'ST_LineString'
      THEN
         boo_isring := ST_IsRing(sdo_geometry);
      
      ELSE
         boo_isring := FALSE;
         
      END IF;   
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for globalid override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'globalid' IS NOT NULL
   THEN
      str_globalid := json_feature->'properties'->>'globalid';
      
   ELSIF p_globalid IS NOT NULL
   THEN
      str_globalid := p_globalid;
      
   ELSE
      str_globalid := '{' || uuid_generate_v1() || '}';

   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for source_featureid override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'source_featureid' IS NOT NULL
   THEN
      str_source_featureid := json_feature->'properties'->>'source_featureid';
      
   ELSIF p_source_featureid IS NOT NULL
   THEN
      str_source_featureid := p_source_featureid;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for permid_joinkey override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'permid_joinkey' IS NOT NULL
   THEN
      str_permid_joinkey := json_feature->'properties'->>'permid_joinkey';
      
   ELSIF p_permid_joinkey IS NOT NULL
   THEN
      str_permid_joinkey := p_permid_joinkey;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for nhdplus_version override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'nhdplus_version' IS NOT NULL
   THEN
      str_nhdplus_version := json_feature->'properties'->>'nhdplus_version';
      
   ELSIF p_nhdplus_version IS NOT NULL
   THEN
      str_nhdplus_version := p_nhdplus_version;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for known_region override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'known_region' IS NOT NULL
   THEN
      str_known_region := json_feature->'properties'->>'known_region';
      
   ELSIF p_known_region IS NOT NULL
   THEN
      str_known_region := p_known_region;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Test for int_srid override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'int_srid' IS NOT NULL
   THEN
      int_srid := json_feature->'properties'->'int_srid';
      
   ELSIF p_int_srid IS NOT NULL
   THEN
      int_srid := p_int_srid;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Try to sort out int_srid with overrides
   ----------------------------------------------------------------------------
   IF  int_srid IS NULL
   AND str_nhdplus_version IS NOT NULL
   AND str_known_region IS NOT NULL
   THEN
      rec := cipsrv_engine.determine_grid_srid(
          p_geometry        := NULL
         ,p_nhdplus_version := str_nhdplus_version
         ,p_known_region    := str_known_region
      );
      int_srid := rec.out_srid;
      
   ELSIF int_srid IS NULL
   AND str_nhdplus_version IS NOT NULL
   AND str_known_region IS NULL
   AND sdo_geometry IS NOT NULL
   THEN
      rec := cipsrv_engine.determine_grid_srid(
          p_geometry        := sdo_geometry
         ,p_nhdplus_version := str_nhdplus_version
         ,p_known_region    := NULL
      );
      int_srid := rec.out_srid;
      str_known_region := rec.out_srid::VARCHAR;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for point indexing_method override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'point_indexing_method' IS NOT NULL
   THEN
      str_point_indexing_method := json_feature->'properties'->>'point_indexing_method';
      
   ELSIF p_point_indexing_method IS NOT NULL
   THEN
      str_point_indexing_method := p_point_indexing_method;

   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for line indexing_method override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'line_indexing_method' IS NOT NULL
   THEN
      str_line_indexing_method := json_feature->'properties'->>'line_indexing_method';
      
   ELSIF p_line_indexing_method IS NOT NULL
   THEN
      str_line_indexing_method := p_line_indexing_method;

   END IF;
   
   IF has_properties
   AND json_feature->'properties'->'line_threshold' IS NOT NULL
   THEN
      num_line_threshold := json_feature->'properties'->'line_threshold';
      
   ELSIF p_line_threshold IS NOT NULL
   THEN
      num_line_threshold := p_line_threshold;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for ring indexing_method override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'ring_indexing_method' IS NOT NULL
   THEN
      str_ring_indexing_method := json_feature->'properties'->>'ring_indexing_method';
      
   ELSIF p_ring_indexing_method IS NOT NULL
   THEN
      str_ring_indexing_method := p_ring_indexing_method;

   END IF;
   
   IF has_properties
   AND json_feature->'properties'->'ring_areacat_threshold' IS NOT NULL
   THEN
      num_ring_areacat_threshold := json_feature->'properties'->'ring_areacat_threshold';
      
   ELSIF p_ring_areacat_threshold IS NOT NULL
   THEN
      num_ring_areacat_threshold := p_ring_areacat_threshold;
      
   END IF;
   
   IF has_properties
   AND json_feature->'properties'->'ring_areaevt_threshold' IS NOT NULL
   THEN
      num_ring_areaevt_threshold := json_feature->'properties'->'ring_areaevt_threshold';
      
   ELSIF p_ring_areaevt_threshold IS NOT NULL
   THEN
      num_ring_areaevt_threshold := p_ring_areaevt_threshold;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for area indexing_method override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'area_indexing_method' IS NOT NULL
   THEN
      str_area_indexing_method := json_feature->'properties'->>'area_indexing_method';
      
   ELSIF p_area_indexing_method IS NOT NULL
   THEN
      str_area_indexing_method := p_area_indexing_method;

   END IF;
   
   IF has_properties
   AND json_feature->'properties'->'areacat_threshold' IS NOT NULL
   THEN
      num_areacat_threshold := json_feature->'properties'->'areacat_threshold';
      
   ELSIF p_areacat_threshold IS NOT NULL
   THEN
      num_areacat_threshold := p_areacat_threshold;
      
   END IF;
   
   IF has_properties
   AND json_feature->'properties'->'areaevt_threshold' IS NOT NULL
   THEN
      num_areaevt_threshold := json_feature->'properties'->'areaevt_threshold';
      
   ELSIF p_areaevt_threshold IS NOT NULL
   THEN
      num_areaevt_threshold := p_areaevt_threshold;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Measure geometry size
   ----------------------------------------------------------------------------
   IF int_srid IS NOT NULL
   AND ST_GeometryType(sdo_geometry) IN ('ST_LineString','ST_MultiLineString')
   THEN
      num_line_lengthkm := ROUND(ST_Length(ST_Transform(
          sdo_geometry
         ,int_srid
      ))::NUMERIC * 0.001,8);
               
   ELSIF int_srid IS NOT NULL
   AND ST_GeometryType(sdo_geometry) IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      num_area_areasqkm := ROUND(ST_Area(ST_Transform(
          sdo_geometry
         ,int_srid
      ))::NUMERIC / 1000000,8);
      
   END IF;

   ----------------------------------------------------------------------------
   -- Create the object
   ----------------------------------------------------------------------------
   obj_rez := (
       str_globalid
      ,ST_GeometryType(sdo_geometry)
      ,sdo_geometry
      ,num_line_lengthkm
      ,num_area_areasqkm
      ,boo_isring
      ,json_feature->'properties'
      ,str_source_featureid
      ,str_permid_joinkey
      ,str_nhdplus_version
      ,str_known_region
      ,int_srid
      ,NULL
      ,NULL
      
      ,str_point_indexing_method
      
      ,str_line_indexing_method
      ,num_line_threshold
      ,NULL
      
      ,str_ring_indexing_method
      ,num_ring_areacat_threshold
      ,NULL
      ,num_ring_areaevt_threshold
      ,NULL
      
      ,str_area_indexing_method
      ,num_areacat_threshold
      ,NULL
      ,num_areaevt_threshold
      ,NULL
   )::cipsrv_engine.cip_feature;

   RETURN ARRAY[obj_rez];   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.jsonb2feature(
    JSONB
   ,GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   
   ,VARCHAR
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.jsonb2feature(
    JSONB
   ,GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   
   ,VARCHAR
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/jsonb2features.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.jsonb2features';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.jsonb2features(
    IN  p_features                       JSONB
   ,IN  p_nhdplus_version                VARCHAR DEFAULT NULL
   ,IN  p_known_region                   VARCHAR DEFAULT NULL
   ,IN  p_int_srid                       INTEGER DEFAULT NULL
   
   ,IN  p_default_point_indexing_method  VARCHAR DEFAULT NULL
   
   ,IN  p_default_line_indexing_method   VARCHAR DEFAULT NULL
   ,IN  p_default_line_threshold         NUMERIC DEFAULT NULL
   
   ,IN  p_default_ring_indexing_method   VARCHAR DEFAULT NULL
   ,IN  p_default_ring_areacat_threshold NUMERIC DEFAULT NULL
   ,IN  p_default_ring_areaevt_threshold NUMERIC DEFAULT NULL
   
   ,IN  p_default_area_indexing_method   VARCHAR DEFAULT NULL
   ,IN  p_default_areacat_threshold      NUMERIC DEFAULT NULL
   ,IN  p_default_areaevt_threshold      NUMERIC DEFAULT NULL
   
) RETURNS cipsrv_engine.cip_feature[]
VOLATILE
AS $BODY$ 
DECLARE
   obj_rez cipsrv_engine.cip_feature[];
   ary_rez cipsrv_engine.cip_feature[];
   str_nhdplus_version                VARCHAR;
   str_known_region                   VARCHAR;
   int_srid                           INTEGER;
   
   str_default_point_indexing_method  VARCHAR;
   
   str_default_line_indexing_method   VARCHAR;
   num_default_line_threshold         NUMERIC;
   
   str_default_ring_indexing_method   VARCHAR;
   num_default_ring_areacat_threshold NUMERIC;
   num_default_ring_areaevt_threshold NUMERIC;
   
   str_default_area_indexing_method   VARCHAR;
   num_default_areacat_threshold      NUMERIC;
   num_default_areaevt_threshold      NUMERIC;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_features IS NULL
   OR ( JSONB_TYPEOF(p_features) = 'array'
   AND JSONB_ARRAY_LENGTH(p_features) = 0 )
   THEN
      RETURN ary_rez;
      
   END IF;
   
   str_nhdplus_version                := p_nhdplus_version;
   str_known_region                   := p_known_region;
   int_srid                           := p_int_srid;
   
   str_default_point_indexing_method  := p_default_point_indexing_method;
   
   str_default_line_indexing_method   := p_default_line_indexing_method;
   num_default_line_threshold         := p_default_line_threshold;
   
   str_default_ring_indexing_method   := p_default_ring_indexing_method;
   num_default_ring_areacat_threshold := p_default_ring_areacat_threshold;
   num_default_ring_areaevt_threshold := p_default_ring_areaevt_threshold;

   str_default_area_indexing_method   := p_default_area_indexing_method;
   num_default_areacat_threshold      := p_default_areacat_threshold;
   num_default_areaevt_threshold      := p_default_areaevt_threshold;

   ----------------------------------------------------------------------------
   -- Build the features
   ----------------------------------------------------------------------------
   IF JSONB_TYPEOF(p_features) = 'object'
   AND p_features->>'type' IN ('Point','LineString','Polygon','MultiPoint','MultiLineString','MultiPolygon','GeometryCollection')
   THEN
      obj_rez := cipsrv_engine.jsonb2feature(
          p_feature               := JSONB_BUILD_OBJECT(
             'type',     'Feature'
            ,'geometry', p_features
          )
         ,p_nhdplus_version        := str_nhdplus_version
         ,p_known_region           := str_known_region
         ,p_int_srid               := int_srid
         
         ,p_point_indexing_method  := str_default_point_indexing_method
         
         ,p_line_indexing_method   := str_default_line_indexing_method
         ,p_line_threshold         := num_default_line_threshold
         
         ,p_ring_indexing_method   := str_default_ring_indexing_method
         ,p_ring_areacat_threshold := num_default_ring_areacat_threshold
         ,p_ring_areaevt_threshold := num_default_ring_areaevt_threshold
         
         ,p_area_indexing_method   := str_default_area_indexing_method
         ,p_areacat_threshold      := num_default_areacat_threshold
         ,p_areaevt_threshold      := num_default_areaevt_threshold
      );
      
      ary_rez := cipsrv_engine.featurecat(ary_rez,obj_rez);
   
   ELSIF JSONB_TYPEOF(p_features) = 'object'
   AND p_features->>'type' = 'Feature'
   THEN
      obj_rez := cipsrv_engine.jsonb2feature(
          p_feature                := p_features
         ,p_nhdplus_version        := str_nhdplus_version
         ,p_known_region           := str_known_region
         ,p_int_srid               := int_srid
         
         ,p_point_indexing_method  := str_default_point_indexing_method
         
         ,p_line_indexing_method   := str_default_line_indexing_method
         ,p_line_threshold         := num_default_line_threshold
         
         ,p_ring_indexing_method   := str_default_ring_indexing_method
         ,p_ring_areacat_threshold := num_default_ring_areacat_threshold
         ,p_ring_areaevt_threshold := num_default_ring_areaevt_threshold
         
         ,p_area_indexing_method   := str_default_area_indexing_method
         ,p_areacat_threshold      := num_default_areacat_threshold
         ,p_areaevt_threshold      := num_default_areaevt_threshold
          
      );
      
      ary_rez := cipsrv_engine.featurecat(ary_rez,obj_rez);
   
   ELSIF JSONB_TYPEOF(p_features) = 'object'
   AND p_features->>'type' = 'FeatureCollection'
   THEN
      FOR i IN 1 .. JSONB_ARRAY_LENGTH(p_features->'features')
      LOOP
         obj_rez := cipsrv_engine.jsonb2feature(
             p_feature                := p_features->'features'->i-1
            ,p_nhdplus_version        := str_nhdplus_version
            ,p_known_region           := str_known_region
            ,p_int_srid               := int_srid
            
            ,p_point_indexing_method  := str_default_point_indexing_method
            
            ,p_line_indexing_method   := str_default_line_indexing_method
            ,p_line_threshold         := num_default_line_threshold
            
            ,p_ring_indexing_method   := str_default_ring_indexing_method
            ,p_ring_areacat_threshold := num_default_ring_areacat_threshold
            ,p_ring_areaevt_threshold := num_default_ring_areaevt_threshold
            
            ,p_area_indexing_method   := str_default_area_indexing_method
            ,p_areacat_threshold      := num_default_areacat_threshold
            ,p_areaevt_threshold      := num_default_areaevt_threshold
            
         );
      
         ary_rez := cipsrv_engine.featurecat(ary_rez,obj_rez);
   
      END LOOP;
      
   ELSE
      RAISE EXCEPTION 'input jsonb is not geojson %', p_features;
   
   END IF;
   
   RETURN ary_rez;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.jsonb2features(
    JSONB
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   
   ,VARCHAR
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.jsonb2features(
    JSONB
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   
   ,VARCHAR
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/lrs_intersection.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.lrs_intersection';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.lrs_intersection(
    IN  p_geometry1          GEOMETRY
   ,IN  p_geometry2          GEOMETRY
) RETURNS GEOMETRY 
AS
$BODY$ 
DECLARE
   sdo_intersection GEOMETRY;
   sdo_initial      GEOMETRY;
   sdo_newinter     GEOMETRY;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF ST_GeometryType(p_geometry1) <> 'ST_LineString'
   OR ST_M(ST_StartPoint(p_geometry1)) IS NULL
   THEN
      RAISE EXCEPTION 'geometry 1 must be single LRS linestring';
      
   END IF;
   
   IF ST_GeometryType(p_geometry2) NOT IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      RAISE EXCEPTION 'geometry 2 must be a polygon or multipolygon';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Do the intersection
   ----------------------------------------------------------------------------
   sdo_intersection := ST_Intersection(
       p_geometry1
      ,p_geometry2
   );
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Now see what we got
   ----------------------------------------------------------------------------
   IF ST_GeometryType(sdo_intersection) IS NULL
   THEN
      RETURN NULL;
      
   ELSIF ST_GeometryType(sdo_intersection) = 'ST_MultiPoint'
   THEN
      RETURN NULL;
      
   ELSIF ST_GeometryType(sdo_intersection) IN (
       'ST_LineString'
      ,'ST_GeometryCollection'
      ,'ST_MultiLineString'
   )
   THEN
      NULL;  -- Do nothing
      
   ELSE
      RAISE EXCEPTION 
          'intersection returned component gtype %'
         , ST_GeometryType(sdo_intersection);
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Pick out the linestrings
   ----------------------------------------------------------------------------
   FOR i IN 1 .. ST_NumGeometries(sdo_intersection)
   LOOP
      sdo_initial := ST_GeometryN(sdo_intersection,i);
      
      IF ST_GeometryType(sdo_initial) = 'ST_LineString'
      THEN
         sdo_initial := cipsrv_engine.overlay_measures(
             p_geometry1 := sdo_initial
            ,p_geometry2 := p_geometry1
         );

         IF sdo_newinter IS NULL
         THEN
            sdo_newinter := sdo_initial;
            
         ELSE
            sdo_newinter := cipsrv_engine.safe_concatenate_geom_segments(
                sdo_newinter
               ,sdo_initial
            );
            
         END IF;
         
      END IF;
   
   END LOOP;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Final check and then return the results
   --------------------------------------------------------------------------
   IF ST_GeometryType(sdo_newinter) NOT IN ('ST_LineString','ST_MultiLineString')
   THEN
      RAISE EXCEPTION 'unable to process geometry';
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 60
   -- Return what we got
   --------------------------------------------------------------------------
   RETURN sdo_newinter;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.lrs_intersection(
    GEOMETRY
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.lrs_intersection(
    GEOMETRY
   ,GEOMETRY
) TO PUBLIC;
--******************************--
----- functions/measure_lengthkm.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.measure_lengthkm';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.measure_lengthkm(
    IN  p_geometry                GEOMETRY
   ,IN  p_nhdplus_version         VARCHAR
   ,IN  p_default_nhdplus_version VARCHAR DEFAULT NULL
   ,IN  p_known_region            VARCHAR DEFAULT NULL
   ,IN  p_default_known_region    VARCHAR DEFAULT NULL
) RETURNS NUMERIC
STABLE
AS $BODY$ 
DECLARE
   str_nhdplus_version VARCHAR;
   str_known_region    VARCHAR;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_geometry IS NULL
   THEN
      RAISE EXCEPTION 'input geometry required';
      
   END IF;
   
   IF p_nhdplus_version IS NULL
   THEN
      IF p_default_nhdplus_version IS NULL
      THEN
         RAISE EXCEPTION 'nhdplus version required';
      
      ELSE
         str_nhdplus_version := p_default_nhdplus_version;
         
      END IF;
      
   ELSE
      str_nhdplus_version := p_nhdplus_version; 
   
   END IF;
   
   str_known_region := p_known_region;
   IF str_known_region IS NULL
   THEN
      str_known_region := p_default_known_region;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine the region from geometry if known region value not provided
   ----------------------------------------------------------------------------
   IF str_nhdplus_version IN ('nhdplus_m','MR')
   THEN
      RETURN cipsrv_nhdplus_m.measure_lengthkm(
          p_geometry     := p_geometry
         ,p_known_region := str_known_region
      );
   
   ELSIF str_nhdplus_version IN ('nhdplus_h','HR')
   THEN
      RETURN cipsrv_nhdplus_h.measure_lengthkm(
          p_geometry     := p_geometry
         ,p_known_region := str_known_region
      );
   
   ELSE
      RAISE EXCEPTION 'unknown nhdplus version %',str_nhdplus_version;
   
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.measure_lengthkm(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.measure_lengthkm(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/measure_areasqkm.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.measure_areasqkm';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.measure_areasqkm(
    IN  p_geometry                GEOMETRY
   ,IN  p_nhdplus_version         VARCHAR
   ,IN  p_default_nhdplus_version VARCHAR DEFAULT NULL
   ,IN  p_known_region            VARCHAR DEFAULT NULL
   ,IN  p_default_known_region    VARCHAR DEFAULT NULL
) RETURNS NUMERIC
STABLE
AS $BODY$ 
DECLARE
   str_nhdplus_version VARCHAR;
   str_known_region    VARCHAR;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_geometry IS NULL
   THEN
      RAISE EXCEPTION 'input geometry required';
      
   END IF;
   
   IF p_nhdplus_version IS NULL
   THEN
      IF p_default_nhdplus_version IS NULL
      THEN
         RAISE EXCEPTION 'nhdplus version required';
      
      ELSE
         str_nhdplus_version := p_default_nhdplus_version;
         
      END IF;
      
   ELSE
      str_nhdplus_version := p_nhdplus_version; 
   
   END IF;
   
   str_known_region := p_known_region;
   IF str_known_region IS NULL
   THEN
      str_known_region := p_default_known_region;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine the region from geometry if known region value not provided
   ----------------------------------------------------------------------------
   IF str_nhdplus_version IN ('nhdplus_m','MR')
   THEN
      RETURN cipsrv_nhdplus_m.measure_areasqkm(
          p_geometry     := p_geometry
         ,p_known_region := str_known_region
      );
   
   ELSIF str_nhdplus_version IN ('nhdplus_h','HR')
   THEN
      RETURN cipsrv_nhdplus_h.measure_areasqkm(
          p_geometry     := p_geometry
         ,p_known_region := str_known_region
      );
   
   ELSE
      RAISE EXCEPTION 'unknown nhdplus version %',str_nhdplus_version;
   
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.measure_areasqkm(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR    
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.measure_areasqkm(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/overlay_measures.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.overlay_measures';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.overlay_measures(
    IN  p_geometry1           GEOMETRY
   ,IN  p_geometry2           GEOMETRY
) RETURNS GEOMETRY 
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_input_start   GEOMETRY;
   sdo_input_end     GEOMETRY;
   num_start_meas    NUMERIC;
   num_end_meas      NUMERIC;
   sdo_lrs_output    GEOMETRY;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF ST_GeometryType(p_geometry1)  <> 'ST_LineString'
   THEN
      RAISE EXCEPTION 'geometry 1 must a single linestring';
      
   END IF;
   
   IF ST_GeometryType(p_geometry2) <> 'ST_LineString'
   OR ST_M(ST_StartPoint(p_geometry2)) IS NULL
   THEN
      RAISE EXCEPTION 'geometry 2 must be single LRS linestring';
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Collect the start and end points of the input geometry
   --------------------------------------------------------------------------
   sdo_input_start := ST_StartPoint(p_geometry1);
   sdo_input_end   := ST_EndPoint(p_geometry1);
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Collect the start and end measure of the input geometry on the lrs
   --------------------------------------------------------------------------
   num_start_meas := ST_InterpolatePoint(
       p_geometry2
      ,sdo_input_start
   );
      
   num_end_meas := ST_InterpolatePoint(
       p_geometry2
      ,sdo_input_end
   );
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Build the new LRS string from the measures
   --------------------------------------------------------------------------
   sdo_lrs_output := ST_AddMeasure(
       p_geometry1
      ,num_start_meas
      ,num_end_meas
   );
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Check to see if the geometry is backwards
   --------------------------------------------------------------------------
   IF num_start_meas < num_end_meas
   THEN
      sdo_lrs_output := cipsrv_engine.reverse_linestring(
          p_geometry := sdo_lrs_output
      );
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 60
   -- Return the results
   --------------------------------------------------------------------------
   RETURN sdo_lrs_output;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.overlay_measures(
    GEOMETRY
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.overlay_measures(
    GEOMETRY
   ,GEOMETRY
) TO PUBLIC;
--******************************--
----- functions/parse_catchment_filter.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.parse_catchment_filter';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.parse_catchment_filter(
    IN  p_catchment_filter             VARCHAR[]
   ,OUT out_filter_by_state            BOOLEAN
   ,OUT out_state_filters              VARCHAR[]
   ,OUT out_filter_by_tribal           BOOLEAN
   ,OUT out_filter_by_notribal         BOOLEAN
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
IMMUTABLE
AS $BODY$ 
DECLARE 
   rec           RECORD;
   sdo_geom      GEOMETRY;
   num_lengthkm  NUMERIC;
   num_areasqkm  NUMERIC;
   ary_states    VARCHAR[];
   
BEGIN

   out_return_code        := 0;
   out_filter_by_state    := FALSE;
   out_filter_by_tribal   := FALSE;
   out_filter_by_notribal := FALSE;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   ary_states := ARRAY[
       'AL','AK','AS','AZ','AR','CA','CO','CT','DE','DC','FL','GA','GU','HI'
      ,'ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MO','MP'
      ,'MS','MT','NE','NV','NH','NJ','NM','NY','NC','ND','MP','OH','OK','OR'
      ,'PA','PR','RI','SC','SD','TN','TX','UT','VT','VI','VA','WA','WV','WI'
      ,'WY'
   ];
   
   IF p_catchment_filter IS NOT NULL
   AND array_length(p_catchment_filter,1) > 0
   THEN
      FOR i IN 1 .. array_length(p_catchment_filter,1)
      LOOP
         IF UPPER(p_catchment_filter[i]) IN ('ALLTRIBES','TRIBAL')
         THEN
            out_filter_by_tribal   := TRUE;
            out_filter_by_notribal := FALSE;
            
         ELSIF UPPER(p_catchment_filter[i]) IN ('NOTRIBES','NOTRIBAL')
         THEN
            out_filter_by_tribal   := FALSE;
            out_filter_by_notribal := TRUE;
            
         ELSIF UPPER(p_catchment_filter[i]) = ANY(ary_states)
         THEN
            out_filter_by_state := TRUE;
            out_state_filters := array_append(out_state_filters,UPPER(p_catchment_filter[i]));
         
         END IF;
         
      END LOOP;
      
   END IF;
   
   RETURN;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.parse_catchment_filter(
    VARCHAR[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.parse_catchment_filter(
    VARCHAR[]
) TO PUBLIC;

--******************************--
----- functions/preprocess2summary.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.preprocess2summary';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.preprocess2summary(
    IN  p_features           cipsrv_engine.cip_feature[]
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE
   obj_rez JSONB;
   int_point_count INTEGER;
   int_line_count  INTEGER;
   int_area_count  INTEGER;
   
BEGIN

   IF p_features IS NULL
   OR array_length(p_features,1) = 0
   THEN
      RETURN NULL;
      
   END IF;
   
   int_point_count := 0;
   int_line_count  := 0;
   int_area_count  := 0;
   
   FOR i IN 1 .. array_length(p_features,1)
   LOOP
      IF p_features[i].gtype IN ('ST_Point','ST_MultiPoint')
      THEN
         int_point_count := int_point_count + 1;
         
      ELSIF p_features[i].gtype IN ('ST_LineString','ST_MultiLineString')
      THEN
         int_line_count := int_line_count + 1;
         
      ELSIF p_features[i].gtype IN ('ST_Polygon','ST_MultiPolygon')
      THEN
         int_area_count := int_area_count + 1;
         
      END IF;
   
   END LOOP;
   
   obj_rez := JSONB_BUILD_OBJECT(
       'point_count', int_point_count
      ,'line_count' , int_line_count
      ,'area_count' , int_area_count
   );
   
   RETURN JSONB_BUILD_OBJECT(
       'input_features', obj_rez
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.preprocess2summary(
    cipsrv_engine.cip_feature[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.preprocess2summary(
    cipsrv_engine.cip_feature[]
) TO PUBLIC;

--******************************--
----- functions/raster_raindrop.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.raster_raindrop';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.raster_raindrop(
    IN  p_raster           RASTER
   ,IN  p_columnX          INTEGER
   ,IN  p_rowY             INTEGER
) RETURNS GEOMETRY
IMMUTABLE
AS
$BODY$
DECLARE
   r             RECORD;
   sdo_output    GEOMETRY;
   sdo_temp      GEOMETRY;
   int_column_x  INTEGER := p_columnX;
   int_row_y     INTEGER := p_rowY;
   int_stop      INTEGER;
   int_value     INTEGER;
   int_width     INTEGER;
   int_height    INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_columnX IS NULL
   OR p_rowY    IS NULL
   OR p_raster  IS NULL
   THEN
      RAISE EXCEPTION 'err';
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- Get the raster size
   ----------------------------------------------------------------------------
   int_width   := ST_Width(p_raster);
   int_height  := ST_Height(p_raster);
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Test the start point on the grid
   ----------------------------------------------------------------------------
   sdo_temp := ST_PixelAsCentroid(
       p_raster
      ,int_column_x
      ,int_row_y
   );
   --RAISE WARNING '% % %', int_column_x,int_row_y,ST_AsEWKT(ST_Transform(sdo_temp,4269));
   
   IF sdo_temp IS NULL
   THEN
      RAISE EXCEPTION 'unable to obtain coordinates from raster with X and Y provided';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Walk the grid downstream
   ----------------------------------------------------------------------------
   int_stop := 10000;
   WHILE int_stop > 0
   LOOP
      sdo_temp := ST_PixelAsCentroid(
          p_raster
         ,int_column_x
         ,int_row_y
      );
      
      int_value := ST_Value(
          rast     := p_raster
         ,band     := 1
         ,x        := int_column_x
         ,y        := int_row_y
         ,exclude_nodata_value := true
      );
      
      CASE int_value
      WHEN 1
      THEN
         int_column_x := int_column_x + 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 2
      THEN
         int_column_x := int_column_x + 1;
         int_row_y    := int_row_y    + 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 4
      THEN
         int_row_y    := int_row_y    + 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 8
      THEN
         int_column_x := int_column_x - 1;
         int_row_y    := int_row_y    + 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 16
      THEN
         int_column_x := int_column_x - 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 32
      THEN
         int_column_x := int_column_x - 1;
         int_row_y    := int_row_y    - 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 64
      THEN
         int_row_y    := int_row_y    - 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 128
      THEN
         int_column_x := int_column_x + 1;
         int_row_y    := int_row_y    - 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 0
      THEN
         int_stop := 0;
         
      WHEN 255
      THEN
         int_stop := 0;
         
      ELSE
         int_stop := 0;
         
      END CASE;

      IF int_column_x < 1
      OR int_row_y < 1
      OR int_column_x > int_width
      OR int_row_y > int_height
      THEN
         int_stop := 0;
         
      END IF;
      
      int_stop := int_stop - 1;
   
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN sdo_output;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.raster_raindrop(
    RASTER
   ,INTEGER
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.raster_raindrop(
    RASTER
   ,INTEGER
   ,INTEGER
) TO PUBLIC;

--******************************--
----- functions/remove_holes.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.remove_holes';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.remove_holes(
    IN  p_input          GEOMETRY
   ,IN  p_threshold_sqkm NUMERIC DEFAULT NULL
) RETURNS GEOMETRY
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_results   GEOMETRY;
   ary_testing   GEOMETRY[];
   ary_clean     GEOMETRY[];
   int_counter   INTEGER;
   int_counter2  INTEGER;
   outer_ring    GEOMETRY;
   inner_rings   GEOMETRY[];
   inner_ring    GEOMETRY;
   num_ring_size NUMERIC;
   int_cnt_ring  INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Short circuit if not polygon or polygon only has a single outer ring
   ----------------------------------------------------------------------------
   IF p_input IS NULL OR ST_ISEMPTY(p_input)
   THEN
      RETURN NULL;
      
   END IF;   
   
   IF GEOMETRYTYPE(p_input) NOT IN ('POLYGON','MULTIPOLYGON')
   OR ST_NRINGS(p_input) = 1
   THEN
      RETURN p_input;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- To remove all inner rings, just grab outer rings sorted by size
   ----------------------------------------------------------------------------
   IF  p_threshold_sqkm IS NULL
   THEN
      SELECT
      ARRAY_AGG(a.shape)
      INTO
      ary_testing
      FROM (
         SELECT
         aa.shape
         FROM (
            SELECT
            ST_MAKEPOLYGON(aaa.shape) AS shape
            FROM (
               SELECT
               ST_EXTERIORRING(aaaa.geom) AS shape
               FROM
               ST_DUMP(p_input) aaaa
            ) aaa
         ) aa
         ORDER BY
         ST_AREA(aa.shape) DESC
      ) a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Exit early if only one outer ring found
   ----------------------------------------------------------------------------
      IF ARRAY_LENGTH(ary_testing,1) = 1
      THEN
         RETURN ary_testing[1];
         
      END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Flush away any exterior rings inside largest exterior ring
   -- This could be more robust for whacked out multi-polygons having lakes
   -- with islands with ponds with even smaller islands
   ----------------------------------------------------------------------------
      ary_clean[1] := ary_testing[1];
      
      int_counter := 2;
      FOR i IN 2 .. ARRAY_LENGTH(ary_testing,1)
      LOOP
         IF NOT ST_WITHIN(ary_testing[i],ary_clean[1])
         THEN
            ary_clean[int_counter] := ary_testing[i];
            int_counter := int_counter + 1;
         
         END IF;
      
      END LOOP;
      
   ELSE
   ----------------------------------------------------------------------------
   -- Step 50
   -- Break down multis into array of polygons sorted by size of outer rings
   ----------------------------------------------------------------------------
      SELECT
      ARRAY_AGG(a.shape)
      INTO
      ary_testing
      FROM (
         SELECT
         aa.shape
         FROM (
            SELECT
            aaa.geom AS shape
            FROM
            ST_DUMP(p_input) aaa
         ) aa
         ORDER BY
         ST_AREA(
            ST_MAKEPOLYGON(ST_EXTERIORRING(aa.shape))
         ) DESC
      ) a;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Remake each polygon applying threshold
   ----------------------------------------------------------------------------      
      int_counter := 1;
      FOR i IN 1 .. ARRAY_LENGTH(ary_testing,1)
      LOOP
         int_cnt_ring := ST_NUMINTERIORRINGS(ary_testing[i]);
         
         IF int_cnt_ring = 0
         THEN
            ary_clean[int_counter] := ary_testing[i];
            
         ELSE
            outer_ring   := ST_EXTERIORRING(ary_testing[i]);
            inner_rings  := ARRAY[]::GEOMETRY[];
         
            int_counter2 := 1;
            FOR j IN 1 .. int_cnt_ring
            LOOP
               inner_ring := ST_INTERIORRINGN(ary_testing[i],j);
               num_ring_size := ST_AREA(ST_TRANSFORM(ST_MAKEPOLYGON(inner_ring),4326)::GEOGRAPHY)::NUMERIC / 1000000;
               
               IF num_ring_size > p_threshold_sqkm
               THEN
                  inner_rings[int_counter2] := inner_ring;
                  int_counter2 := int_counter2 + 1;
               
               END IF;
            
            END LOOP;
            
            IF ARRAY_LENGTH(inner_rings,1) = 0
            THEN
               ary_clean[int_counter] := ST_MAKEPOLYGON(outer_ring);
               
            ELSE
               ary_clean[int_counter] := ST_MAKEPOLYGON(outer_ring,inner_rings);
            
            END IF;
         
         END IF;

         int_counter := int_counter + 1;         
      
      END LOOP;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Rebuild into geometry
   ----------------------------------------------------------------------------
   sdo_results := ST_COLLECT(ary_clean);
   
   RETURN sdo_results;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.remove_holes';
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

--******************************--
----- functions/reverse_linestring.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.reverse_linestring';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.reverse_linestring(
    IN  p_geometry          GEOMETRY
) RETURNS GEOMETRY
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_lrs_output    GEOMETRY;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF ST_GeometryType(p_geometry)  <> 'ST_LineString'
   THEN
      RAISE EXCEPTION 'geometry must a single linestring';
      
   END IF;
   
   IF ST_M(
      ST_StartPoint(p_geometry)
   ) IS NULL
   THEN
      RETURN ST_Reverse(p_geometry);
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Reverse the linestring taking measures along for a ride
   --------------------------------------------------------------------------
   SELECT
   ST_MakeLine(a.geom)
   INTO
   sdo_lrs_output
   FROM (
      SELECT
      aa.geom
      FROM (
         SELECT (ST_DumpPoints(
            p_geometry
         )).*
      ) aa
      ORDER BY
      aa.path[1] DESC
   ) a;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Return the results
   --------------------------------------------------------------------------
   RETURN sdo_lrs_output;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.reverse_linestring(
    GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.reverse_linestring(
    GEOMETRY
) TO PUBLIC;
--******************************--
----- functions/safe_concatenate_geom_segments.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.safe_concatenate_geom_segments';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.safe_concatenate_geom_segments(
    IN  p_geometry1           GEOMETRY
   ,IN  p_geometry2           GEOMETRY
) RETURNS GEOMETRY 
AS
$BODY$ 
DECLARE
   sdo_array_in      GEOMETRY[];
   sdo_array_in2     GEOMETRY[];
   sdo_concatenate   GEOMETRY;
   sdo_output        GEOMETRY;
   num_remove1       NUMERIC;
   num_remove2       NUMERIC;
   int_counter       INTEGER;
   int_sanity        INTEGER := 0;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_geometry1 IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF p_geometry2 IS NULL
   THEN
      RETURN p_geometry1;
      
   END IF;

   IF ST_GeometryType(p_geometry1) NOT IN ('ST_LineString','ST_MultiLineString')
   OR NOT cipsrv_engine.is_lrs(p_geometry := p_geometry1)
   THEN
      RAISE EXCEPTION 'geometry 1 must be LRS linestring';
      
   END IF;
   
   IF ST_GeometryType(p_geometry2) NOT IN ('ST_LineString','ST_MultiLineString')
   OR NOT cipsrv_engine.is_lrs(p_geometry := p_geometry2)
   THEN
      RAISE EXCEPTION 'geometry 2 must be LRS linestring';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Do the easiest solution of two single linestrings
   ----------------------------------------------------------------------------
   IF  ST_GeometryType(p_geometry1) = 'ST_LineString'
   AND ST_GeometryType(p_geometry2) = 'ST_LineString'
   THEN
      IF ST_M(ST_EndPoint(p_geometry1)) = ST_M(ST_StartPoint(p_geometry2))
      THEN
         RETURN ST_MakeLine(
             p_geometry1
            ,p_geometry2
         );
         
      ELSIF ST_M(ST_EndPoint(p_geometry2)) = ST_M(ST_StartPoint(p_geometry1))
      THEN
         RETURN ST_MakeLine(
             p_geometry2
            ,p_geometry1
         );
         
      ELSE
         RETURN ST_Collect(
             p_geometry2
            ,p_geometry1
         );
      
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Create an array of all the linestrings in both geometries
   ----------------------------------------------------------------------------
   SELECT
   array_agg(a.geom)
   INTO
   sdo_array_in
   FROM (
      SELECT (ST_Dump(
         ST_Collect(p_geometry1,p_geometry2)
      )).*
   ) a;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Set an anchor point for processing
   ----------------------------------------------------------------------------
   <<start_over>>
   LOOP
      num_remove1   := NULL;
      num_remove2   := NULL;
      sdo_array_in2 := NULL;
      
   ----------------------------------------------------------------------------
   -- Step 50
   -- Loop over all the linestrings and search for match
   ----------------------------------------------------------------------------
      <<outer_loop>>
      FOR i IN 1 .. array_length(sdo_array_in, 1)
      LOOP
         FOR j IN 1 .. array_length(sdo_array_in, 1)
         LOOP
            IF i <> j
            AND ST_M(ST_EndPoint(sdo_array_in[i])) = ST_M(ST_StartPoint(sdo_array_in[j]))
            THEN
               sdo_concatenate := ST_MakeLine(
                   sdo_array_in[i]
                  ,sdo_array_in[j]
               );
               
               IF ST_GeometryType(sdo_concatenate) = 'ST_LineString'
               THEN
                  num_remove1 := i;
                  num_remove2 := j;
                  EXIT outer_loop;
                  
               END IF;
               
            END IF;
            
         END LOOP;
         
      END LOOP outer_loop;

   --------------------------------------------------------------------------
   -- Step 60
   -- Bail if there are no matches in the mess
   --------------------------------------------------------------------------
      IF num_remove1 IS NULL
      THEN
         sdo_output := ST_Collect(sdo_array_in);
         
         IF ST_NumGeometries(sdo_output) = 1
         THEN
            RETURN ST_GeometryN(sdo_output,1);
         
         ELSE
            RETURN sdo_output;
         
         END IF;
         
      END IF;
  
   --------------------------------------------------------------------------
   -- Step 70
   -- Add match to start of array and remove parts from array
   --------------------------------------------------------------------------
      int_counter := 1;
      sdo_array_in2[int_counter] := sdo_concatenate;
      int_counter := int_counter + 1;
      
      FOR i IN 1 .. array_length(sdo_array_in,1)
      LOOP
         IF  i <> num_remove1
         AND i <> num_remove2
         THEN
            sdo_array_in2[int_counter] := sdo_array_in[i];
            int_counter := int_counter + 1;
            
         END IF;
         
      END LOOP;
      
      sdo_array_in := sdo_array_in2;
   
   --------------------------------------------------------------------------
   -- Step 80
   -- Check that loop is not stuck
   --------------------------------------------------------------------------
      IF int_sanity > array_length(sdo_array_in,1) * array_length(sdo_array_in,1)
      THEN
         sdo_output := ST_Collect(sdo_array_in);
         
         IF ST_NumGeometries(sdo_output) = 1
         THEN
            RETURN ST_GeometryN(sdo_output,1);
         
         ELSE
            RETURN sdo_output;
         
         END IF;
         
      END IF;
   
      int_sanity := int_sanity + 1;
      
   END LOOP start_over; 
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.safe_concatenate_geom_segments(
    GEOMETRY
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.safe_concatenate_geom_segments(
    GEOMETRY
   ,GEOMETRY
) TO PUBLIC;

--******************************--
----- functions/table_exists.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.table_exists';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.table_exists(
    IN  p_schema_name VARCHAR
   ,IN  p_table_name  VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_table_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for table
   ----------------------------------------------------------------------------
   SELECT 
   c.relname
   INTO str_table_name
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   WHERE  
       n.nspname = p_schema_name
   AND c.relname = p_table_name
   AND c.relkind = 'r';

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_table_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.table_exists(
    VARCHAR
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.table_exists(
    VARCHAR
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/resource_exists.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.resource_exists';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.resource_exists(
    IN  p_schema_name VARCHAR
   ,IN  p_table_name  VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_table_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for table
   ----------------------------------------------------------------------------
   SELECT 
   c.relname
   INTO str_table_name
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   WHERE  
       n.nspname = p_schema_name
   AND c.relname = p_table_name
   AND c.relkind IN ('r','m','v','p');

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_table_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ 
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.resource_exists';
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


--******************************--
----- functions/temp_table_exists.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.temp_table_exists';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.temp_table_exists(
   IN p_table_name VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_table_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for temp table
   ----------------------------------------------------------------------------
   SELECT 
    n.nspname
   INTO str_table_name
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   where 
       n.nspname like 'pg_temp_%'
   AND pg_catalog.pg_table_is_visible(c.oid)
   AND UPPER(relname) = UPPER(p_table_name);

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_table_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.temp_table_exists(
   VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.temp_table_exists(
   VARCHAR
) TO PUBLIC;

--******************************--
----- functions/unpackjsonb.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.unpackjsonb';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.unpackjsonb(
    IN  p_points                         JSONB
   ,IN  p_lines                          JSONB
   ,IN  p_areas                          JSONB
   ,IN  p_geometry                       JSONB
   ,IN  p_nhdplus_version                VARCHAR DEFAULT NULL
   ,IN  p_known_region                   VARCHAR DEFAULT NULL
   ,IN  p_int_srid                       INTEGER DEFAULT NULL
   
   ,IN  p_default_point_indexing_method  VARCHAR DEFAULT NULL
   
   ,IN  p_default_line_indexing_method   VARCHAR DEFAULT NULL
   ,IN  p_default_line_threshold         NUMERIC DEFAULT NULL
   
   ,IN  p_default_ring_indexing_method   VARCHAR DEFAULT NULL
   ,IN  p_default_ring_areacat_threshold NUMERIC DEFAULT NULL
   ,IN  p_default_ring_areaevt_threshold NUMERIC DEFAULT NULL
   
   ,IN  p_default_area_indexing_method   VARCHAR DEFAULT NULL
   ,IN  p_default_areacat_threshold      NUMERIC DEFAULT NULL
   ,IN  p_default_areaevt_threshold      NUMERIC DEFAULT NULL
   
   ,OUT out_return_code                  INTEGER
   ,OUT out_status_message               VARCHAR
   ,OUT out_features                     cipsrv_engine.cip_feature[]
)
IMMUTABLE
AS $BODY$ 
DECLARE 
   rec              RECORD;
   ary_points       cipsrv_engine.cip_feature[];
   ary_lines        cipsrv_engine.cip_feature[];
   ary_areas        cipsrv_engine.cip_feature[];
   str_known_region VARCHAR := p_known_region;
   int_srid         INTEGER := p_int_srid;
   
BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  p_points   IS NULL
   AND p_lines    IS NULL
   AND p_areas    IS NULL
   AND p_geometry IS NULL
   THEN
      RETURN;
      
   END IF;
   
   IF p_nhdplus_version IS NULL
   THEN
      RAISE EXCEPTION 'nhdplus version cannot be null';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Branch when geometry is provided
   ----------------------------------------------------------------------------
   IF p_geometry IS NOT NULL
   THEN
      out_features := cipsrv_engine.jsonb2features(
          p_features                      := p_geometry
         ,p_nhdplus_version               := p_nhdplus_version
         ,p_known_region                  := str_known_region
         ,p_int_srid                      := int_srid
         ,p_default_point_indexing_method := p_default_point_indexing_method
         ,p_default_line_indexing_method  := p_default_line_indexing_method
         ,p_default_ring_indexing_method  := p_default_ring_indexing_method
         ,p_default_area_indexing_method  := p_default_area_indexing_method
         ,p_default_line_threshold        := p_default_line_threshold
         ,p_default_areacat_threshold     := p_default_areacat_threshold
         ,p_default_areaevt_threshold     := p_default_areaevt_threshold
      );
   
   ELSE
      ary_points := cipsrv_engine.jsonb2features(
          p_features                      := p_points
         ,p_nhdplus_version               := p_nhdplus_version
         ,p_known_region                  := str_known_region
         ,p_int_srid                      := int_srid
         ,p_default_point_indexing_method := p_default_point_indexing_method
         ,p_default_line_indexing_method  := p_default_line_indexing_method
         ,p_default_ring_indexing_method  := p_default_ring_indexing_method
         ,p_default_area_indexing_method  := p_default_area_indexing_method
         ,p_default_line_threshold        := p_default_line_threshold
         ,p_default_areacat_threshold     := p_default_areacat_threshold
         ,p_default_areaevt_threshold     := p_default_areaevt_threshold
      );
      
      ary_lines := cipsrv_engine.jsonb2features(
          p_features                      := p_lines
         ,p_nhdplus_version               := p_nhdplus_version
         ,p_known_region                  := str_known_region
         ,p_int_srid                      := int_srid
         ,p_default_point_indexing_method := p_default_point_indexing_method
         ,p_default_line_indexing_method  := p_default_line_indexing_method
         ,p_default_ring_indexing_method  := p_default_ring_indexing_method
         ,p_default_area_indexing_method  := p_default_area_indexing_method
         ,p_default_line_threshold        := p_default_line_threshold
         ,p_default_areacat_threshold     := p_default_areacat_threshold
         ,p_default_areaevt_threshold     := p_default_areaevt_threshold
      );
      
      ary_areas := cipsrv_engine.jsonb2features(
          p_features                      := p_areas
         ,p_nhdplus_version               := p_nhdplus_version
         ,p_known_region                  := str_known_region
         ,p_int_srid                      := int_srid
         ,p_default_point_indexing_method := p_default_point_indexing_method
         ,p_default_line_indexing_method  := p_default_line_indexing_method
         ,p_default_ring_indexing_method  := p_default_ring_indexing_method
         ,p_default_area_indexing_method  := p_default_area_indexing_method
         ,p_default_line_threshold        := p_default_line_threshold
         ,p_default_areacat_threshold     := p_default_areacat_threshold
         ,p_default_areaevt_threshold     := p_default_areaevt_threshold
      );
      
      out_features := cipsrv_engine.featurecat(out_features,ary_points);
      out_features := cipsrv_engine.featurecat(out_features,ary_lines);
      out_features := cipsrv_engine.featurecat(out_features,ary_areas);
      
   END IF;
      
   -------------------------------------------------------------------------
   -- Ring Handling
   -------------------------------------------------------------------------
   IF out_features IS NOT NULL
   AND array_length(out_features,1) > 0
   THEN
      FOR i IN 1 .. array_length(out_features,1)
      LOOP
         IF out_features[i].isRing 
         AND out_features[i].ring_indexing_method != 'treat_as_lines'
         THEN
            out_features[i].geometry := ST_MakePolygon(out_features[i].geometry);
            out_features[i].gtype    := ST_GeometryType(out_features[i].geometry);
            out_features[i].converted_to_ring := TRUE;
            out_features[i].area_indexing_method := out_features[i].ring_indexing_method;
            out_features[i].areacat_threshold    := out_features[i].ring_areacat_threshold;
            out_features[i].areaevt_threshold    := out_features[i].ring_areaevt_threshold;
            
            out_features[i].lengthkm := NULL;
            out_features[i].areasqkm := ROUND(ST_Area(ST_Transform(
                out_features[i].geometry
               ,out_features[i].int_srid
            ))::NUMERIC / 1000000,8);
            
         END IF;
      
      END LOOP;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.unpackjsonb';
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

--******************************--
----- procedures/cipsrv_batch_index.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.cipsrv_batch_index';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP PROCEDURE IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE PROCEDURE cipsrv_engine.cipsrv_batch_index(
    IN  p_dataset_prefix                   VARCHAR
   ,OUT out_return_code                    INTEGER
   ,OUT out_status_message                 VARCHAR
   ,IN  p_override_default_nhdplus_version VARCHAR DEFAULT NULL
   ,IN  p_debug                            BOOLEAN DEFAULT FALSE
)
AS $BODY$ 
DECLARE
   rec                                RECORD;
   rec2                               RECORD;
   rec3                               RECORD;
   str_sql                            VARCHAR;
   str_dataset_prefix                 VARCHAR;
   str_control_dataset_prefix         VARCHAR;
   str_control_source_originator      VARCHAR;
   str_control_source_series          VARCHAR;
   str_geometry_clip                  VARCHAR;
   ary_geometry_clip                  VARCHAR[];
   str_geometry_clip_stage            VARCHAR;
   str_catchment_filter               VARCHAR;
   ary_catchment_filter               VARCHAR[];
   str_nhdplus_version                VARCHAR;
   str_default_nhdplus_version        VARCHAR;
   str_catchment_resolution           VARCHAR;
   str_xwalk_huc12_version            VARCHAR;
   
   str_point_indexing_method          VARCHAR;
   str_default_point_indexing_method  VARCHAR;
   
   str_line_indexing_method           VARCHAR;
   str_default_line_indexing_method   VARCHAR;
   num_line_threshold                 NUMERIC;
   num_default_line_threshold         NUMERIC;
   
   str_ring_indexing_method           VARCHAR;
   str_default_ring_indexing_method   VARCHAR;
   num_ring_areacat_threshold         NUMERIC;
   num_default_ring_areacat_threshold NUMERIC;
   num_ring_areaevt_threshold         NUMERIC;
   num_default_ring_areaevt_threshold NUMERIC;
   
   str_area_indexing_method           VARCHAR;
   str_default_area_indexing_method   VARCHAR;
   num_areacat_threshold              NUMERIC;
   num_default_areacat_threshold      NUMERIC;
   num_areaevt_threshold              NUMERIC;
   num_default_areaevt_threshold      NUMERIC;
   
   str_known_region                   VARCHAR;
   str_default_known_region           VARCHAR;
   str_username                       VARCHAR;
   dat_datecreated                    DATE;
   boo_filter_by_state                BOOLEAN;
   ary_state_filters                  VARCHAR[];
   boo_filter_by_tribal               BOOLEAN;
   boo_filter_by_notribal             BOOLEAN;
   int_count                          INTEGER;
   boo_isring                         BOOLEAN;
   num_point_indexing_return_code     INTEGER;
   str_point_indexing_status_message  VARCHAR;
   num_line_indexing_return_code      INTEGER;
   str_line_indexing_status_message   VARCHAR;
   num_ring_indexing_return_code      INTEGER;
   str_ring_indexing_status_message   VARCHAR;
   num_area_indexing_return_code      INTEGER;
   str_area_indexing_status_message   VARCHAR;
   geom_part                          GEOMETRY;
   num_line_lengthkm                  NUMERIC;
   num_area_areasqkm                  NUMERIC;
   int_cat_mr_count                   INTEGER;
   int_cat_hr_count                   INTEGER;
   
BEGIN

   out_return_code := cipsrv_engine.create_cip_batch_temp_tables();
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_dataset_prefix IS NULL
   THEN
      RAISE EXCEPTION 'err';
   
   END IF;
   
   str_dataset_prefix := LOWER(p_dataset_prefix);
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_control')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_control';
      
   END IF;
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_points')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_points';
      
   END IF;
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_lines')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_lines';
      
   END IF;
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_areas')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_areas';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Index source_featureid if needed
   ----------------------------------------------------------------------------
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_points',str_dataset_prefix || '_points_sfid')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_points_sfid ON cipsrv_upload.' || str_dataset_prefix || '_points(source_featureid)';
   
   END IF;
   
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_lines',str_dataset_prefix || '_lines_sfid')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_lines_sfid ON cipsrv_upload.' || str_dataset_prefix || '_lines(source_featureid)';
   
   END IF;
   
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_areas',str_dataset_prefix || '_areas_sfid')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_areas_sfid ON cipsrv_upload.' || str_dataset_prefix || '_areas(source_featureid)';
   
   END IF;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Add source_joinkey field if needed and index if needed
   ----------------------------------------------------------------------------
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_points','source_joinkey')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_dataset_prefix || '_points ADD COLUMN source_joinkey VARCHAR(40)';
   
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_lines','source_joinkey')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_dataset_prefix || '_lines ADD COLUMN source_joinkey VARCHAR(40)';
   
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_areas','source_joinkey')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_dataset_prefix || '_areas ADD COLUMN source_joinkey VARCHAR(40)';
   
   END IF;
   
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_points',str_dataset_prefix || '_points_sjk')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_points_sjk ON cipsrv_upload.' || str_dataset_prefix || '_points(source_joinkey)';
   
   END IF;
   
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_lines',str_dataset_prefix || '_lines_sjk')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_lines_sjk ON cipsrv_upload.' || str_dataset_prefix || '_lines(source_joinkey)';
   
   END IF;
   
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_areas',str_dataset_prefix || '_areas_sjk')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_areas_sjk ON cipsrv_upload.' || str_dataset_prefix || '_areas(source_joinkey)';
   
   END IF;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Add measurements field if needed
   ----------------------------------------------------------------------------
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_lines','lengthkm')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_dataset_prefix || '_lines ADD COLUMN lengthkm NUMERIC';
   
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_areas','areasqkm')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_dataset_prefix || '_areas ADD COLUMN areasqkm NUMERIC';
   
   END IF;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Analyze incoming tables
   ----------------------------------------------------------------------------
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_points';
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_lines';
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_areas';
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Create the sfid table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_sfid';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_sfid_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    objectid                      INTEGER     NOT NULL '
           || '   ,source_originator             VARCHAR(130) '
           || '   ,source_featureid              VARCHAR(100) '
           || '   ,source_featureid2             VARCHAR(100) '
           || '   ,source_series                 VARCHAR(100) '
           || '   ,source_subdivision            VARCHAR(100) '
           || '   ,source_joinkey                VARCHAR(40) NOT NULL '
           || '   ,start_date                    DATE '
           || '   ,end_date                      DATE '
           || '   ,src_point_count               INTEGER '
           || '   ,point_indexing_method_used    VARCHAR(40) '
           || '   ,point_indexing_return_code    INTEGER '
           || '   ,point_indexing_status_message VARCHAR(40) '
           || '   ,src_line_count                INTEGER '
           || '   ,line_indexing_method_used     VARCHAR(40) '
           || '   ,line_indexing_return_code     INTEGER '
           || '   ,line_indexing_status_message  VARCHAR(40) '
           || '   ,ring_indexing_method_used     VARCHAR(40) '
           || '   ,ring_indexing_return_code     INTEGER '
           || '   ,ring_indexing_status_message  VARCHAR(40) '
           || '   ,src_area_count                INTEGER '
           || '   ,area_indexing_method_used     VARCHAR(40) '
           || '   ,area_indexing_return_code     INTEGER '
           || '   ,area_indexing_status_message  VARCHAR(40) '
           || '   ,cat_mr_count                  INTEGER '
           || '   ,cat_hr_count                  INTEGER '
           || '   ,globalid                      VARCHAR(40) NOT NULL '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_sfid TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_sfid_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    source_featureid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_sfid_pk2 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    source_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_sfid_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_sfid_u02 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    globalid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_sfid_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Load the sfid table
   ----------------------------------------------------------------------------
   str_sql := 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    objectid '
           || '   ,source_originator '
           || '   ,source_series '
           || '   ,source_featureid '
           || '   ,source_joinkey '
           || '   ,src_point_count '
           || '   ,src_line_count '
           || '   ,src_area_count '
           || '   ,globalid '
           || ') '
           || 'SELECT '
           || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_sfid_seq'') '
           || ',a.source_originator '
           || ',a.source_series '
           || ',a.source_featureid '
           || ',''{'' || uuid_generate_v1() || ''}'' '
           || ',a.src_point_count '
           || ',a.src_line_count '
           || ',a.src_area_count '
           || ',''{'' || uuid_generate_v1() || ''}'' '
           || 'FROM ( '
           || '   SELECT '
           || '    aa.source_originator '
           || '   ,aa.source_series '
           || '   ,aa.source_featureid '
           || '   ,bb.src_point_count '
           || '   ,cc.src_line_count '
           || '   ,dd.src_area_count '
           || '   FROM ( '
           || '      SELECT '
           || '       aa1.source_originator '
           || '      ,aa1.source_series '
           || '      ,aa1.source_featureid '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_points aa1 '
           || '      UNION SELECT '
           || '       aa2.source_originator '
           || '      ,aa2.source_series '
           || '      ,aa2.source_featureid '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_lines aa2 '
           || '      UNION SELECT '
           || '       aa3.source_originator '
           || '      ,aa3.source_series '
           || '      ,aa3.source_featureid '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_areas aa3 '
           || '      GROUP BY 1,2,3 '
           || '   ) aa '
           || '   LEFT JOIN ( '
           || '      SELECT '
           || '       bbb.source_originator '
           || '      ,bbb.source_series '
           || '      ,bbb.source_featureid '
           || '      ,COUNT(*) AS src_point_count '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_points bbb '
           || '      GROUP BY '
           || '       bbb.source_originator '
           || '      ,bbb.source_series '
           || '      ,bbb.source_featureid '
           || '   ) bb '
           || '   ON '
           || '       aa.source_originator = bb.source_originator '
           || '   AND aa.source_series     = bb.source_series '
           || '   AND aa.source_featureid  = bb.source_featureid '
           || '   LEFT JOIN ( '
           || '      SELECT '
           || '       ccc.source_originator '
           || '      ,ccc.source_series '
           || '      ,ccc.source_featureid '
           || '      ,COUNT(*) AS src_line_count '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_lines ccc '
           || '      GROUP BY '
           || '       ccc.source_originator '
           || '      ,ccc.source_series '
           || '      ,ccc.source_featureid '
           || '   ) cc '
           || '   ON '
           || '       aa.source_originator = cc.source_originator '
           || '   AND aa.source_series     = cc.source_series '
           || '   AND aa.source_featureid  = cc.source_featureid '
           || '   LEFT JOIN ( '
           || '      SELECT '
           || '       ddd.source_originator '
           || '      ,ddd.source_series '
           || '      ,ddd.source_featureid '
           || '      ,COUNT(*) AS src_area_count '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_areas ddd '
           || '      GROUP BY '
           || '       ddd.source_originator '
           || '      ,ddd.source_series '
           || '      ,ddd.source_featureid '
           || '   ) dd '
           || '   ON '
           || '       aa.source_originator = dd.source_originator '
           || '   AND aa.source_series     = dd.source_series '
           || '   AND aa.source_featureid  = dd.source_featureid '
           || ') a ';
           
   EXECUTE str_sql;
   
   COMMIT;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Update the feature tables with new source_joinkeys
   ----------------------------------------------------------------------------
   str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_points a '
           || 'SET source_joinkey = ( '
           || '   SELECT '
           || '   b.source_joinkey '
           || '   FROM '
           || '   cipsrv_upload.' || str_dataset_prefix || '_sfid b '
           || '   WHERE '
           || '   b.source_featureid = a.source_featureid '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_lines a '
           || 'SET source_joinkey = ( '
           || '   SELECT '
           || '   c.source_joinkey '
           || '   FROM '
           || '   cipsrv_upload.' || str_dataset_prefix || '_sfid c '
           || '   WHERE '
           || '   c.source_featureid = a.source_featureid '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_areas a '
           || 'SET source_joinkey = ( '
           || '   SELECT '
           || '   d.source_joinkey '
           || '   FROM '
           || '   cipsrv_upload.' || str_dataset_prefix || '_sfid d '
           || '   WHERE '
           || '   d.source_featureid = a.source_featureid '
           || ') ';
           
   EXECUTE str_sql;
   
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_points';
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_lines';
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_areas';
           
   ----------------------------------------------------------------------------
   -- Step 90
   -- Create the cip results table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_cip';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_cip_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    objectid              INTEGER     NOT NULL '
           || '   ,source_originator     VARCHAR(130) '
           || '   ,source_featureid      VARCHAR(100) '
           || '   ,source_featureid2     VARCHAR(100) '
           || '   ,source_series         VARCHAR(100) '
           || '   ,source_subdivision    VARCHAR(100) '
           || '   ,source_joinkey        VARCHAR(40) '
           || '   ,start_date            DATE '
           || '   ,end_date              DATE '
           || '   ,cat_joinkey           VARCHAR(40) NOT NULL '
           || '   ,catchmentstatecode    VARCHAR(2)  NOT NULL '
           || '   ,nhdplusid             NUMERIC     NOT NULL '
           || '   ,istribal              VARCHAR(1)  NOT NULL '
           || '   ,istribal_areasqkm     NUMERIC '
           || '   ,catchmentresolution   VARCHAR(2)  NOT NULL '
           || '   ,catchmentareasqkm     NUMERIC     NOT NULL '
           || '   ,xwalk_huc12           VARCHAR(12) '
           || '   ,xwalk_method          VARCHAR(18) '
           || '   ,xwalk_huc12_version   VARCHAR(16) '
           || '   ,isnavigable           VARCHAR(1)  NOT NULL '
           || '   ,hasvaa                VARCHAR(1)  NOT NULL '
           || '   ,issink                VARCHAR(1)  NOT NULL '
           || '   ,isheadwater           VARCHAR(1)  NOT NULL '
           || '   ,iscoastal             VARCHAR(1)  NOT NULL '
           || '   ,isocean               VARCHAR(1)  NOT NULL '
           || '   ,isalaskan             VARCHAR(1) '
           || '   ,h3hexagonaddr         VARCHAR(64) '
           || '   ,globalid              VARCHAR(40) NOT NULL '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_cip TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_cip_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    source_joinkey ' 
           || '   ,cat_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_cip_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_cip_u02 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    globalid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_cip_u03 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    source_joinkey ' 
           || '   ,catchmentstatecode '
           || '   ,nhdplusid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE INDEX ' || str_dataset_prefix || '_cip_i01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    source_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE INDEX ' || str_dataset_prefix || '_cip_i02 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    catchmentstatecode '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_cip_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 100
   -- Create the src2cip results table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_src2cip';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_src2cip_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    objectid              INTEGER     NOT NULL '
           || '   ,source_joinkey        VARCHAR(40) NOT NULL '
           || '   ,permid_joinkey        VARCHAR(40) NOT NULL '
           || '   ,cat_joinkey           VARCHAR(40) NOT NULL '
           || '   ,catchmentstatecode    VARCHAR(2)  NOT NULL '
           || '   ,nhdplusid             NUMERIC     NOT NULL '
           || '   ,overlap_measure       NUMERIC '
           || '   ,cip_method            VARCHAR(255) '
           || '   ,cip_parms             VARCHAR(255) '
           || '   ,cip_date              DATE '
           || '   ,cip_version           VARCHAR(255) '
           || '   ,globalid              VARCHAR(40) NOT NULL '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_src2cip TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_src2cip_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    source_joinkey '
           || '   ,permid_joinkey '           
           || '   ,cat_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_src2cip_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_src2cip_u02 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    globalid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE INDEX ' || str_dataset_prefix || '_src2cip_i01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    source_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE INDEX ' || str_dataset_prefix || '_src2cip_i02 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    permid_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_src2cip_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   IF p_debug
   THEN
      RAISE WARNING 'setup complete';
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 110
   -- Read the control table
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || ' a.source_originator '
           || ',a.source_series '
           || ',a.dataset_prefix '
           || ',a.geometry_clip '
           || ',a.geometry_clip_stage '
           || ',a.catchment_filter '
           || ',a.nhdplus_version '
           || ',a.xwalk_huc12_version '
           
           || ',a.default_point_indexing_method '
           
           || ',a.default_line_indexing_method '
           || ',a.default_line_threshold '
           
           || ',a.default_ring_indexing_method '
           || ',a.default_ring_areacat_threshold '
           || ',a.default_ring_areaevt_threshold '
           
           || ',a.default_area_indexing_method '
           || ',a.default_areacat_threshold '
           || ',a.default_areaevt_threshold '
           
           || ',a.known_region '
           || ',a.username '
           || ',a.datecreated '
           || 'FROM '
           || 'cipsrv_upload.' || str_dataset_prefix || '_control a '
           || 'LIMIT 1';
           
   EXECUTE str_sql INTO
    str_control_source_originator
   ,str_control_source_series
   ,str_control_dataset_prefix
   ,str_geometry_clip
   ,str_geometry_clip_stage
   ,str_catchment_filter
   ,str_default_nhdplus_version
   ,str_xwalk_huc12_version
   
   ,str_default_point_indexing_method
   
   ,str_default_line_indexing_method
   ,num_default_line_threshold
   
   ,str_default_ring_indexing_method
   ,num_default_ring_areacat_threshold
   ,num_default_ring_areaevt_threshold
   
   ,str_default_area_indexing_method
   ,num_default_areacat_threshold
   ,num_default_areaevt_threshold
   
   ,str_default_known_region
   ,str_username
   ,dat_datecreated;
   
   IF p_override_default_nhdplus_version IS NOT NULL
   THEN
      str_default_nhdplus_version := p_override_default_nhdplus_version;
   
   END IF;
   
   IF str_dataset_prefix != LOWER(str_control_dataset_prefix)
   THEN
      RAISE WARNING 'mismatch between parameter and control dataset prefixes: % <> %',str_dataset_prefix,LOWER(str_control_dataset_prefix);
      
   END IF;
   
   ary_catchment_filter := string_to_array(str_catchment_filter,',');
   rec := cipsrv_engine.parse_catchment_filter(
      p_catchment_filter := ary_catchment_filter
   );
   boo_filter_by_state    := rec.out_filter_by_state;
   ary_state_filters      := rec.out_state_filters;
   boo_filter_by_tribal   := rec.out_filter_by_tribal;
   boo_filter_by_notribal := rec.out_filter_by_notribal;
   
   ary_geometry_clip := string_to_array(str_geometry_clip,',');
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 120
   -- Update the feature tables with measures
   ----------------------------------------------------------------------------
   str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_lines a '
           || 'SET lengthkm = cipsrv_engine.measure_lengthkm( '
           || '    a.shape '
           || '   ,a.nhdplus_version '
           || '   ,$1 '
           || '   ,a.known_region '
           || '   ,$2 '
           || ') ';
           
   EXECUTE str_sql 
   USING str_default_nhdplus_version,str_default_known_region;
   
   str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_areas a '
           || 'SET areasqkm = cipsrv_engine.measure_areasqkm( '
           || '    a.shape '
           || '   ,a.nhdplus_version '
           || '   ,$1 '
           || '   ,a.known_region '
           || '   ,$2 '
           || ') ';
           
   EXECUTE str_sql 
   USING str_default_nhdplus_version,str_default_known_region;
   
   COMMIT;
   
   IF p_debug
   THEN
      RAISE WARNING 'measuring complete';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 130
   -- Clip features if requested
   ----------------------------------------------------------------------------
   IF str_geometry_clip_stage = 'BEFORE'
   AND str_geometry_clip IS NOT NULL
   AND array_length(ary_geometry_clip,1) > 0
   THEN
      rec := cipsrv_engine.feature_batch_clip(
          p_dataset_prefix        := str_dataset_prefix
         ,p_clippers              := ary_geometry_clip
         ,p_known_region          := str_default_known_region
      );
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      COMMIT;

   END IF;
   
   IF p_debug
   THEN
      RAISE WARNING 'BEFORE clip complete';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 140
   -- Step through each sfid
   ----------------------------------------------------------------------------
   FOR rec IN EXECUTE 'SELECT a.* FROM cipsrv_upload.' || str_dataset_prefix || '_sfid a '
   LOOP
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      str_nhdplus_version               := NULL;
            
      num_point_indexing_return_code    := NULL;
      str_point_indexing_status_message := NULL;
      
      str_line_indexing_method          := NULL;
      num_line_indexing_return_code     := NULL;
      str_line_indexing_status_message  := NULL;
      num_line_threshold                := NULL;
            
      str_ring_indexing_method          := NULL;
      num_ring_indexing_return_code     := NULL;
      str_ring_indexing_status_message  := NULL;
      num_ring_areacat_threshold        := NULL;
      num_ring_areaevt_threshold        := NULL;
      
      str_area_indexing_method          := NULL;
      num_areacat_threshold             := NULL;
      num_areaevt_threshold             := NULL;
      num_area_indexing_return_code     := NULL;
      str_area_indexing_status_message  := NULL;
      
      str_known_region                  := NULL;

      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      IF rec.src_point_count > 0
      THEN
         FOR rec2 IN EXECUTE 'SELECT b.* FROM cipsrv_upload.' || str_dataset_prefix || '_points b WHERE b.source_joinkey = $1' USING rec.source_joinkey
         LOOP
            --###############################################################--
            str_nhdplus_version := NULL;
            IF rec2.nhdplus_version IS NOT NULL
            THEN
               str_nhdplus_version := rec2.nhdplus_version;
               
            ELSE
               IF str_default_nhdplus_version IS NOT NULL
               THEN
                  str_nhdplus_version := str_default_nhdplus_version;
                  
               END IF;
               
            END IF;

            --###############################################################--
            str_known_region := NULL;
            IF rec2.known_region IS NOT NULL
            THEN
               str_known_region := rec2.known_region;
               
            ELSE
               IF str_default_known_region IS NOT NULL
               THEN
                  str_known_region := str_default_known_region;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            str_point_indexing_method := NULL;
            IF rec2.point_indexing_method IS NOT NULL
            THEN
               str_point_indexing_method := rec2.point_indexing_method;
               
            ELSE
               IF str_default_point_indexing_method IS NOT NULL
               THEN
                  str_point_indexing_method := str_default_point_indexing_method;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            IF str_point_indexing_method IS NULL
            OR str_point_indexing_method = 'point_simple'
            THEN
               IF str_nhdplus_version = 'nhdplus_m'
               THEN
                  rec3 := cipsrv_nhdplus_m.index_point_simple(
                      p_geometry             := rec2.shape
                     ,p_known_region         := str_known_region
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_point_indexing_return_code    := rec3.out_return_code;
                  str_point_indexing_status_message := rec3.out_status_message;
                  
               ELSIF str_nhdplus_version = 'nhdplus_h'
               THEN
                  rec3 := cipsrv_nhdplus_h.index_point_simple(
                      p_geometry             := rec2.shape
                     ,p_known_region         := str_known_region
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_point_indexing_return_code    := rec3.out_return_code;
                  str_point_indexing_status_message := rec3.out_status_message;
               
               ELSE
                  RAISE EXCEPTION 'err %',str_nhdplus_version;
               
               END IF;               
            
            ELSE
               RAISE EXCEPTION 'err %',str_point_indexing_method;
               
            END IF;

            INSERT INTO tmp_src2cip(
                permid_joinkey
               ,nhdplusid
               ,cip_method
               ,cip_parms
               ,overlap_measure               
            ) 
            SELECT
             a.permid_joinkey
            ,a.nhdplusid
            ,str_point_indexing_method
            ,NULL
            ,a.overlap_measure
            FROM
            tmp_cip a
            WHERE
            a.permid_joinkey = rec2.permid_joinkey::UUID;            
            
         END LOOP;
         
      END IF;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      IF rec.src_line_count > 0
      THEN
         FOR rec2 IN EXECUTE 'SELECT c.* FROM cipsrv_upload.' || str_dataset_prefix || '_lines c WHERE c.source_joinkey = $1' USING rec.source_joinkey
         LOOP
            --###############################################################--
            str_nhdplus_version := NULL;
            IF rec2.nhdplus_version IS NOT NULL
            THEN
               str_nhdplus_version := rec2.nhdplus_version;
               
            ELSE
               IF str_default_nhdplus_version IS NOT NULL
               THEN
                  str_nhdplus_version := str_default_nhdplus_version;
                  
               END IF;
               
            END IF;
         
            --###############################################################--
            str_known_region := NULL;
            IF rec2.known_region IS NOT NULL
            THEN
               str_known_region := rec2.known_region;
               
            ELSE
               IF str_default_known_region IS NOT NULL
               THEN
                  str_known_region := str_default_known_region;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            str_ring_indexing_method := NULL;
            IF rec2.ring_indexing_method IS NOT NULL
            THEN
               str_ring_indexing_method := rec2.ring_indexing_method;
               
            ELSE
               IF str_default_ring_indexing_method IS NOT NULL
               THEN
                  str_ring_indexing_method := str_default_ring_indexing_method;
               
               END IF;
               
            END IF;
            
            --###*********************************************************###--
            FOR i IN 1 .. ST_NumGeometries(rec2.shape)
            LOOP
               geom_part := ST_GeometryN(rec2.shape,i);
               
               IF str_ring_indexing_method = 'treat_as_lines'
               THEN
                  boo_isring := FALSE;
                  
               ELSE
                  boo_isring := ST_IsRing(geom_part);
               
               END IF;
               
               --- Branch for ring handling -----------------------------------
               IF boo_isring
               THEN
                  str_line_indexing_method := NULL;
                  
                  --###############################################################--
                  num_ring_areacat_threshold := NULL;
                  IF rec2.ring_areacat_threshold IS NOT NULL
                  THEN
                     num_ring_areacat_threshold := rec2.ring_areacat_threshold;
                     
                  ELSE
                     IF num_default_ring_areacat_threshold IS NOT NULL
                     THEN
                        num_ring_areacat_threshold := num_default_ring_areacat_threshold;
                     
                     END IF;
                     
                  END IF;
                  
                  --###############################################################--
                  num_ring_areaevt_threshold := NULL;
                  IF rec2.ring_areaevt_threshold IS NOT NULL
                  THEN
                     num_ring_areaevt_threshold := rec2.ring_areaevt_threshold;
                     
                  ELSE
                     IF num_default_ring_areaevt_threshold IS NOT NULL
                     THEN
                        num_ring_areaevt_threshold := num_default_ring_areaevt_threshold;
                     
                     END IF;
                     
                  END IF;
                  
                  --###############################################################--
                  IF str_ring_indexing_method IS NULL
                  OR str_ring_indexing_method = 'area_simple'
                  THEN
                     IF str_nhdplus_version = 'nhdplus_m'
                     THEN
                        rec3 := cipsrv_nhdplus_m.index_area_simple(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                        
                     ELSIF str_nhdplus_version IN ('nhdplus_h','HR')
                     THEN
                        rec3 := cipsrv_nhdplus_h.index_area_simple(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                     
                     ELSE
                        RAISE EXCEPTION 'err %',str_nhdplus_version;
                     
                     END IF;
                  
                  ELSIF str_ring_indexing_method = 'area_centroid'
                  THEN
                     IF str_nhdplus_version = 'nhdplus_m'
                     THEN
                        rec3 := cipsrv_nhdplus_m.index_area_centroid(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                        
                     ELSIF str_nhdplus_version = 'nhdplus_h'
                     THEN
                        rec3 := cipsrv_nhdplus_h.index_area_centroid(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                     
                     ELSE
                        RAISE EXCEPTION 'err %',str_nhdplus_version;
                     
                     END IF;
                     
                  ELSIF str_ring_indexing_method = 'area_artpath'
                  THEN
                     IF str_nhdplus_version = 'nhdplus_m'
                     THEN
                        rec3 := cipsrv_nhdplus_m.index_area_artpath(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                        
                     ELSIF str_nhdplus_version = 'nhdplus_h'
                     THEN
                        rec3 := cipsrv_nhdplus_h.index_area_artpath(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                     
                     ELSE
                        RAISE EXCEPTION 'err %',str_nhdplus_version;
                     
                     END IF;
                     
                  ELSE
                     RAISE EXCEPTION 'err %',str_ring_indexing_method;
                     
                  END IF;
               
               -- Normal line handling here --------------------------------------
               ELSE
                  str_ring_indexing_method := NULL;
                  
                  --###############################################################--
                  str_line_indexing_method := NULL;
                  IF rec2.line_indexing_method IS NOT NULL
                  THEN
                     str_line_indexing_method := rec2.line_indexing_method;
                     
                  ELSE
                     IF str_default_line_indexing_method IS NOT NULL
                     THEN
                        str_line_indexing_method := str_default_line_indexing_method;
                     
                     END IF;
                     
                  END IF;
               
                  --###############################################################--
                  num_line_threshold := NULL;
                  IF rec2.line_threshold IS NOT NULL
                  THEN
                     num_line_threshold := rec2.line_threshold;
                     
                  ELSE
                     IF num_default_line_threshold IS NOT NULL
                     THEN
                        num_line_threshold := num_default_line_threshold;
                     
                     END IF;
                     
                  END IF;

                  --###############################################################--
                  IF str_line_indexing_method IS NULL
                  OR str_line_indexing_method = 'line_simple'
                  THEN
                     IF str_nhdplus_version = 'nhdplus_m'
                     THEN
                        rec3 := cipsrv_nhdplus_m.index_line_simple(
                            p_geometry             := geom_part
                           ,p_geometry_lengthkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_line_threshold_perc  := num_line_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := rec2.shape
                        );
                        num_line_indexing_return_code    := rec3.out_return_code;
                        str_line_indexing_status_message := rec3.out_status_message;
                        
                     ELSIF str_nhdplus_version = 'nhdplus_h'
                     THEN
                        rec3 := cipsrv_nhdplus_h.index_line_simple(
                            p_geometry             := geom_part
                           ,p_geometry_lengthkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_line_threshold_perc  := num_line_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := rec2.shape
                        );
                        num_line_indexing_return_code    := rec3.out_return_code;
                        str_line_indexing_status_message := rec3.out_status_message;
                     
                     ELSE
                        RAISE EXCEPTION 'err %',str_nhdplus_version;
                     
                     END IF;
                  
                  ELSIF str_line_indexing_method = 'line_levelpath'
                  THEN
                     IF str_nhdplus_version = 'nhdplus_m'
                     THEN
                        rec3 := cipsrv_nhdplus_m.index_line_levelpath(
                            p_geometry             := geom_part
                           ,p_geometry_lengthkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_line_threshold_perc  := num_line_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := rec2.shape
                        );
                        num_line_indexing_return_code    := rec3.out_return_code;
                        str_line_indexing_status_message := rec3.out_status_message;
                        
                     ELSIF str_nhdplus_version = 'nhdplus_h'
                     THEN
                        rec3 := cipsrv_nhdplus_h.index_line_levelpath(
                            p_geometry             := geom_part
                           ,p_geometry_lengthkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_line_threshold_perc  := num_line_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := rec2.shape
                        );
                        num_line_indexing_return_code    := rec3.out_return_code;
                        str_line_indexing_status_message := rec3.out_status_message;
                     
                     ELSE
                        RAISE EXCEPTION 'err %',str_nhdplus_version;
                     
                     END IF;
                     
                  ELSE
                     RAISE EXCEPTION 'err %',str_line_indexing_method;
                     
                  END IF;

               END IF;

            END LOOP;
            
            INSERT INTO tmp_src2cip(
                permid_joinkey
               ,nhdplusid
               ,cip_method
               ,cip_parms
               ,overlap_measure               
            ) 
            SELECT
             a.permid_joinkey
            ,a.nhdplusid
            ,str_line_indexing_method
            ,num_line_threshold::VARCHAR
            ,a.overlap_measure
            FROM
            tmp_cip a
            WHERE
            a.permid_joinkey = rec2.permid_joinkey::UUID; 

         END LOOP;
      
      END IF;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      IF rec.src_area_count > 0
      THEN
         FOR rec2 IN EXECUTE 'SELECT d.* FROM cipsrv_upload.' || str_dataset_prefix || '_areas d WHERE d.source_joinkey = $1' USING rec.source_joinkey
         LOOP
            --###############################################################--
            str_nhdplus_version := NULL;
            IF rec2.nhdplus_version IS NOT NULL
            THEN
               str_nhdplus_version := rec2.nhdplus_version;
               
            ELSE
               IF str_default_nhdplus_version IS NOT NULL
               THEN
                  str_nhdplus_version := str_default_nhdplus_version;
                  
               END IF;
               
            END IF;
            
            --###############################################################--
            str_known_region := NULL;
            IF rec2.known_region IS NOT NULL
            THEN
               str_known_region := rec2.known_region;
               
            ELSE
               IF str_default_known_region IS NOT NULL
               THEN
                  str_known_region := str_default_known_region;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            str_area_indexing_method := NULL;
            IF rec2.area_indexing_method IS NOT NULL
            THEN
               str_area_indexing_method := rec2.area_indexing_method;
               
            ELSE
               IF str_default_area_indexing_method IS NOT NULL
               THEN
                  str_area_indexing_method := str_default_area_indexing_method;
               
               END IF;
               
            END IF;
  
            --###############################################################--
            num_areacat_threshold := NULL;
            IF rec2.areacat_threshold IS NOT NULL
            THEN
               num_areacat_threshold := rec2.areacat_threshold;
               
            ELSE
               IF num_default_areacat_threshold IS NOT NULL
               THEN
                  num_areacat_threshold := num_default_areacat_threshold;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            num_areaevt_threshold := NULL;
            IF rec2.areaevt_threshold IS NOT NULL
            THEN
               num_areaevt_threshold := rec2.areaevt_threshold;
               
            ELSE
               IF num_default_areaevt_threshold IS NOT NULL
               THEN
                  num_areaevt_threshold := num_default_areaevt_threshold;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            IF str_area_indexing_method IS NULL
            OR str_area_indexing_method = 'area_simple'
            THEN
               IF str_nhdplus_version = 'nhdplus_m'
               THEN
                  rec3 := cipsrv_nhdplus_m.index_area_simple(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;
                  
               ELSIF str_nhdplus_version = 'nhdplus_h'
               THEN
                  rec3 := cipsrv_nhdplus_h.index_area_simple(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;
               
               ELSE
                  RAISE EXCEPTION 'err %',str_nhdplus_version;
               
               END IF;
            
            ELSIF str_area_indexing_method = 'area_centroid'
            THEN
               IF str_nhdplus_version = 'nhdplus_m'
               THEN
                  rec3 := cipsrv_nhdplus_m.index_area_centroid(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;
                  
               ELSIF str_nhdplus_version = 'nhdplus_h'
               THEN
                  rec3 := cipsrv_nhdplus_h.index_area_centroid(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;
               
               ELSE
                  RAISE EXCEPTION 'err %',str_nhdplus_version;
               
               END IF;
               
            ELSIF str_area_indexing_method = 'area_artpath'
            THEN
               IF str_nhdplus_version = 'nhdplus_m'
               THEN
                  rec3 := cipsrv_nhdplus_m.index_area_artpath(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;
                  
               ELSIF str_nhdplus_version = 'nhdplus_h'
               THEN
                  rec3 := cipsrv_nhdplus_h.index_area_artpath(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;

               ELSE
                  RAISE EXCEPTION 'err %',str_nhdplus_version;
               
               END IF;
               
            ELSE
               RAISE EXCEPTION 'err %',str_area_indexing_method;
               
            END IF;
            
            INSERT INTO tmp_src2cip(
                permid_joinkey
               ,nhdplusid
               ,cip_method
               ,cip_parms
               ,overlap_measure               
            ) 
            SELECT
             a.permid_joinkey
            ,a.nhdplusid
            ,str_area_indexing_method
            ,num_areacat_threshold::VARCHAR || ',' || num_areaevt_threshold::VARCHAR
            ,a.overlap_measure
            FROM
            tmp_cip a
            WHERE
            a.permid_joinkey = rec2.permid_joinkey::UUID; 
            
         END LOOP;
      
      END IF;
      
      IF p_debug
      THEN
         RAISE WARNING 'indexing complete';
      
      END IF;
      
      IF str_nhdplus_version = 'nhdplus_m'
      THEN
         str_catchment_resolution := 'MR';
         
      ELSIF str_nhdplus_version = 'nhdplus_h'
      THEN
         str_catchment_resolution := 'HR';
         
      ELSE
         str_catchment_resolution := str_nhdplus_version;
      
      END IF;
      
      EXECUTE 'ANALYZE tmp_cip';
      
      --************************************************************--
      str_sql := 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_cip( '
              || '    objectid '
              || '   ,source_originator '
              || '   ,source_featureid '
              || '   ,source_featureid2 '
              || '   ,source_series '
              || '   ,source_subdivision '
              || '   ,source_joinkey '
              || '   ,start_date '
              || '   ,end_date '
              || '   ,cat_joinkey '
              || '   ,catchmentstatecode '
              || '   ,nhdplusid '
              || '   ,istribal '
              || '   ,istribal_areasqkm '
              || '   ,catchmentresolution '
              || '   ,catchmentareasqkm '
              || '   ,xwalk_huc12 '
              || '   ,xwalk_method '
              || '   ,xwalk_huc12_version '
              || '   ,isnavigable '
              || '   ,hasvaa '
              || '   ,issink '
              || '   ,isheadwater '
              || '   ,iscoastal '
              || '   ,isocean '
              || '   ,isalaskan '
              || '   ,h3hexagonaddr '
              || '   ,globalid '
              || ') '
              || 'SELECT '
              || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_cip_seq'') AS objectid '
              || ',$1 '
              || ',$2 '
              || ',$3 '
              || ',$4 '
              || ',$5 '
              || ',$6 '
              || ',$7 '
              || ',$8 '
              || ',a.catchmentstatecode || a.nhdplusid::BIGINT::VARCHAR '
              || ',a.catchmentstatecode '
              || ',a.nhdplusid '
              || ',a.istribal '
              || ',a.istribal_areasqkm '
              || ',$9 '
              || ',a.areasqkm AS catchmentareasqkm '
              || ',a.xwalk_huc12 '
              || ',NULL '
              || ',NULL '
              || ',a.isnavigable '
              || ',a.hasvaa '
              || ',a.issink '
              || ',a.isheadwater '
              || ',a.iscoastal '
              || ',a.isocean '
              || ',CASE '
              || ' WHEN a.h3hexagonaddr IS NOT NULL '
              || ' THEN '
              || '    ''Y'' '
              || ' ELSE '
              || '    ''N'' '
              || ' END AS isalaskan '
              || ',a.h3hexagonaddr '
              || ',''{'' || uuid_generate_v1() || ''}'' '
              || 'FROM '
              || 'cipsrv_' || str_nhdplus_version || '.catchment_fabric a '
              || 'WHERE '
              || 'EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid) '
              || 'AND (NOT $10 OR a.catchmentstatecode = ANY($11) ) '
              || 'AND (NOT $12 OR a.istribal IN (''F'',''P''))'
              || 'AND (NOT $13 OR a.istribal = ''N'')';
              
      EXECUTE str_sql 
      USING 
       rec.source_originator
      ,rec.source_featureid
      ,rec.source_featureid2
      ,rec.source_series
      ,rec.source_subdivision
      ,rec.source_joinkey
      ,rec.start_date
      ,rec.end_date
      ,str_catchment_resolution
      ,boo_filter_by_state
      ,ary_state_filters
      ,boo_filter_by_tribal
      ,boo_filter_by_notribal;

      GET DIAGNOSTICS int_count = ROW_COUNT;

      IF str_nhdplus_version = 'nhdplus_m'
      THEN
         int_cat_mr_count := int_count;
         int_cat_hr_count := 0;
         
      ELSIF str_nhdplus_version = 'nhdplus_h'
      THEN
         int_cat_mr_count := 0;
         int_cat_hr_count := int_count;
         
      END IF;
      
      COMMIT;
      
      EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_cip';
      
      --************************************************************--
      str_sql := 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
              || '    objectid '
              || '   ,source_joinkey '
              || '   ,permid_joinkey '
              || '   ,cat_joinkey '
              || '   ,catchmentstatecode '
              || '   ,nhdplusid '
              || '   ,overlap_measure '
              || '   ,cip_method '
              || '   ,cip_parms '
              || '   ,cip_date '
              || '   ,cip_version '
              || '   ,globalid '
              || ') '
              || 'SELECT '
              || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_src2cip_seq'') AS objectid '
              || ',b.source_joinkey '
              || ',''{'' || a.permid_joinkey::VARCHAR || ''}'' '
              || ',b.cat_joinkey '
              || ',b.catchmentstatecode '
              || ',b.nhdplusid '
              || ',a.overlap_measure '
              || ',a.cip_method '
              || ',a.cip_parms '
              || ',$1 '
              || ',$2 '
              || ',''{'' || uuid_generate_v1() || ''}'' '
              || 'FROM '
              || 'tmp_src2cip a '
              || 'JOIN '
              || 'cipsrv_upload.' || str_dataset_prefix || '_cip b '
              || 'ON '
              || 'a.nhdplusid = b.nhdplusid '
              || 'WHERE '
              || 'b.source_joinkey = $3 '
              || 'ON CONFLICT DO NOTHING ';
              
      EXECUTE str_sql 
      USING
       CURRENT_TIMESTAMP
      ,cipsrv_engine.cipsrv_version()
      ,rec.source_joinkey;

      GET DIAGNOSTICS int_count = ROW_COUNT;
      
      COMMIT;
      
      IF p_debug
      THEN
         RAISE WARNING 'src2cip complete';
         
      END IF;
      
      EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_src2cip';
      
      --************************************************************--
      EXECUTE 'TRUNCATE TABLE tmp_cip';
      EXECUTE 'TRUNCATE TABLE tmp_src2cip';
      
      --************************************************************--
      str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_sfid a '
              || 'SET '
              || ' point_indexing_method_used    = $1 '
              || ',point_indexing_return_code    = $2 '
              || ',point_indexing_status_message = $3 '
              || ',line_indexing_method_used     = $4 '
              || ',line_indexing_return_code     = $5 '
              || ',line_indexing_status_message  = $6 '
              || ',ring_indexing_method_used     = $7 '
              || ',ring_indexing_return_code     = $8 '
              || ',ring_indexing_status_message  = $9 '
              || ',area_indexing_method_used     = $10 '
              || ',area_indexing_return_code     = $11 '
              || ',area_indexing_status_message  = $12 '
              || ',cat_mr_count                  = $13 '
              || ',cat_hr_count                  = $14 '
              || 'WHERE '
              || 'a.source_joinkey = $15 ';
      
      EXECUTE str_sql
      USING
       str_point_indexing_method
      ,num_point_indexing_return_code
      ,str_point_indexing_status_message
      ,str_line_indexing_method
      ,num_line_indexing_return_code
      ,str_line_indexing_status_message
      ,str_ring_indexing_method
      ,num_ring_indexing_return_code
      ,str_ring_indexing_status_message
      ,str_area_indexing_method
      ,num_area_indexing_return_code
      ,str_area_indexing_status_message
      ,int_cat_mr_count
      ,int_cat_hr_count
      ,rec.source_joinkey;
      
      COMMIT;

   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 150
   -- Clip features AFTER if requested
   ----------------------------------------------------------------------------
   IF str_geometry_clip_stage = 'AFTER'
   AND str_geometry_clip IS NOT NULL
   AND array_length(ary_geometry_clip,1) > 0
   THEN
      rec := cipsrv_engine.feature_batch_clip(
          p_dataset_prefix        := str_dataset_prefix
         ,p_clippers              := ary_geometry_clip
         ,p_known_region          := str_default_known_region
      );
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      COMMIT;

   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER PROCEDURE cipsrv_engine.cipsrv_batch_index(
    VARCHAR
   ,VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON PROCEDURE cipsrv_engine.cipsrv_batch_index(
    VARCHAR
   ,VARCHAR
   ,BOOLEAN
) TO PUBLIC;

--******************************--
----- procedures/point_batch_index_table.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.point_batch_index_table';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP PROCEDURE IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE PROCEDURE cipsrv_engine.point_batch_index_table(
    IN  p_cipsrv_upload_table              VARCHAR
   ,IN  p_nhdplus_version                  VARCHAR
   ,IN  p_point_indexing_method            VARCHAR
   ,OUT out_return_code                    INTEGER
   ,OUT out_status_message                 VARCHAR
   ,IN  p_fcode_allow                      INTEGER[] DEFAULT NULL
   ,IN  p_fcode_deny                       INTEGER[] DEFAULT NULL
   ,IN  p_distance_max_dist_km             NUMERIC   DEFAULT 15
   ,IN  p_return_link_path                 BOOLEAN   DEFAULT NULL
   ,IN  p_limit_innetwork                  BOOLEAN   DEFAULT FALSE
   ,IN  p_limit_navigable                  BOOLEAN   DEFAULT FALSE
   ,IN  p_known_region                     VARCHAR   DEFAULT NULL
   ,IN  p_commit_limit                     INTEGER   DEFAULT 2000
)
AS $BODY$ 
DECLARE
   rec                                RECORD;
   rec2                               RECORD;
   str_nhdplus_version                VARCHAR;
   str_point_indexing_method          VARCHAR;
   str_cipsrv_upload_table            VARCHAR;
   num_nhdplusid                      NUMERIC;
   str_reachcode                      VARCHAR;
   num_snap_measure                   NUMERIC;
   num_snap_distancekm                NUMERIC;
   int_return_code                    INTEGER;
   str_status_message                 VARCHAR;
   int_counter                        INTEGER;
   geo_snap_point                     GEOMETRY;
   
BEGIN

   out_return_code := cipsrv_engine.create_cip_batch_temp_tables();
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_cipsrv_upload_table IS NULL
   THEN
      RAISE EXCEPTION 'p_cipsrv_upload_table required';
   
   ELSE
      str_cipsrv_upload_table := LOWER(REPLACE(p_cipsrv_upload_table,';',''));
      
      IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_cipsrv_upload_table)
      THEN
         RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_cipsrv_upload_table;
      
      END IF;
      
   END IF;
   
   IF p_nhdplus_version IS NULL
   THEN
      RAISE EXCEPTION 'p_nhdplus_version required';
   
   ELSIF UPPER(p_nhdplus_version) IN ('MEDIUM','MR','3')
   THEN
      str_nhdplus_version := 'MR';
      
   ELSIF UPPER(p_nhdplus_version) IN ('HIGH','HR','2')
   THEN
      str_nhdplus_version := 'HR';
      
   END IF;
   
   IF p_point_indexing_method IS NULL
   THEN
      RAISE EXCEPTION 'p_point_indexing_method required';
   
   ELSIF UPPER(p_point_indexing_method) IN ('CATCHMENT CONSTRAINED','CATCONSTRAINED')
   THEN
      str_point_indexing_method := 'CATCONSTRAINED';
      
   ELSIF UPPER(p_point_indexing_method) IN ('DISTANCE')
   THEN
      str_point_indexing_method := 'DISTANCE';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Add or clear output fields
   ----------------------------------------------------------------------------
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_guid')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_guid VARCHAR(40)';
   
   END IF;
   
   EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_guid = ''{'' || uuid_generate_v1() || ''}''';
   COMMIT;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_cipsrv_upload_table,'idx_guid',TRUE)
   THEN
      EXECUTE 'CREATE UNIQUE INDEX ' || str_cipsrv_upload_table || '_u99 ON cipsrv_upload.' || str_cipsrv_upload_table || '(idx_guid)';
   
   END IF;
      
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_nhdplusid')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_nhdplusid NUMERIC';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_nhdplusid = NULL';
      COMMIT;
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_reachcode')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_reachcode VARCHAR(14)';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_reachcode = NULL';
      COMMIT;
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_snap_measure')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_snap_measure NUMERIC';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_snap_measure = NULL';
      COMMIT;
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_snap_distancekm')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_snap_distancekm NUMERIC';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_snap_distancekm = NULL';
      COMMIT;
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_return_code')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_return_code INTEGER';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_return_code = NULL';
      COMMIT;
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_status_message')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_status_message VARCHAR';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_status_message = NULL';
      COMMIT;
      
   END IF;
   
   COMMIT;
   EXECUTE 'ANALYZE cipsrv_upload.' || str_cipsrv_upload_table;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Index Each record
   ----------------------------------------------------------------------------
   int_counter := 1;
   FOR rec IN EXECUTE 'SELECT a.* FROM cipsrv_upload.' || str_cipsrv_upload_table || ' a '
   LOOP
      IF rec.shape IS NOT NULL
      AND NOT ST_ISEMPTY(rec.shape)
      THEN
         num_nhdplusid       := NULL;
         str_reachcode       := NULL;
         num_snap_measure    := NULL;
         num_snap_distancekm := NULL;
         int_return_code     := NULL;
         str_status_message  := NULL;
         geo_snap_point      := NULL;
         
         IF str_nhdplus_version = 'MR'
         THEN
            IF str_point_indexing_method = 'CATCONSTRAINED'
            THEN
               rec2 := cipsrv_nhdplus_m.catconstrained_index(
                   p_point                     := rec.shape
                  ,p_return_link_path          := FALSE
                  ,p_known_catchment_nhdplusid := NULL
                  ,p_known_region              := p_known_region
               );
               num_nhdplusid       := rec2.out_nhdplusid;
               str_reachcode       := rec2.out_reachcode;
               num_snap_measure    := rec2.out_snap_measure;
               geo_snap_point      := rec2.out_snap_point;
               num_snap_distancekm := rec2.out_snap_distancekm;
               int_return_code     := rec2.out_return_code;
               str_status_message  := rec2.out_status_message;
            
            ELSIF str_point_indexing_method = 'DISTANCE'
            THEN
               rec2 := cipsrv_nhdplus_m.distance_index(
                   p_point                  := rec.shape
                  ,p_fcode_allow            := p_fcode_allow
                  ,p_fcode_deny             := p_fcode_deny
                  ,p_distance_max_dist_km   := p_distance_max_dist_km
                  ,p_limit_innetwork        := p_limit_innetwork
                  ,p_limit_navigable        := p_limit_navigable
                  ,p_return_link_path       := FALSE
                  ,p_known_region           := p_known_region
               );
               num_nhdplusid       := rec2.out_nhdplusid;
               str_reachcode       := rec2.out_reachcode;
               num_snap_measure    := rec2.out_snap_measure;
               geo_snap_point      := rec2.out_snap_point;
               num_snap_distancekm := rec2.out_snap_distancekm;
               int_return_code     := rec2.out_return_code;
               str_status_message  := rec2.out_status_message;
            
            ELSE
               RAISE EXCEPTION 'err2';

            END IF;
            
         ELSIF str_nhdplus_version = 'HR'
         THEN
            IF str_point_indexing_method = 'CATCONSTRAINED'
            THEN
               rec2 := cipsrv_nhdplus_h.catconstrained_reach_index(
                   p_geometry               := rec.shape
                  ,p_catchment_nhdplusid    := NULL
                  ,p_known_region           := p_known_region
               );
               num_nhdplusid       := rec2.out_nhdplusid;
               str_reachcode       := rec2.out_reachcode;
               num_snap_measure    := rec2.out_snap_measure;
               geo_snap_point      := rec2.out_snap_point;
               num_snap_distancekm := rec2.out_snap_distancekm;
               int_return_code     := rec2.out_return_code;
               str_status_message  := rec2.out_status_message;
            
            ELSIF str_point_indexing_method = 'DISTANCE'
            THEN
               rec2 := cipsrv_nhdplus_h.distance_reach_index(
                   p_geometry               := rec.shape
                  ,p_fcode_allow            := p_fcode_allow
                  ,p_fcode_deny             := p_fcode_deny
                  ,p_distance_max_dist_km   := p_distance_max_dist_km
                  ,p_limit_innetwork        := p_limit_innetwork
                  ,p_limit_navigable        := p_limit_navigable
                  ,p_known_region           := p_known_region
               );
               num_nhdplusid       := rec2.out_nhdplusid;
               str_reachcode       := rec2.out_reachcode;
               num_snap_measure    := rec2.out_snap_measure;
               geo_snap_point      := rec2.out_snap_point;
               num_snap_distancekm := rec2.out_snap_distancekm;
               int_return_code     := rec2.out_return_code;
               str_status_message  := rec2.out_status_message;
           
            ELSE
               RAISE EXCEPTION 'err2';

            END IF;
         
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;
         
      END IF;
      
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' a '
           || 'SET '
           || ' idx_nhdplusid       = $1 '
           || ',idx_reachcode       = $2 '
           || ',idx_snap_measure    = $3 '
           || ',idx_snap_distancekm = $4 '
           || ',shape               = $5 '
           || ',idx_return_code     = $6 '
           || ',idx_status_message  = $7 '
           || 'WHERE '
           || 'a.idx_guid = $8 '
      USING
       num_nhdplusid
      ,str_reachcode
      ,num_snap_measure
      ,num_snap_distancekm
      ,geo_snap_point
      ,int_return_code
      ,str_status_message     
      ,rec.idx_guid;
      
      int_counter := int_counter + 1;
      IF int_counter > p_commit_limit
      THEN
         COMMIT;
         int_counter := 1;
         
      END IF;

   END LOOP;
   
   COMMIT;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER PROCEDURE cipsrv_engine.point_batch_index_table(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON PROCEDURE cipsrv_engine.point_batch_index_table(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,INTEGER
) TO PUBLIC;

--******************************--
----- procedures/updn_batch_search.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.updn_batch_search';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP PROCEDURE IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE PROCEDURE cipsrv_engine.updn_batch_search(
    IN  p_cip_updn_prefix                  VARCHAR
   ,IN  p_nhdplus_version                  VARCHAR
   ,IN  p_points_to_process_id             VARCHAR
   ,IN  p_points_to_process_nhdplusid      VARCHAR
   ,IN  p_points_to_process_measure        VARCHAR
   ,IN  p_points_to_search_id              VARCHAR
   ,IN  p_points_to_search_nhdplusid       VARCHAR
   ,IN  p_points_to_search_measure         VARCHAR
   ,IN  p_updn_search_type                 VARCHAR
   ,IN  p_updn_maximum_dist_km             NUMERIC
   ,IN  p_updn_gather_streams              BOOLEAN
   ,IN  p_updn_streams_between             VARCHAR
   ,IN  p_ignore_matching_ids              BOOLEAN
   ,OUT out_return_code                    INTEGER
   ,OUT out_status_message                 VARCHAR
   ,IN  p_known_region                     VARCHAR   DEFAULT NULL
   ,IN  p_commit_limit                     INTEGER   DEFAULT 2000
)
AS $BODY$ 
DECLARE
   rec                                RECORD;
   rec2                               RECORD;
   str_sql                            VARCHAR;
   str_nhdplus_version                VARCHAR;
   str_dataset_prefix                 VARCHAR;
   str_points_to_process_id           VARCHAR;
   str_points_to_search_id            VARCHAR;
   str_search_type                    VARCHAR;
   boo_updn_gather_streams            BOOLEAN;
   num_max_distancekm                 NUMERIC;
   str_updn_streams_between           VARCHAR;
   boo_ignore_matching_ids            BOOLEAN;
   boo_return_flowline_details        BOOLEAN;
   boo_return_flowline_geometry       BOOLEAN;
   int_grid_srid                      INTEGER;
   int_flowline_count                 INTEGER;
   int_return_code                    INTEGER;
   str_status_message                 VARCHAR;
   int_commit_limit                   INTEGER;
   int_counter                        INTEGER;
   
   c_analyze_threshold                INTEGER := 500;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_cip_updn_prefix IS NULL
   THEN
      RAISE EXCEPTION 'err';
   
   END IF;
   
   str_dataset_prefix       := LOWER(p_cip_updn_prefix);
   str_points_to_process_id := LOWER(p_points_to_process_id);
   str_points_to_search_id  := LOWER(p_points_to_search_id);
   str_search_type          := UPPER(p_updn_search_type);
   num_max_distancekm       := p_updn_maximum_dist_km;
   int_commit_limit         := p_commit_limit;
   
   IF int_commit_limit IS NULL
   OR int_commit_limit < 1
   THEN
      int_commit_limit := 2000;
      
   END IF;
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_pointstoprocess')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_pointstoprocess';
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_pointstoprocess',str_points_to_process_id)
   THEN
      RAISE EXCEPTION 'missing %.%.%','cipsrv_upload',str_dataset_prefix || '_pointstoprocess',str_points_to_process_id;
      
   END IF;
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_pointstosearch')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_pointstosearch';
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_pointstosearch',str_points_to_search_id)
   THEN
      RAISE EXCEPTION 'missing %.%.%','cipsrv_upload',str_dataset_prefix || '_pointstosearch',str_points_to_search_id;
      
   END IF;
   
   IF UPPER(p_nhdplus_version) IN ('MR','NHDPLUS_M')
   THEN
      str_nhdplus_version := 'nhdplus_m';
      
   ELSIF UPPER(p_nhdplus_version) IN ('HR','NHDPLUS_H')
   THEN
      str_nhdplus_version := 'nhdplus_h';
   
   ELSE
      RAISE EXCEPTION 'unknown nhdplus resolution %',p_nhdplus_version;
   
   END IF;
   
   boo_updn_gather_streams := p_updn_gather_streams;
   str_updn_streams_between := UPPER(p_updn_streams_between);
   IF boo_updn_gather_streams
   OR str_updn_streams_between IN ('PP','PPALL')
   THEN
      boo_return_flowline_details  := TRUE;
      boo_return_flowline_geometry := TRUE;
      
   ELSE
      IF str_updn_streams_between IS NULL
      THEN
         str_updn_streams_between := 'NONE';
      
      
      END IF;
      
      boo_return_flowline_details  := FALSE;
      boo_return_flowline_geometry := FALSE;

   END IF;

   boo_ignore_matching_ids := p_ignore_matching_ids;
   IF boo_ignore_matching_ids IS NULL
   THEN
      boo_ignore_matching_ids := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Index ids if needed
   ----------------------------------------------------------------------------
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstoprocess',p_points_to_process_id,TRUE)
   THEN
      EXECUTE 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_pointstoprocess_u99 ON cipsrv_upload.' || str_dataset_prefix || '_pointstoprocess(' || str_points_to_process_id || ')';
   
   END IF;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstoprocess',p_points_to_process_nhdplusid,FALSE)
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_pointstoprocess_i88 ON cipsrv_upload.' || str_dataset_prefix || '_pointstoprocess(' || p_points_to_process_nhdplusid || ')';
   
   END IF;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstoprocess',p_points_to_process_measure,FALSE)
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_pointstoprocess_i89 ON cipsrv_upload.' || str_dataset_prefix || '_pointstoprocess(' || p_points_to_process_measure || ')';
   
   END IF;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstosearch',p_points_to_search_id,TRUE)
   THEN
      EXECUTE 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_pointstosearch_u99 ON cipsrv_upload.' || str_dataset_prefix || '_pointstosearch(' || str_points_to_search_id || ')';
   
   END IF;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstosearch',p_points_to_search_nhdplusid,FALSE)
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_pointstosearch_i88 ON cipsrv_upload.' || str_dataset_prefix || '_pointstosearch(' || p_points_to_search_nhdplusid || ')';
   
   END IF;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstosearch',p_points_to_search_measure,FALSE)
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_pointstosearch_i89 ON cipsrv_upload.' || str_dataset_prefix || '_pointstosearch(' || p_points_to_search_measure || ')';
   
   END IF;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Analyze incoming tables
   ----------------------------------------------------------------------------
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_pointstoprocess';
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_pointstosearch';
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Create the results table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_results';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_results_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_results( '
           || '    objectid                      INTEGER     NOT NULL '
           || '   ,process_pointid               VARCHAR     NOT NULL '
           || '   ,process_pointnhdplusid        NUMERIC     NOT NULL '
           || '   ,process_pointmeasure          NUMERIC     NOT NULL '
           || '   ,search_pointid                VARCHAR     NOT NULL '
           || '   ,search_pointnhdplusid         NUMERIC     NOT NULL '
           || '   ,search_pointmeasure           NUMERIC     NOT NULL '
           || '   ,network_distancekm            NUMERIC '
           || '   ,network_flowtimeday           NUMERIC '
           || '   ,shape                         GEOMETRY '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_results TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_results_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_results( '
           || '    process_pointid '
           || '   ,search_pointid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_results_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_results( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_results_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Create the streams table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_streams';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_streams_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_streams( '
           || '    objectid                      INTEGER     NOT NULL '
           || '   ,process_pointid               VARCHAR(80) NOT NULL '
           || '   ,nhdplusid                     NUMERIC     NOT NULL '
           || '   ,permanent_identifier          VARCHAR(40) NOT NULL '
           || '   ,reachcode                     VARCHAR(14) '
           || '   ,fmeasure                      NUMERIC '
           || '   ,tmeasure                      NUMERIC '
           || '   ,network_distancekm            NUMERIC '
           || '   ,network_flowtimeday           NUMERIC '
           || '   ,lengthkm                      NUMERIC '
           || '   ,flowtimeday                   NUMERIC '
           || '   ,ftype                         INTEGER '
           || '   ,fcode                         INTEGER '
           || '   ,gnis_id                       VARCHAR '
           || '   ,gnis_name                     VARCHAR '
           || '   ,wbarea_permanent_identifier   VARCHAR(40) '
           || '   ,navtermination_flag           INTEGER '
           || '   ,nav_order                     INTEGER '
           || '   ,shape                         GEOMETRY '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_streams TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_streams_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_streams( '
           || '    process_pointid '
           || '   ,nhdplusid '
           || '   ,fmeasure '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_streams_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_streams( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_streams_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Create the between table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_between';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_between_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_between( '
           || '    objectid                      INTEGER     NOT NULL '
           || '   ,process_pointid               VARCHAR(80) NOT NULL '
           || '   ,search_pointid                VARCHAR(80) NOT NULL '
           || '   ,nhdplusid                     NUMERIC     NOT NULL '
           || '   ,permanent_identifier          VARCHAR(40) NOT NULL '
           || '   ,reachcode                     VARCHAR(14) '
           || '   ,fmeasure                      NUMERIC '
           || '   ,tmeasure                      NUMERIC '
           || '   ,network_distancekm            NUMERIC '
           || '   ,network_flowtimeday           NUMERIC '
           || '   ,lengthkm                      NUMERIC '
           || '   ,flowtimeday                   NUMERIC '
           || '   ,ftype                         INTEGER '
           || '   ,fcode                         INTEGER '
           || '   ,gnis_id                       VARCHAR '
           || '   ,gnis_name                     VARCHAR '
           || '   ,wbarea_permanent_identifier   VARCHAR(40) '
           || '   ,navtermination_flag           INTEGER '
           || '   ,nav_order                     INTEGER '
           || '   ,shape                         GEOMETRY '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_between TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_between_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_between( '
           || '    process_pointid '
           || '   ,search_pointid '
           || '   ,nhdplusid '
           || '   ,fmeasure '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_between_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_between( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_between_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Roll through the points to process
   ----------------------------------------------------------------------------
   int_counter := 1;
   str_sql := 'SELECT '
   || ' a.' || p_points_to_process_id        || ' AS pointstoprocessid '
   || ',a.' || p_points_to_process_nhdplusid || ' AS pointstoprocessnhdplusid '
   || ',a.' || p_points_to_process_measure   || ' AS pointstoprocessmeasure '
   || 'FROM '
   || 'cipsrv_upload.' || str_dataset_prefix || '_pointstoprocess a ';
   
   FOR rec IN EXECUTE str_sql
   LOOP
raise warning '% %',boo_return_flowline_details,boo_return_flowline_geometry;
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      IF str_nhdplus_version = 'nhdplus_m'
      THEN
         rec2 := cipsrv_nhdplus_m.navigate(
             p_search_type                := str_search_type
            ,p_start_nhdplusid            := rec.pointstoprocessnhdplusid::BIGINT
            ,p_start_permanent_identifier := NULL
            ,p_start_reachcode            := NULL
            ,p_start_hydroseq             := NULL
            ,p_start_measure              := rec.pointstoprocessmeasure
            ,p_stop_nhdplusid             := NULL
            ,p_stop_permanent_identifier  := NULL
            ,p_stop_reachcode             := NULL
            ,p_stop_hydroseq              := NULL
            ,p_stop_measure               := NULL
            ,p_max_distancekm             := num_max_distancekm
            ,p_max_flowtimeday            := NULL
            ,p_return_flowline_details    := boo_return_flowline_details
            ,p_return_flowline_geometry   := boo_return_flowline_geometry
         );
         int_grid_srid       := rec2.out_grid_srid;
         int_flowline_count  := rec2.out_flowline_count;
         int_return_code     := rec2.out_return_code;
         str_status_message  := rec2.out_status_message;
         
      ELSIF str_nhdplus_version = 'nhdplus_h'
      THEN
         rec2 := cipsrv_nhdplus_h.navigate(
             p_search_type                := str_search_type
            ,p_start_nhdplusid            := rec.pointstoprocessnhdplusid::BIGINT
            ,p_start_permanent_identifier := NULL
            ,p_start_reachcode            := NULL
            ,p_start_hydroseq             := NULL
            ,p_start_measure              := rec.pointstoprocessmeasure
            ,p_stop_nhdplusid             := NULL
            ,p_stop_permanent_identifier  := NULL
            ,p_stop_reachcode             := NULL
            ,p_stop_hydroseq              := NULL
            ,p_stop_measure               := NULL
            ,p_max_distancekm             := num_max_distancekm
            ,p_max_flowtimeday            := NULL
            ,p_return_flowline_details    := boo_return_flowline_details
            ,p_return_flowline_geometry   := boo_return_flowline_geometry
         );
         int_grid_srid       := rec2.out_grid_srid;
         int_flowline_count  := rec2.out_flowline_count;
         int_return_code     := rec2.out_return_code;
         str_status_message  := rec2.out_status_message;
      
      ELSE
         RAISE EXCEPTION 'err %',str_nhdplus_version;
         
      END IF;
      
      IF int_return_code = 0
      THEN
         IF int_flowline_count > c_analyze_threshold
         THEN
            EXECUTE 'ANALYZE tmp_navigation_results';
      
         END IF;
      
         --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
         EXECUTE 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_results( '
         || '    objectid '
         || '   ,process_pointid '
         || '   ,process_pointnhdplusid '
         || '   ,process_pointmeasure '
         || '   ,search_pointid '
         || '   ,search_pointnhdplusid '
         || '   ,search_pointmeasure '
         || '   ,network_distancekm '
         || '   ,network_flowtimeday '
         || '   ,shape '
         || ') '
         || 'SELECT '
         || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_results_seq'') '
         || ',$1 '
         || ',$2 '
         || ',$3 '
         || ',b.' || p_points_to_search_id        || ' '
         || ',b.' || p_points_to_search_nhdplusid || ' '
         || ',b.' || p_points_to_search_measure   || ' '
         || ',cipsrv_engine.adjust_point_extent( '
         || '   p_extent_value      := a.network_distancekm '
         || '  ,p_direction         := $4 '
         || '  ,p_flowline_amount   := a.lengthkm '
         || '  ,p_flowline_fmeasure := a.fmeasure '
         || '  ,p_flowline_tmeasure := a.tmeasure '
         || '  ,p_event_measure     := b.' || p_points_to_search_measure   || ' '
         || ' ) AS network_distancekm '
         || ',cipsrv_engine.adjust_point_extent( '
         || '   p_extent_value      => a.network_flowtimeday '
         || '  ,p_direction         => $5 '
         || '  ,p_flowline_amount   => a.flowtimeday '
         || '  ,p_flowline_fmeasure => a.fmeasure '
         || '  ,p_flowline_tmeasure => a.tmeasure '
         || '  ,p_event_measure     => b.' || p_points_to_search_measure   || ' '
         || ' ) AS network_flowtimeday '
         || ',b.shape '
         || 'FROM '
         || 'tmp_navigation_results a '
         || 'JOIN '
         || 'cipsrv_upload.' || str_dataset_prefix || '_pointstosearch b '
         || 'ON '
         || '    b.' || p_points_to_search_nhdplusid || ' =  a.nhdplusid '
         || 'AND b.' || p_points_to_search_measure   || ' >= a.fmeasure '
         || 'AND b.' || p_points_to_search_measure   || ' <= a.tmeasure '
         || 'WHERE '
         || '$6 IS FALSE OR b.' || p_points_to_search_id || ' != $7 '
         USING
          rec.pointstoprocessid
         ,rec.pointstoprocessnhdplusid
         ,rec.pointstoprocessmeasure
         ,str_search_type
         ,str_search_type
         ,boo_ignore_matching_ids
         ,rec.pointstoprocessid;

         IF boo_updn_gather_streams
         THEN
            EXECUTE 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_streams( '
            || '    objectid '
            || '   ,process_pointid '
            || '   ,nhdplusid '
            || '   ,permanent_identifier '
            || '   ,reachcode '
            || '   ,fmeasure '
            || '   ,tmeasure '
            || '   ,network_distancekm '
            || '   ,network_flowtimeday '
            || '   ,lengthkm '
            || '   ,flowtimeday '
            || '   ,ftype '
            || '   ,fcode '
            || '   ,gnis_id '
            || '   ,gnis_name '
            || '   ,wbarea_permanent_identifier '
            || '   ,navtermination_flag '
            || '   ,nav_order '
            || '   ,shape '
            || ') '
            || 'SELECT '
            || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_streams_seq'') '
            || ',$1 '
            || ',a.nhdplusid '
            || ',a.permanent_identifier '
            || ',a.reachcode '
            || ',a.fmeasure '
            || ',a.tmeasure '
            || ',a.network_distancekm '
            || ',a.network_flowtimeday '
            || ',a.lengthkm '
            || ',a.flowtimeday '
            || ',SUBSTR(a.fcode::VARCHAR,1,3)::INTEGER '
            || ',a.fcode '
            || ',a.gnis_id '
            || ',a.gnis_name '
            || ',a.wbarea_permanent_identifier '
            || ',a.navtermination_flag '
            || ',a.nav_order '
            || ',a.shape '
            || 'FROM '
            || 'tmp_navigation_results a '
            USING
            rec.pointstoprocessid;
           
         END IF;
      
      END IF;
      
      --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      int_counter := int_counter + 1;
      IF int_counter > int_commit_limit
      THEN
         COMMIT;
         int_counter := 1;
         
      END IF;
   
   END LOOP;
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Roll through the results for further streams generation
   ----------------------------------------------------------------------------
   IF str_updn_streams_between != 'NONE'
   THEN
      str_sql := 'SELECT '
      || ' a.objectid '
      || ',a.process_pointid '
      || ',a.process_pointnhdplusid '
      || ',a.process_pointmeasure '
      || ',a.search_pointid '
      || ',a.search_pointnhdplusid '
      || ',a.search_pointmeasure '
      || ',a.network_distancekm '
      || ',a.network_flowtimeday '
      || 'FROM '
      || 'cipsrv_upload.' || str_dataset_prefix || '_results a '
      || 'WHERE '
      || '    a.network_distancekm IS NOT NULL '
      || 'AND a.network_distancekm > 0 ';
   
      FOR rec IN EXECUTE str_sql
      LOOP
      
         --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
         IF str_nhdplus_version = 'nhdplus_m'
         THEN
            rec2 := cipsrv_nhdplus_m.navigate(
                p_search_type                := str_updn_streams_between
               ,p_start_nhdplusid            := rec.process_pointnhdplusid::BIGINT
               ,p_start_permanent_identifier := NULL
               ,p_start_reachcode            := NULL
               ,p_start_hydroseq             := NULL
               ,p_start_measure              := rec.process_pointmeasure
               ,p_stop_nhdplusid             := rec.search_pointnhdplusid::BIGINT
               ,p_stop_permanent_identifier  := NULL
               ,p_stop_reachcode             := NULL
               ,p_stop_hydroseq              := NULL
               ,p_stop_measure               := rec.search_pointmeasure
               ,p_max_distancekm             := NULL
               ,p_max_flowtimeday            := NULL
               ,p_return_flowline_details    := boo_return_flowline_details
               ,p_return_flowline_geometry   := boo_return_flowline_geometry
            );
            int_grid_srid       := rec2.out_grid_srid;
            int_flowline_count  := rec2.out_flowline_count;
            int_return_code     := rec2.out_return_code;
            str_status_message  := rec2.out_status_message;
            
         ELSIF str_nhdplus_version = 'nhdplus_h'
         THEN
            rec2 := cipsrv_nhdplus_h.navigate(
                p_search_type                := str_updn_streams_between
               ,p_start_nhdplusid            := rec.process_pointnhdplusid::BIGINT
               ,p_start_permanent_identifier := NULL
               ,p_start_reachcode            := NULL
               ,p_start_hydroseq             := NULL
               ,p_start_measure              := rec.process_pointmeasure
               ,p_stop_nhdplusid             := rec.search_pointnhdplusid::BIGINT
               ,p_stop_permanent_identifier  := NULL
               ,p_stop_reachcode             := NULL
               ,p_stop_hydroseq              := NULL
               ,p_stop_measure               := rec.search_pointmeasure
               ,p_max_distancekm             := NULL
               ,p_max_flowtimeday            := NULL
               ,p_return_flowline_details    := boo_return_flowline_details
               ,p_return_flowline_geometry   := boo_return_flowline_geometry
            );
            int_grid_srid       := rec2.out_grid_srid;
            int_flowline_count  := rec2.out_flowline_count;
            int_return_code     := rec2.out_return_code;
            str_status_message  := rec2.out_status_message;
         
         ELSE
            RAISE EXCEPTION 'err %',str_nhdplus_version;
            
         END IF;
      
         IF int_return_code = 0
         THEN
            IF int_flowline_count > c_analyze_threshold
            THEN
               EXECUTE 'ANALYZE tmp_navigation_results';
         
            END IF;
            
            str_sql := 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_between( '
            || '    objectid '
            || '   ,process_pointid '
            || '   ,search_pointid '
            || '   ,nhdplusid '
            || '   ,permanent_identifier '
            || '   ,reachcode '
            || '   ,fmeasure '
            || '   ,tmeasure '
            || '   ,network_distancekm '
            || '   ,network_flowtimeday '
            || '   ,lengthkm '
            || '   ,flowtimeday '
            || '   ,ftype '
            || '   ,fcode '
            || '   ,gnis_id '
            || '   ,gnis_name '
            || '   ,wbarea_permanent_identifier '
            || '   ,navtermination_flag '
            || '   ,nav_order '
            || '   ,shape '
            || ') '
            || 'SELECT '
            || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_between_seq'') '
            || ',$1 '
            || ',$2 '
            || ',a.nhdplusid '
            || ',a.permanent_identifier '
            || ',a.reachcode '
            || ',a.fmeasure '
            || ',a.tmeasure '
            || ',a.network_distancekm '
            || ',a.network_flowtimeday '
            || ',a.lengthkm '
            || ',a.flowtimeday '
            || ',SUBSTR(a.fcode::VARCHAR,1,3)::INTEGER '
            || ',a.fcode '
            || ',a.gnis_id '
            || ',a.gnis_name '
            || ',a.wbarea_permanent_identifier '
            || ',a.navtermination_flag '
            || ',a.nav_order '
            || ',a.shape '
            || 'FROM '
            || 'tmp_navigation_results a ';
            
            EXECUTE str_sql 
            USING
             rec.process_pointid
            ,rec.search_pointid;
            
         END IF;
      
      END LOOP;
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER PROCEDURE cipsrv_engine.updn_batch_search(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,BOOLEAN
   ,VARCHAR
   ,BOOLEAN
   ,VARCHAR
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON PROCEDURE cipsrv_engine.updn_batch_search(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,BOOLEAN
   ,VARCHAR
   ,BOOLEAN
   ,VARCHAR
   ,INTEGER
) TO PUBLIC;

