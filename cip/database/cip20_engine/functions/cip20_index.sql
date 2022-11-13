CREATE OR REPLACE FUNCTION cip20_engine.cip20_index(
    IN  p_points                    JSONB
   ,IN  p_lines                     JSONB
   ,IN  p_areas                     JSONB
   ,IN  p_geometry                  GEOMETRY
   ,IN  p_state_filter              VARCHAR
   ,IN  p_nhdplus_version           VARCHAR
   ,IN  p_wbd_version               VARCHAR
   ,IN  p_default_point_method      VARCHAR
   ,IN  p_default_line_method       VARCHAR
   ,IN  p_default_area_method       VARCHAR
   ,IN  p_default_line_threshold    NUMERIC
   ,IN  p_default_areacat_threshold NUMERIC
   ,IN  p_default_areaevt_threshold NUMERIC
   ,IN  p_known_region              VARCHAR
   ,IN  p_return_catchment_geometry BOOLEAN
   ,OUT out_indexed_points          JSONB
   ,OUT out_indexed_lines           JSONB
   ,OUT out_indexed_areas           JSONB
   ,OUT out_indexed_collection      GEOMETRY
   ,OUT out_indexing_summary        JSONB
   ,OUT out_catchment_count         INTEGER
   ,OUT out_catchment_areasqkm      NUMERIC
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                           RECORD;
   str_gtype                     VARCHAR;
   int_meassrid                  INTEGER;
   int_gridsize                  INTEGER;
   
   json_points                   JSONB;
   json_lines                    JSONB;
   json_areas                    JSONB;
   json_features                 JSONB;
   json_feature                  JSONB;
   
   sdo_input                     GEOMETRY;
   sdo_sampler                   GEOMETRY;
   sdo_state_clipped             GEOMETRY;
   boo_return_geometry           BOOLEAN;
   str_state_filter              VARCHAR(2);
   
   str_default_point_method      VARCHAR;
   str_default_line_method       VARCHAR;
   str_default_area_method       VARCHAR;
   num_default_line_threshold    NUMERIC;
   num_default_areacat_threshold NUMERIC;
   num_default_areaevt_threshold NUMERIC;
   str_known_region              VARCHAR;
   str_geometry_type             VARCHAR;
   int_feature_count             INTEGER;
   str_indexing_method           VARCHAR;
   num_lengthkm                  NUMERIC;
   num_areasqkm                  NUMERIC;
   num_line_threshold            NUMERIC;
   num_areacat_threshold         NUMERIC;
   num_areaevt_threshold         NUMERIC;
   str_category                  VARCHAR;
   boo_check                     BOOLEAN;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   json_points := p_points;
   json_lines  := p_lines;
   json_areas  := p_areas;
   IF  json_points   IS NULL
   AND json_lines    IS NULL
   AND json_areas    IS NULL
   AND p_geometry    IS NULL
   THEN
      out_return_code    := -10;
      out_status_message := 'input geometry cannot be null';
      RETURN;
   
   END IF;
   
   IF p_nhdplus_version IS NULL
   THEN
      out_return_code    := -10;
      out_status_message := 'nhdplus version cannot be null';
      RETURN;
      
   ELSIF p_nhdplus_version NOT IN ('nhdplus_m','nhdplus_h') 
   THEN
      out_return_code    := -10;
      out_status_message := 'invalid nhdplus version';
      RETURN;
   
   END IF;
   
   str_state_filter := UPPER(p_state_filter);
   
   str_default_point_method  := p_default_point_method;
   IF str_default_point_method IS NULL
   THEN
      str_default_point_method := 'point_simple';
      
   ELSIF str_default_point_method NOT IN ('point_simple')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP point indexing method';
      RETURN;
    
   END IF;
   
   str_default_line_method  := p_default_line_method;
   IF str_default_line_method IS NULL
   THEN
      str_default_line_method := 'line_simple';
      
   ELSIF str_default_line_method NOT IN ('line_simple','line_levelpath')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP line indexing method';
      RETURN;
    
   END IF;
   
   str_default_area_method  := p_default_area_method;
   IF str_default_area_method IS NULL
   THEN
      str_default_area_method := 'area_simple';
      
   ELSIF str_default_area_method NOT IN ('area_simple','area_centroid','area_artpath')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP area indexing method';
      RETURN;
    
   END IF;
   
   num_default_line_threshold := p_default_line_threshold;
   IF num_default_line_threshold IS NULL
   THEN
      num_default_line_threshold := 10;
      
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
   
   boo_return_geometry := p_return_catchment_geometry;
   IF boo_return_geometry IS NULL
   THEN
      boo_return_geometry := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine measurement srid
   ----------------------------------------------------------------------------
   str_known_region := p_known_region;
   
   IF str_known_region IS NULL
   THEN
      IF p_geometry IS NOT NULL
      THEN
         sdo_sampler := cip20_engine.sample_geometry(p_in := p_geometry);

      ELSE
         IF json_points IS NOT NULL
         THEN
            sdo_sampler := cip20_engine.sample_geometry(
               p_in := ST_GeomFromGeoJSON(json_points->'features'->0->'geometry')
            );
            
         ELSE
            IF json_lines IS NOT NULL
            THEN
               sdo_sampler := cip20_engine.sample_geometry(
                  p_in := ST_GeomFromGeoJSON(json_lines->'features'->0->'geometry')
               );
               
            ELSE
               sdo_sampler := cip20_engine.sample_geometry(
                  p_in := ST_GeomFromGeoJSON(json_areas->'features'->0->'geometry')
               );
               
            END IF;
            
         END IF;

      END IF;
      
   END IF;

   rec := cip20_engine.determine_grid_srid(
       p_geometry        := sdo_sampler
      ,p_nhdplus_version := p_nhdplus_version
      ,p_known_region    := str_known_region
   );
   int_meassrid       := rec.out_srid;
   int_gridsize       := rec.out_grid_size;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_meassrid::VARCHAR;

   ----------------------------------------------------------------------------
   -- Step 30
   -- Validate incoming geometry workload
   ----------------------------------------------------------------------------
   IF p_geometry IS NOT NULL
   THEN
      json_points := NULL;
      json_lines  := NULL;
      json_areas  := NULL;
      
      str_geometry_type := ST_GeometryType(p_geometry);
      IF str_geometry_type NOT IN ('GeometryCollection')
      THEN
         IF str_geometry_type IN ('ST_Point','ST_MultiPoint')
         THEN
            json_points := cip20_engine.append_feature(
                p_in         := json_points
               ,p_feature    := NULL
               ,p_geometry   := p_geometry
            );
            
         ELSIF str_geometry_type IN ('ST_LineString','ST_MultiLineString')
         THEN
            json_lines := cip20_engine.append_feature(
                p_in         := json_lines
               ,p_feature    := NULL
               ,p_geometry   := p_geometry
            );
            
         ELSIF str_geometry_type IN ('ST_Polygon','ST_MultiPolygon')
         THEN
            json_areas := cip20_engine.append_feature(
                p_in         := json_areas
               ,p_feature    := NULL
               ,p_geometry   := p_geometry
            );
         
         ELSE
            RAISE EXCEPTION 'err %',str_geometry_type;
         
         END IF;
      
      ELSE
         FOR i IN 1 .. ST_NumGeometries(p_geometry)
         LOOP
            sdo_input := ST_Transform(ST_GeometryN(p_geometry,i),4326);
            str_geometry_type := ST_GeometryType(sdo_input);
    
            IF str_geometry_type = 'ST_Point'
            THEN
               json_points := cip20_engine.append_feature(
                   p_in         := json_points
                  ,p_feature    := NULL
                  ,p_geometry   := sdo_input
               );
               
            ELSIF str_geometry_type = 'ST_LineString'
            THEN
               json_lines := cip20_engine.append_feature(
                   p_in         := json_lines
                  ,p_feature    := NULL
                  ,p_geometry   := sdo_input
               );
               
            ELSIF str_geometry_type = 'ST_Polygon'
            THEN
               json_areas := cip20_engine.append_feature(
                   p_in         := json_areas
                  ,p_feature    := NULL
                  ,p_geometry   := sdo_input
               );
            
            ELSE
               RAISE EXCEPTION 'err %',str_geometry_type;
            
            END IF;
            
         END LOOP;
         
      END IF;

   END IF;

   IF json_points IS NOT NULL
   THEN
      rec := cip20_engine.validate_feature_collection(
          p_in                        := json_points
         ,p_test_type                 := 'Point'
         ,p_category                  := 'points'
         ,p_known_region_srid         := int_meassrid
         ,p_default_point_method      := p_default_point_method
         ,p_default_line_method       := p_default_line_method
         ,p_default_area_method       := p_default_area_method
         ,p_default_line_threshold    := p_default_line_threshold
         ,p_default_areacat_threshold := p_default_areacat_threshold
         ,p_default_areaevt_threshold := p_default_areaevt_threshold 
      );
      json_points        := rec.out_cleaned_feature_collection;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
      
      END IF;         
      
   END IF;

   IF json_lines IS NOT NULL
   THEN
      rec := cip20_engine.validate_feature_collection(
          p_in                        := json_lines
         ,p_test_type                 := 'LineString'
         ,p_category                  := 'lines'
         ,p_known_region_srid         := int_meassrid
         ,p_default_point_method      := p_default_point_method
         ,p_default_line_method       := p_default_line_method
         ,p_default_area_method       := p_default_area_method
         ,p_default_line_threshold    := p_default_line_threshold
         ,p_default_areacat_threshold := p_default_areacat_threshold
         ,p_default_areaevt_threshold := p_default_areaevt_threshold 
      );
      json_lines         := rec.out_cleaned_feature_collection;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;

      IF out_return_code != 0
      THEN
         RETURN;
      
      END IF;
      
   END IF;
   
   IF json_areas IS NOT NULL
   THEN
      rec := cip20_engine.validate_feature_collection(
          p_in                        := json_areas
         ,p_test_type                 := 'Polygon'
         ,p_category                  := 'areas'
         ,p_known_region_srid         := int_meassrid
         ,p_default_point_method      := p_default_point_method
         ,p_default_line_method       := p_default_line_method
         ,p_default_area_method       := p_default_area_method
         ,p_default_line_threshold    := p_default_line_threshold
         ,p_default_areacat_threshold := p_default_areacat_threshold
         ,p_default_areaevt_threshold := p_default_areaevt_threshold 
      );
      json_areas         := rec.out_cleaned_feature_collection;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
      
      END IF;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Index the catchments
   ----------------------------------------------------------------------------
   out_return_code := cip20_engine.create_cip_temp_tables();
   json_features := NULL;   
   
   IF json_points IS NOT NULL
   THEN
      IF json_features IS NULL
      OR JSONB_ARRAY_LENGTH(json_features) = 0
      THEN
         json_features := json_points->'features';
      
      ELSE
         json_features := json_features || (json_points->'features');
         
      END IF;
      
   END IF;
 
   IF json_lines IS NOT NULL
   THEN
      IF json_features IS NULL
      OR JSONB_ARRAY_LENGTH(json_features) = 0
      THEN
         json_features := json_lines->'features';
      
      ELSE
         json_features := json_features || (json_lines->'features');
         
      END IF;
      
   END IF;
   
   IF json_areas IS NOT NULL
   THEN
      IF json_features IS NULL
      OR JSONB_ARRAY_LENGTH(json_features) = 0
      THEN
         json_features := json_areas->'features';
      
      ELSE
         json_features := json_features || (json_areas->'features');
         
      END IF;
      
   END IF;

   int_feature_count := JSONB_ARRAY_LENGTH(json_features);

   FOR i IN 0 .. int_feature_count - 1
   LOOP
      str_indexing_method   := json_features->i->'properties'->>'indexing_method';
      sdo_input             := ST_GeomFromGeoJSON(json_features->i->'geometry');
      num_lengthkm          := json_features->i->'properties'->>'lengthkm';
      num_areasqkm          := json_features->i->'properties'->>'areasqkm';
      num_line_threshold    := json_features->i->'properties'->>'line_threshold';
      num_areacat_threshold := json_features->i->'properties'->>'areacat_threshold';
      num_areaevt_threshold := json_features->i->'properties'->>'areaevt_threshold';
 
      IF str_indexing_method = 'point_simple'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_point_simple(
                p_geometry               := sdo_input
               ,p_known_region           := str_known_region
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
                     
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_point_simple(
                p_geometry               := sdo_input
               ,p_known_region           := str_known_region
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
         
      ELSIF str_indexing_method = 'line_simple'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_line_simple(
                p_geometry               := sdo_input
               ,p_geometry_lengthkm      := num_lengthkm
               ,p_known_region           := str_known_region
               ,p_line_threashold_perc   := num_line_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_line_simple(
                p_geometry               := sdo_input
               ,p_geometry_lengthkm      := num_lengthkm
               ,p_known_region           := str_known_region
               ,p_line_threashold_perc   := num_line_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
         
      ELSIF str_indexing_method = 'line_levelpath'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_line_levelpath(
                p_geometry               := sdo_input
               ,p_geometry_lengthkm      := num_lengthkm
               ,p_known_region           := str_known_region
               ,p_line_threashold_perc   := num_line_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_line_levelpath(
                p_geometry               := sdo_input
               ,p_geometry_lengthkm      := num_lengthkm
               ,p_known_region           := str_known_region
               ,p_line_threashold_perc   := num_line_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
         
      ELSIF str_indexing_method = 'area_artpath'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_area_artpath(
                p_geometry               := sdo_input
               ,p_geometry_areasqkm      := num_areasqkm
               ,p_known_region           := str_known_region
               ,p_cat_threashold_perc    := num_areacat_threshold
               ,p_evt_threashold_perc    := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
         
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_area_artpath(
                p_geometry               := sdo_input
               ,p_geometry_areasqkm      := num_areasqkm
               ,p_known_region           := str_known_region
               ,p_cat_threashold_perc    := num_areacat_threshold
               ,p_evt_threashold_perc    := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
         
      ELSIF str_indexing_method = 'area_simple'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_area_simple(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := num_areasqkm
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := num_areacat_threshold
               ,p_evt_threashold_perc  := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;

         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_area_simple(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := num_areasqkm
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := num_areacat_threshold
               ,p_evt_threashold_perc  := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
         
      ELSIF str_indexing_method = 'area_centroid'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_area_centroid(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := num_areasqkm
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := num_areacat_threshold
               ,p_evt_threashold_perc  := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_area_centroid(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := num_areasqkm
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := num_areacat_threshold
               ,p_evt_threashold_perc  := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
      
      ELSE
         RAISE EXCEPTION 'err %',str_indexing_method;
         
      END IF;

      IF out_return_code != 0
      THEN
         RETURN;
         
      END IF;      
      
   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Return the full state-clipped catchment results
   ----------------------------------------------------------------------------
   IF p_nhdplus_version = 'nhdplus_m'
   THEN
      INSERT INTO tmp_cip_out(
          nhdplusid
         ,catchmentstatecode
         ,xwalk_huc12
         ,areasqkm
         ,shape
      )
      SELECT
       a.nhdplusid
      ,a.catchmentstatecode
      ,a.xwalk_huc12
      ,a.areasqkm
      ,CASE
       WHEN boo_return_geometry
       THEN
         a.shape
       ELSE
         CAST(NULL AS GEOMETRY)       
       END AS shape
      FROM
      cip20_nhdplus_m.catchment_fabric a
      WHERE
      EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
      AND (str_state_filter IS NULL OR a.catchmentstatecode = str_state_filter);
   
   ELSIF p_nhdplus_version = 'nhdplus_h'
   THEN
      INSERT INTO tmp_cip_out(
          nhdplusid
         ,catchmentstatecode
         ,xwalk_huc12
         ,areasqkm
         ,shape
      )
      SELECT
       a.nhdplusid
      ,a.catchmentstatecode
      ,a.xwalk_huc12
      ,a.areasqkm
      ,CASE
       WHEN boo_return_geometry
       THEN
         a.shape
       ELSE
         CAST(NULL AS GEOMETRY)       
       END AS shape
      FROM
      cip20_nhdplus_h.catchment_fabric a
      WHERE
      EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
      AND (str_state_filter IS NULL OR a.catchmentstatecode = str_state_filter);
   
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
   int_feature_count := JSONB_ARRAY_LENGTH(json_features);
      
   FOR i IN 0 .. int_feature_count - 1
   LOOP
      boo_check := TRUE;
      json_feature := json_features->i;
      str_category := json_feature->'properties'->>'category';

      IF str_state_filter IS NOT NULL
      AND str_state_filter NOT IN ('AK','HI','AS')
      THEN
         sdo_input := ST_GeomFromGeoJSON(json_feature->'geometry');
         str_geometry_type := ST_GeometryType(sdo_input);
         
         rec := cip20_support.clip_by_state(
             p_geometry     := sdo_input
            ,p_known_region := str_known_region
            ,p_state_filter := p_state_filter
         );
         sdo_input          := rec.out_clipped_geometry;
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         
         IF out_return_code != 0
         THEN
            RETURN;
            
         END IF;
         
         IF sdo_input IS NULL
         THEN
            boo_check := FALSE;
            
         ELSE
            IF str_category = 'lines'
            THEN
               num_lengthkm := ROUND(ST_Length(ST_Transform(sdo_input,int_meassrid))::NUMERIC * 0.001,8);
               json_feature := JSONB_SET(
                   jsonb_in          := json_feature
                  ,path              := ARRAY['properties','lengthkm']
                  ,replacement       := TO_JSONB(num_lengthkm)
                  ,create_if_missing := TRUE
               );
            
            ELSIF str_category = 'areas'
            THEN
               num_areasqkm := ROUND(ST_Area(ST_Transform(sdo_input,int_meassrid))::NUMERIC / 1000000,8);
               json_feature := JSONB_SET(
                   jsonb_in          := json_feature
                  ,path              := ARRAY['properties','areasqkm']
                  ,replacement       := TO_JSONB(num_areasqkm)
                  ,create_if_missing := TRUE
               );
            
            END IF;
         
         END IF;
      
      END IF;
      
      IF boo_check
      THEN
         json_feature := JSONB_SET(
             jsonb_in          := json_feature
            ,path              := ARRAY['geometry']
            ,replacement       := ST_AsGeoJSON(ST_Transform(sdo_input,4326))::JSONB
            ,create_if_missing := TRUE
         );
         
         IF str_category = 'points'
         THEN
            out_indexed_points := cip20_engine.append_feature(
                p_in        := out_indexed_points
               ,p_feature   := json_feature
               ,p_geometry  := NULL
            );
            
         ELSIF str_category = 'lines'
         THEN
            out_indexed_lines := cip20_engine.append_feature(
                p_in        := out_indexed_lines
               ,p_feature   := json_feature
               ,p_geometry  := NULL
            );
         
         ELSIF str_category = 'areas'
         THEN
            out_indexed_areas := cip20_engine.append_feature(
                p_in        := out_indexed_areas
               ,p_feature   := json_feature
               ,p_geometry  := NULL
            );
      
         END IF;
         
      END IF;

   END LOOP;  
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.cip20_index(
    JSONB
   ,JSONB
   ,JSONB
   ,GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,VARCHAR
   ,BOOLEAN
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.cip20_index(
    JSONB
   ,JSONB
   ,JSONB
   ,GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,VARCHAR
   ,BOOLEAN
) TO PUBLIC;

