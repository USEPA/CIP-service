CREATE OR REPLACE FUNCTION cipsrv_engine.validate_feature_collection(
    IN  p_in                           JSONB
   ,IN  p_test_type                    VARCHAR
   ,IN  p_category                     VARCHAR
   ,IN  p_known_region_srid            INTEGER
   ,IN  p_default_point_method         VARCHAR
   ,IN  p_default_line_method          VARCHAR
   ,IN  p_default_ring_method          VARCHAR
   ,IN  p_default_area_method          VARCHAR
   ,IN  p_default_line_threshold       NUMERIC
   ,IN  p_default_areacat_threshold    NUMERIC
   ,IN  p_default_areaevt_threshold    NUMERIC
   ,OUT out_cleaned_feature_collection JSONB
   ,OUT out_feature_count              INTEGER
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
IMMUTABLE
AS $BODY$ 
DECLARE 
   rec              RECORD;
   json_feature     JSONB;
   str_feature_type VARCHAR;
   
BEGIN

   out_return_code := 0;
   out_cleaned_feature_collection := p_in;
   
   IF JSONB_PATH_EXISTS(out_cleaned_feature_collection,'$.type')
   THEN
      IF out_cleaned_feature_collection->>'type' IN (
          'Point'
         ,'LineString'
         ,'Polygon'
         ,'MultiPoint'
         ,'MultiLineString'
         ,'MultiPolygon'
         ,'GeometryCollection'
      )
      THEN
         out_cleaned_feature_collection := JSONB_BUILD_OBJECT(
             'type',     'Feature'
            ,'geometry', out_cleaned_feature_collection
         );
      
      END IF;

      IF out_cleaned_feature_collection->>'type' = 'Feature'
      THEN
         out_cleaned_feature_collection := JSONB_BUILD_OBJECT(
             'type',     'FeatureCollection'
            ,'features', JSONB_BUILD_ARRAY(
               out_cleaned_feature_collection
             )
         );
      
      END IF;

      IF  out_cleaned_feature_collection->>'type' = 'FeatureCollection' 
      AND JSONB_PATH_EXISTS(out_cleaned_feature_collection,'$.features')
      THEN
         IF JSONB_TYPEOF(out_cleaned_feature_collection->'features') = 'array'
         THEN
            out_feature_count := JSONB_ARRAY_LENGTH(out_cleaned_feature_collection->'features');

            FOR i IN 0 .. out_feature_count - 1
            LOOP
               rec := cipsrv_engine.validate_feature(
                   p_in                        := out_cleaned_feature_collection->'features'->i
                  ,p_category                  := p_category
                  ,p_known_region_srid         := p_known_region_srid
                  ,p_default_point_method      := p_default_point_method
                  ,p_default_line_method       := p_default_line_method
                  ,p_default_area_method       := p_default_area_method
                  ,p_default_ring_method       := p_default_ring_method
                  ,p_default_line_threshold    := p_default_line_threshold
                  ,p_default_areacat_threshold := p_default_areacat_threshold
                  ,p_default_areaevt_threshold := p_default_areaevt_threshold 
               );
               json_feature       := rec.out_cleaned_feature;
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               str_feature_type   := rec.out_geometry_type;

               IF out_return_code != 0
               THEN
                  RETURN;
                  
               END IF;
               
               IF p_test_type IS NOT NULL
               THEN
                  -- Note this needs to be loose enough to allow ring polygons
                  IF (p_test_type = 'Point'      AND str_feature_type NOT IN ('Point','MultiPoint'))
                  OR (p_test_type = 'LineString' AND str_feature_type NOT IN ('LineString','MultiLineString','ST_Polygon'))
                  OR (p_test_type = 'Polygon'    AND str_feature_type NOT IN ('Polygon','MultiPolygon'))
                  THEN
                     out_return_code    := -30;
                     out_status_message := 'geometry type ' || str_feature_type || ' does not match test type ' || p_test_type;
                     RETURN;
                     
                  END IF;
               
               END IF;

               out_cleaned_feature_collection := JSONB_SET(
                   out_cleaned_feature_collection
                  ,ARRAY['features',i::VARCHAR]
                  ,json_feature
                  ,FALSE
               );

               IF NOT JSONB_PATH_EXISTS(
                   target := out_cleaned_feature_collection
                  ,path   := '$.globalid'
               )
               THEN
                  out_cleaned_feature_collection := JSONB_SET(
                      out_cleaned_feature_collection
                     ,ARRAY['globalid']
                     ,TO_JSONB('{' || uuid_generate_v1() || '}') 
                     ,TRUE
                  );
                  
               END IF;
               
            END LOOP;
         
         ELSE
            out_return_code    := -10;
            out_status_message := 'invalid GeoJSON feature collection';
      
         END IF;
      
      ELSE
         out_return_code    := -10;
         out_status_message := 'invalid GeoJSON feature collection';
      
      END IF;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'invalid GeoJSON feature collection';
   
   END IF;
 
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.validate_feature_collection(
    JSONB
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.validate_feature_collection(
    JSONB
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

