CREATE OR REPLACE FUNCTION cip20_engine.validate_feature(
    IN  p_in                           JSONB
   ,IN  p_category                     VARCHAR
   ,IN  p_known_region_srid            INTEGER
   ,IN  p_default_point_method         VARCHAR
   ,IN  p_default_line_method          VARCHAR
   ,IN  p_default_area_method          VARCHAR
   ,IN  p_default_line_threshold       NUMERIC
   ,IN  p_default_areacat_threshold    NUMERIC
   ,IN  p_default_areaevt_threshold    NUMERIC
   ,OUT out_cleaned_feature            JSONB
   ,OUT out_property_count             INTEGER
   ,OUT out_geometry_type              VARCHAR
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
   
BEGIN

   out_return_code := 0;
   out_cleaned_feature := p_in;

   IF JSONB_PATH_EXISTS(
       target := out_cleaned_feature
      ,path   := '$.type'
   )
   THEN
      IF out_cleaned_feature->>'type' = 'Feature' 
      THEN
         IF JSONB_PATH_EXISTS(
             target := out_cleaned_feature
            ,path   := '$.geometry'
         )
         THEN
            sdo_geom := ST_GeomFromGeoJSON(out_cleaned_feature->'geometry');
            out_geometry_type := out_cleaned_feature->'geometry'->>'type';
            
            IF NOT ST_IsValid(sdo_geom)
            THEN
               sdo_geom := ST_MakeValid(sdo_geom);
               
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['geometry']
                  ,replacement       := ST_AsGeoJSON(sdo_geom)::JSONB
                  ,create_if_missing := FALSE
               );
               
            END IF;
            
         ELSE
            out_return_code    := -20;
            out_status_message := 'no geometry included in feature record';
            RETURN;
            
         END IF;
         
         IF NOT JSONB_PATH_EXISTS(
             target := out_cleaned_feature
            ,path   := '$.properties'
         )
         THEN
            out_cleaned_feature := JSONB_SET(
                jsonb_in          := out_cleaned_feature
               ,path              := ARRAY['properties']
               ,replacement       := JSONB_BUILD_OBJECT()
               ,create_if_missing := TRUE
            );
         
         END IF;

         IF NOT JSONB_PATH_EXISTS(
             target := out_cleaned_feature
            ,path   := '$.properties.globalid'
         )
         THEN
            out_cleaned_feature := JSONB_SET(
                jsonb_in          := out_cleaned_feature
               ,path              := ARRAY['properties','globalid']
               ,replacement       := TO_JSONB('{' || uuid_generate_v1() || '}') 
               ,create_if_missing := TRUE
            );
            
         END IF;
           
         IF NOT JSONB_PATH_EXISTS(
             target := out_cleaned_feature
            ,path   := '$.properties.indexing_method'
         )
         THEN
            IF out_geometry_type IN ('Point','MultiPoint')
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','indexing_method']
                  ,replacement       := TO_JSONB(p_default_point_method)
                  ,create_if_missing := TRUE
               );
               
            ELSIF out_geometry_type IN ('LineString','MultiLineString')
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','indexing_method']
                  ,replacement       := TO_JSONB(p_default_line_method) 
                  ,create_if_missing := TRUE
               );
               
            ELSIF out_geometry_type IN ('Polygon','MultiPolygon')
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','indexing_method']
                  ,replacement       := TO_JSONB(p_default_area_method) 
                  ,create_if_missing := TRUE
               );
               
            ELSE
               RAISE EXCEPTION '%',out_geometry_type;
            
            END IF;
            
         END IF;
         
         IF out_geometry_type IN ('LineString','MultiLineString')
         THEN
            IF NOT JSONB_PATH_EXISTS(
                target := out_cleaned_feature
               ,path   := '$.properties.line_threshold'
            ) AND p_default_line_threshold IS NOT NULL
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','line_threshold']
                  ,replacement       := TO_JSONB(p_default_line_threshold)
                  ,create_if_missing := TRUE
               );

            END IF;
            
            num_lengthkm := ROUND(ST_Length(ST_Transform(sdo_geom,p_known_region_srid))::NUMERIC * 0.001,8);
            out_cleaned_feature := JSONB_SET(
                jsonb_in          := out_cleaned_feature
               ,path              := ARRAY['properties','lengthkm']
               ,replacement       := TO_JSONB(num_lengthkm)
               ,create_if_missing := TRUE
            );
            
         END IF;
         
         IF out_geometry_type IN ('Polygon','MultiPolygon')
         THEN
            IF NOT JSONB_PATH_EXISTS(
                target := out_cleaned_feature
               ,path   := '$.properties.areacat_threshold'
            ) AND p_default_areacat_threshold IS NOT NULL
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','areacat_threshold']
                  ,replacement       := TO_JSONB(p_default_areacat_threshold) 
                  ,create_if_missing := TRUE
               );
               
            END IF;
         
            IF NOT JSONB_PATH_EXISTS(
                target := out_cleaned_feature
               ,path   := '$.properties.areaevt_threshold'
            ) AND p_default_areaevt_threshold IS NOT NULL
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','areaevt_threshold']
                  ,replacement       := TO_JSONB(p_default_areaevt_threshold) 
                  ,create_if_missing := TRUE
               );
               
            END IF;
            
            num_areasqkm := ROUND(ST_Area(ST_Transform(sdo_geom,p_known_region_srid))::NUMERIC / 1000000,8);
            out_cleaned_feature := JSONB_SET(
                jsonb_in          := out_cleaned_feature
               ,path              := ARRAY['properties','areasqkm']
               ,replacement       := TO_JSONB(num_areasqkm)
               ,create_if_missing := TRUE
            );
            
         END IF;
         
         out_cleaned_feature := JSONB_SET(
             jsonb_in          := out_cleaned_feature
            ,path              := ARRAY['properties','category']
            ,replacement       := TO_JSONB(p_category) 
            ,create_if_missing := TRUE
         );

      ELSE
         out_return_code    := -10;
         out_status_message := 'invalid GeoJSON feature';
         RETURN;
         
      END IF;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'invalid GeoJSON feature';
      RETURN;
   
   END IF;

EXCEPTION
   WHEN OTHERS
   THEN
      RAISE;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.validate_feature(
    JSONB
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.validate_feature(
    JSONB
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

