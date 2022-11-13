CREATE OR REPLACE FUNCTION cip20_engine.append_feature(
    IN  p_in                         JSONB
   ,IN  p_feature                    JSONB
   ,IN  p_geometry                   GEOMETRY
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE 
   json_result      JSONB;
   int_count        INTEGER;
   
BEGIN

   json_result := p_in;
   
   IF json_result IS NULL
   THEN
      json_result := JSONB_BUILD_OBJECT(
          'type',    'FeatureCollection'
         ,'features', JSONB_BUILD_ARRAY()
      );
      
   END IF;
   
   int_count := JSONB_ARRAY_LENGTH(json_result->'features');
   
   IF p_feature IS NOT NULL
   THEN
      json_result := JSONB_SET(
          jsonb_in          := json_result
         ,path              := ARRAY['features',int_count::VARCHAR]
         ,replacement       := p_feature 
         ,create_if_missing := TRUE
      );   
   
   ELSIF p_geometry IS NOT NULL
   THEN
      json_result := JSONB_SET(
          jsonb_in          := json_result
         ,path              := ARRAY['features',int_count::VARCHAR]
         ,replacement       := JSONB_BUILD_OBJECT(
             'type',      'Feature'
            ,'geometry',  ST_AsGeoJSON(p_geometry)::JSONB
          ) 
         ,create_if_missing := TRUE
      );
   
   END IF;
   
   RETURN json_result;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.append_feature(
    JSONB
   ,JSONB
   ,GEOMETRY
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.append_feature(
    JSONB
   ,JSONB
   ,GEOMETRY
) TO PUBLIC;

