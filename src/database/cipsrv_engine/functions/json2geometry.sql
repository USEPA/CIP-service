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

