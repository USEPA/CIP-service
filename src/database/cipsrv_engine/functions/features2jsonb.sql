CREATE OR REPLACE FUNCTION cipsrv_engine.features2jsonb(
    IN  p_features      cipsrv_engine.cip_feature[]
   ,IN  p_geometry_type VARCHAR DEFAULT NULL
   ,IN  p_empty_array   BOOLEAN DEFAULT FALSE
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
      IF p_empty_array
      THEN
         RETURN '[]'::JSONB;
      
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
   
   RETURN ary_rez;  

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

