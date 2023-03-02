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

