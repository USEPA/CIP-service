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

