CREATE OR REPLACE FUNCTION cip20_engine.measure_geometry(
    IN  p_geometry          GEOMETRY
   ,IN  p_nhdplus_version   VARCHAR
   ,IN  p_known_region      VARCHAR
   ,OUT p_measurement       NUMERIC
   ,OUT p_sub_meas          NUMERIC[]
   ,OUT p_unit              VARCHAR
   ,OUT p_return_code       INTEGER
   ,OUT p_status_message    VARCHAR
)
IMMUTABLE
AS $BODY$ 
DECLARE 
   rec          RECORD;
   int_srid     INTEGER;
   str_gtype    VARCHAR;
   int_count    INTEGER;
   
BEGIN

   p_return_code := 0;
   
   rec := cip20_engine.determine_grid_srid(
       p_geometry        := p_geometry
      ,p_nhdplus_version := p_nhdplus_version
      ,p_known_region    := p_known_region
   );
   int_srid         := rec.p_srid;
   p_return_code    := rec.p_return_code;
   p_status_message := rec.p_status_message;
   
   IF p_return_code!= 0
   THEN
      RETURN;
      
   END IF;
   
   str_gtype := ST_GeometryType(p_geometry);
   int_count := ST_NumGeometries(p_geometry);
   
   IF str_gtype IN ('ST_Point','ST_MultiPoint')
   THEN
      p_measurement   := NULL;
      p_unit          := NULL;
      
   ELSIF str_gtype IN ('ST_LineString','ST_MultiLineString')
   THEN
      p_measurement   := ROUND(ST_Length(ST_Transform(p_geometry,int_srid))::NUMERIC * 0.001,8);
      p_unit          := 'KM';
      int_count       := ST_NumGeometries(p_geometry);
      
      IF int_count = 1
      THEN
         p_sub_meas[1] := p_measurement;
      
      ELSE
         FOR i IN 1 .. int_count
         LOOP
            p_sub_meas[i] := ROUND(ST_Length(ST_Transform(ST_GeometryN(p_geometry,i),int_srid))::NUMERIC * 0.001,8);
            
         END LOOP;

      END IF;
      
   ELSIF str_gtype IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      p_measurement   := ROUND(ST_Area(ST_Transform(p_geometry,int_srid))::NUMERIC / 1000000,8);
      p_unit          := 'SQ_KM';
      int_count       := ST_NumGeometries(p_geometry);
      
      IF int_count = 1
      THEN
         p_sub_meas[1] := p_measurement;
      
      ELSE
         FOR i IN 1 .. int_count
         LOOP
            p_sub_meas[i] := ROUND(ST_Area(ST_Transform(ST_GeometryN(p_geometry,i),int_srid))::NUMERIC / 1000000,8);
            
         END LOOP;

      END IF;         
   
   ELSE
      p_return_code    := -1;
      p_status_message := 'unable to measure ' || str_gtype;
      RETURN; 
   
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.measure_geometry(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.measure_geometry(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

