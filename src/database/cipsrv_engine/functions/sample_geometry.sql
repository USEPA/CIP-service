CREATE OR REPLACE FUNCTION cipsrv_engine.sample_geometry(
    IN  p_in                         GEOMETRY
) RETURNS GEOMETRY
IMMUTABLE
AS $BODY$ 
DECLARE 
   str_geometry_type VARCHAR;
   sdo_first_geom    GEOMETRY;
   
BEGIN

   IF p_in IS NULL
   OR ST_IsEmpty(p_in)
   THEN
      RETURN NULL;
      
   END IF;
   
   str_geometry_type := ST_GeometryType(p_in);
   
   IF str_geometry_type = 'ST_Point'
   THEN
      RETURN p_in;
      
   ELSIF str_geometry_type = 'ST_LineString'
   THEN
      RETURN ST_PointN(p_in,1);
      
   ELSIF str_geometry_type = 'ST_Polygon'
   THEN
      RETURN ST_PointN(ST_ExteriorRing(p_in),1);
      
   ELSIF str_geometry_type = 'ST_MultiPoint'
   THEN
      RETURN ST_GeometryN(p_in,1);
      
   ELSIF str_geometry_type = 'ST_MultiLineString'
   THEN
      RETURN ST_PointN(ST_GeometryN(p_in,1),1);
      
   ELSIF str_geometry_type = 'ST_MultiPolygon'
   THEN
      RETURN ST_PointN(ST_ExteriorRing(ST_GeometryN(p_in,1)),1);
      
   ELSIF str_geometry_type = 'ST_MultiPolygon'
   THEN
      sdo_first_geom := ST_GeometryN(p_in,1);
      str_geometry_type := ST_GeometryType(sdo_first_geom);
   
      IF str_geometry_type = 'ST_Point'
      THEN
         RETURN sdo_first_geom;
         
      ELSIF str_geometry_type = 'ST_LineString'
      THEN
         RETURN ST_PointN(sdo_first_geom,1);
         
      ELSIF str_geometry_type = 'ST_Polygon'
      THEN
         RETURN ST_PointN(ST_ExteriorRing(sdo_first_geom),1);
         
      END IF;
   
   ELSE
      RAISE EXCEPTION 'err %',str_geometry_type;
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.sample_geometry(
   GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.sample_geometry(
   GEOMETRY
) TO PUBLIC;

