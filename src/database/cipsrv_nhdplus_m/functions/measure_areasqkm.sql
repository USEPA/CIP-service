DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.measure_areasqkm';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.measure_areasqkm(
    IN  p_geometry          GEOMETRY
   ,IN  p_known_region      VARCHAR DEFAULT NULL
) RETURNS NUMERIC
STABLE
AS $BODY$ 
DECLARE
   rec                RECORD;
   int_srid           INTEGER;
   int_return_code    INTEGER;
   str_status_message VARCHAR;
   sdo_geometry       GEOMETRY;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_geometry IS NULL
   THEN
      RAISE EXCEPTION 'input geometry required';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine the region from geometry if known region value not provided
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   int_return_code    := rec.out_return_code;
   str_status_message := rec.out_status_message;
  
   ----------------------------------------------------------------------------
   -- Step 30
   -- Project as needed
   ----------------------------------------------------------------------------
   sdo_geometry := ST_Transform(
       p_geometry
      ,int_srid
   );
   
   IF ST_GeometryType(sdo_geometry) = 'ST_GeometryCollection'
   THEN
      sdo_geometry := ST_CollectionExtract(sdo_geometry,3);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return results in km
   ----------------------------------------------------------------------------
   IF sdo_geometry IS NULL OR ST_IsEmpty(sdo_geometry)
   THEN
      RETURN 0;
      
   ELSIF ST_GeometryType(sdo_geometry) IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      RETURN ROUND(ST_Area(sdo_geometry)::NUMERIC / 1000000,8);
      
   ELSE
      RAISE EXCEPTION 'measure areasqkm requires polygon geometry - %',ST_GeometryType(sdo_geometry);
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.measure_areasqkm(
    GEOMETRY
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.measure_areasqkm(
    GEOMETRY
   ,VARCHAR
) TO PUBLIC;

