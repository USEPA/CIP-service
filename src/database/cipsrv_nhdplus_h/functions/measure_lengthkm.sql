DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.measure_lengthkm';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.measure_lengthkm(
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
   rec := cipsrv_nhdplus_h.determine_grid_srid(
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
   sdo_geometry := public.ST_TRANSFORM(
       p_geometry
      ,int_srid
   );
   
   IF public.ST_GEOMETRYTYPE(sdo_geometry) = 'ST_GeometryCollection'
   THEN
      sdo_geometry := public.ST_COLLECTIONEXTRACT(sdo_geometry,2);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return results in km
   ----------------------------------------------------------------------------
   IF sdo_geometry IS NULL OR ST_ISEMPTY(sdo_geometry)
   THEN
      RETURN 0;
      
   ELSIF public.ST_GEOMETRYTYPE(sdo_geometry) IN ('ST_LineString','ST_MultiLineString')
   THEN
      RETURN ROUND(public.ST_LENGTH(sdo_geometry)::NUMERIC * 0.001,8);
      
   ELSE
      RAISE EXCEPTION 'measure lengthkm requires linear geometry - %',public.ST_GEOMETRYTYPE(sdo_geometry);
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.measure_lengthkm';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;

