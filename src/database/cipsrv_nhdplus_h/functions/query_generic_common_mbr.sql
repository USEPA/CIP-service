DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.query_generic_common_mbr';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.query_generic_common_mbr(
   IN  p_input  GEOMETRY
) RETURNS VARCHAR
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_point GEOGRAPHY;
   
BEGIN

   IF p_input IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   sdo_point := ST_Transform(
       ST_PointOnSurface(p_input)
      ,4326
   )::GEOGRAPHY;
   
   IF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('CONUS')
   )
   THEN
      RETURN 'CONUS';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('HI')
   )
   THEN
      RETURN 'HI';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('PRVI')
   )
   THEN
      RETURN 'PRVI';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('AK')
   )
   THEN
      RETURN 'AK';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('GUMP')
   )
   THEN
      RETURN 'GUMP';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('SAMOA')
   )
   THEN
      RETURN 'SAMOA';
      
   END IF;
   
   RETURN NULL;  
   
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.query_generic_common_mbr';
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

