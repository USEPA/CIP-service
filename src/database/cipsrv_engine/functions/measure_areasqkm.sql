DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.measure_areasqkm';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.measure_areasqkm(
    IN  p_geometry                GEOMETRY
   ,IN  p_nhdplus_version         VARCHAR
   ,IN  p_default_nhdplus_version VARCHAR DEFAULT NULL
   ,IN  p_known_region            VARCHAR DEFAULT NULL
   ,IN  p_default_known_region    VARCHAR DEFAULT NULL
) RETURNS NUMERIC
STABLE
AS $BODY$ 
DECLARE
   str_nhdplus_version VARCHAR;
   str_known_region    VARCHAR;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_geometry IS NULL
   THEN
      RAISE EXCEPTION 'input geometry required';
      
   END IF;
   
   IF p_nhdplus_version IS NULL
   THEN
      IF p_default_nhdplus_version IS NULL
      THEN
         RAISE EXCEPTION 'nhdplus version required';
      
      ELSE
         str_nhdplus_version := p_default_nhdplus_version;
         
      END IF;
      
   ELSE
      str_nhdplus_version := p_nhdplus_version; 
   
   END IF;
   
   str_known_region := p_known_region;
   IF str_known_region IS NULL
   THEN
      str_known_region := p_default_known_region;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine the region from geometry if known region value not provided
   ----------------------------------------------------------------------------
   IF str_nhdplus_version IN ('nhdplus_m','MR')
   THEN
      RETURN cipsrv_nhdplus_m.measure_areasqkm(
          p_geometry     := p_geometry
         ,p_known_region := str_known_region
      );
   
   ELSIF str_nhdplus_version IN ('nhdplus_h','HR')
   THEN
      RETURN cipsrv_nhdplus_h.measure_areasqkm(
          p_geometry     := p_geometry
         ,p_known_region := str_known_region
      );
   
   ELSE
      RAISE EXCEPTION 'unknown nhdplus version %',str_nhdplus_version;
   
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.measure_areasqkm(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR    
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.measure_areasqkm(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

