DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.cipsrv_version';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE or REPLACE FUNCTION cipsrv_engine.cipsrv_version()
RETURNS VARCHAR
STABLE
AS $BODY$
DECLARE
BEGIN

   RETURN '1.0';

END;
$BODY$ LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.cipsrv_version()
OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.cipsrv_version()
TO PUBLIC;

