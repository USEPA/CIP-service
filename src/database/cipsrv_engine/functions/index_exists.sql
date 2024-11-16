DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.index_exists';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.index_exists(
    IN  p_schema_name VARCHAR
   ,IN  p_table_name  VARCHAR
   ,IN  p_index_name  VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_index_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for table
   ----------------------------------------------------------------------------
   SELECT 
   a.indexname
   INTO str_index_name
   FROM 
   pg_indexes a
   WHERE
       a.schemaname = p_schema_name
   AND a.tablename  = p_table_name
   AND a.indexname  = p_index_name;

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_index_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.index_exists(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.index_exists(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

