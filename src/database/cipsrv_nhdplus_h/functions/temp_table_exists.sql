DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.temp_table_exists';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.temp_table_exists(
   IN p_table_name VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_table_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for temp table
   ----------------------------------------------------------------------------
   SELECT 
    n.nspname
   INTO str_table_name
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   where 
       n.nspname like 'pg_temp_%'
   AND pg_catalog.pg_table_is_visible(c.oid)
   AND UPPER(relname) = UPPER(p_table_name);

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_table_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.temp_table_exists(
   VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.temp_table_exists(
   VARCHAR
) TO PUBLIC;

