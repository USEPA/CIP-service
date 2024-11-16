DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.field_exists';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.field_exists(
    IN  p_schema_name VARCHAR
   ,IN  p_table_name  VARCHAR
   ,IN  p_field_name  VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   int_oid        INTEGER;
   str_table_name VARCHAR(255);
   str_field_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for table
   ----------------------------------------------------------------------------
   SELECT 
    c.oid
   INTO int_oid
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   WHERE  
       n.nspname = p_schema_name
   AND c.relname = p_table_name
   AND c.relkind = 'r';
   
   IF int_oid IS NULL 
   THEN
      RETURN FALSE;
      
   END IF;
   
   SELECT 
    a.attname
   INTO str_field_name
   FROM 
   pg_catalog.pg_attribute a 
   WHERE 
       a.attrelid = int_oid
   AND a.attname  = p_field_name
   AND NOT attisdropped
   AND attnum > 0; 

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_field_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.field_exists(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.field_exists(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

