DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.column_has_single_index';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.column_has_single_index(
    IN  p_schema_name  VARCHAR
   ,IN  p_table_name   VARCHAR
   ,IN  p_column_name  VARCHAR
   ,IN  p_unique       BOOLEAN DEFAULT FALSE
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   boo_results BOOLEAN;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for table
   ----------------------------------------------------------------------------
   SELECT
   TRUE
   INTO boo_results
   FROM 
   pg_class t
   JOIN
   pg_namespace ns
   ON
   t.relnamespace = ns.oid
   JOIN
   pg_index ix
   ON
   t.oid = ix.indrelid
   JOIN
   pg_class i
   ON
   i.oid = ix.indexrelid
   JOIN
   pg_attribute a
   ON
       a.attrelid = t.oid
   AND a.attnum = ANY(ix.indkey)
   WHERE 
       t.relkind  = 'r'
   AND ns.nspname = p_schema_name
   AND t.relname  = p_table_name
   AND attname    = p_column_name
   AND (NOT p_unique OR ix.indisunique)
   GROUP BY
    a.attname
   ,i.relname
   HAVING 
   COUNT(*) = 1;

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   RETURN COALESCE(boo_results,FALSE);

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.column_has_single_index(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.column_has_single_index(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
) TO PUBLIC;

