CREATE or REPLACE FUNCTION cipsrv_engine.index_exists(
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
$BODY$ LANGUAGE plpgsql;

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

