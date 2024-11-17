DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.json2bigint';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.json2bigint(
    IN  p_in                         JSONB
) RETURNS BIGINT
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   IF p_in IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF JSONB_TYPEOF(p_in) = 'string'
   THEN
      IF p_in::VARCHAR IN ('',' ','null','""')
      THEN
         RETURN NULL;

      ELSE            
         RETURN REPLACE(
            p_in::VARCHAR
           ,'"'
           ,''           
         )::BIGINT;
         
      END IF;
   
   ELSE
      RETURN p_in;

   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.json2bigint(
   JSONB
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.json2bigint(
   JSONB
) TO PUBLIC;

