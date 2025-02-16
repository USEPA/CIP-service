DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_owld_programs';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.cipsrv_owld_programs()
RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   json_program  JSONB;
   json_array    JSONB;
   rec           RECORD;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Collect the available owld programs in system
   ----------------------------------------------------------------------------
   json_program := NULL;
   
   IF EXISTS(
      SELECT 1 FROM information_schema.routines a
      WHERE a.routine_schema = 'cipsrv_owld' AND a.routine_name = 'owld_programs'
   )
   THEN
      FOR rec IN
      SELECT a.* FROM cipsrv_owld.owld_programs() a
      LOOP
         json_program := ROW_TO_JSON(rec);
         
         IF json_array IS NULL
         THEN
            json_array := JSONB_BUILD_ARRAY(
               json_program
            );
            
         ELSE
            json_array := json_array || json_program;
            
         END IF;
         
      END LOOP;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   IF json_array IS NULL
   THEN
      json_array := '[]'::JSONB;
      
   END IF;
   
   RETURN JSON_BUILD_OBJECT(
       'programs', json_array
   );
      
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_owld_programs';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv_pgrest',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv_pgrest',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;

