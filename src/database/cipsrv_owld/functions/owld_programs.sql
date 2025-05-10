DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.owld_programs';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_owld.owld_programs()
RETURNS TABLE(
    program_id          VARCHAR
   ,program_name        VARCHAR
   ,program_short_name  VARCHAR
   ,program_description VARCHAR
   ,program_url         VARCHAR
   ,program_eventtype   INTEGER
   ,program_vintage     DATE
   ,program_resolutions VARCHAR[]
   ,program_precisions  VARCHAR[]
)
VOLATILE
AS $BODY$
DECLARE
   rec         RECORD;
   rec2        RECORD;
   int_counter INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Collect available programs
   ----------------------------------------------------------------------------
   FOR rec IN 
      SELECT
       a.table_name
      ,UPPER(REPLACE(a.table_name,'_control','')) AS program_id 
      FROM 
      information_schema.tables a
      WHERE 
          a.table_schema  = 'cipsrv_owld'
      AND a.table_name LIKE '%_control'
   LOOP
      program_id          := rec.program_id;
      program_name        := NULL;
      program_short_name  := NULL;
      program_description := NULL;
      program_url         := NULL;
      program_eventtype   := NULL;
      program_vintage     := NULL;
      program_resolutions := NULL;
      
      BEGIN
         EXECUTE 'SELECT a.value_str FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''NAME'' LIMIT 1' 
         INTO program_name; 
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      BEGIN
         EXECUTE 'SELECT a.value_str FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''SHORT_NAME'' LIMIT 1' 
         INTO program_short_name; 
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      BEGIN
         EXECUTE 'SELECT a.value_str FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''DESCRIPTION'' LIMIT 1' 
         INTO program_description; 
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      BEGIN
         EXECUTE 'SELECT a.value_str FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''URL'' LIMIT 1' 
         INTO program_url; 
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      BEGIN
         EXECUTE 'SELECT a.value_num::INT FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''EVENTTYPE'' LIMIT 1' 
         INTO program_eventtype; 
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      BEGIN
         EXECUTE 'SELECT a.value_date FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''VINTAGE'' LIMIT 1' 
         INTO program_vintage;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      int_counter := 1;
      program_resolutions = NULL;
      FOR rec2 IN EXECUTE '
         SELECT
         a.value_str
         FROM cipsrv_owld.' || rec.table_name || ' a
         WHERE UPPER(a.keyword) = ''RESOLUTION'' 
         ORDER BY a.value_str'
      LOOP
         program_resolutions[int_counter] := rec2.value_str;
         int_counter := int_counter + 1;
      
      END LOOP;
      
      int_counter := 1;
      program_precisions = NULL;
      FOR rec2 IN EXECUTE '
         SELECT
         a.value_str
         FROM cipsrv_owld.' || rec.table_name || ' a
         WHERE UPPER(a.keyword) = ''PRECISION'' 
         ORDER BY a.value_str'
      LOOP
         program_precisions[int_counter] := rec2.value_str;
         int_counter := int_counter + 1;
      
      END LOOP;
      
      RETURN NEXT;
      
   END LOOP;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.owld_programs';
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
