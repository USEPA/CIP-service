DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_registry';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.cipsrv_registry()
RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   json_components JSONB;
   str_version     VARCHAR;
   dat_installed   DATE;
   str_notes       VARCHAR;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Collect the state domain values
   ----------------------------------------------------------------------------
   json_components := NULL;
   SELECT 
   JSON_AGG(a.*)
   INTO json_components
   FROM (
      SELECT
       aa.component
      ,aa.component_vintage
      ,aa.installation_date
      ,aa.notes
      FROM
      cipsrv.registry aa
      ORDER BY
      aa.component
   ) a;
   
   IF json_components IS NULL
   THEN
      json_components := '[]'::JSONB;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Collect the components
   ----------------------------------------------------------------------------
   BEGIN
      SELECT
       a.version
      ,a.installation_date
      ,a.notes
      INTO
       str_version
      ,dat_installed
      ,str_notes
      FROM
      cipsrv.version a
      LIMIT 1;
   
   EXCEPTION
      WHEN NO_DATA_FOUND THEN NULL;
      WHEN OTHERS THEN RAISE;
   
   END;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'components'       , json_components
      ,'version'          , str_version
      ,'installation_date', dat_installed
      ,'notes'            , str_notes
   );
      
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_registry';
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

