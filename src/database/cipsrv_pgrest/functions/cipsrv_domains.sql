DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_domains';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.cipsrv_domains()
RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   json_states JSONB;
   json_tribes JSONB;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Collect the state domain values
   ----------------------------------------------------------------------------
   SELECT 
   JSON_AGG(a.*)
   INTO json_states
   FROM (
      SELECT
       aa.geoid
      ,aa.stusps
      ,aa.name
      FROM
      cipsrv_support.tiger_fedstatewaters aa
      ORDER BY
      aa.stusps
   ) a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Collect the tribes domain values
   ----------------------------------------------------------------------------
   SELECT 
   JSON_AGG(a.*)
   INTO json_tribes
   FROM (
      SELECT
       aa.aiannhns_stem
      ,aa.aiannhns_namelsad
      ,aa.has_reservation_lands
      ,aa.has_trust_lands
      FROM
      cipsrv_support.tribal_crosswalk aa
      ORDER BY
      aa.aiannhns_stem
   ) a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'states', json_states
      ,'tribes', json_tribes
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.cipsrv_domains() 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.cipsrv_domains() 
TO PUBLIC;

