DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.owld_locator';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_owld.owld_locator(
    IN  p_nhdplus_version               VARCHAR
   ,IN  p_source_featureid              VARCHAR
   ,IN  p_source_featureid2             VARCHAR
   ,IN  p_source_originator             VARCHAR
   ,IN  p_source_series                 VARCHAR
   ,IN  p_start_date                    DATE
   ,IN  p_end_date                      DATE
   ,IN  p_linked_data_program           VARCHAR
   ,IN  p_source_joinkey                VARCHAR
   ,IN  p_permid_joinkey                VARCHAR
   ,IN  p_cat_joinkey                   VARCHAR
   ,IN  p_search_direction              VARCHAR
   ,IN  p_reference_catchment_nhdplusid BIGINT
   ,IN  p_reference_reachcode           VARCHAR
   ,IN  p_reference_flowline_nhdplusid  BIGINT
   ,IN  p_reference_hydroseq            BIGINT
   ,IN  p_reference_measure             NUMERIC
   ,IN  p_reference_point               GEOMETRY
   ,IN  p_search_precision              VARCHAR
   ,IN  p_known_region                  VARCHAR DEFAULT NULL
   ,OUT out_catchment_nhdplusid         BIGINT
   ,OUT out_reachcode                   VARCHAR
   ,OUT out_flowline_nhdplusid          BIGINT
   ,OUT out_hydroseq                    BIGINT
   ,OUT out_measure                     NUMERIC
   ,OUT out_shape                       GEOMETRY
   ,OUT out_source_featureid            VARCHAR
   ,OUT out_source_featureid2           VARCHAR
   ,OUT out_source_originator           VARCHAR
   ,OUT out_source_series               VARCHAR
   ,OUT out_source_subdivision          VARCHAR
   ,OUT out_start_date                  DATE
   ,OUT out_end_date                    DATE
   ,OUT out_source_joinkey              VARCHAR
   ,OUT out_permid_joinkey              VARCHAR
   ,OUT out_nhdplus_version             VARCHAR
   ,OUT out_cat_joinkey                 VARCHAR
   ,OUT out_return_code                 NUMERIC
   ,OUT out_status_message              VARCHAR
)
VOLATILE
AS $BODY$
DECLARE

BEGIN

   out_return_code := cipsrv_engine.create_navigation_temp_tables();

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   out_return_code    := 0;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.owld_locator';
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
