DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.determine_grid_srid';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.determine_grid_srid(
    IN  p_geometry             GEOMETRY
   ,IN  p_nhdplus_version      VARCHAR
   ,IN  p_known_region         VARCHAR
   ,OUT out_srid               INTEGER
   ,OUT out_grid_size          NUMERIC
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec RECORD;
   
BEGIN

   IF p_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cipsrv_nhdplus_m.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      out_srid           := rec.out_srid;
      out_grid_size      := rec.out_grid_size;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
   
   ELSIF p_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cipsrv_nhdplus_h.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      out_srid           := rec.out_srid;
      out_grid_size      := rec.out_grid_size;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;

   ELSE
      RAISE EXCEPTION 'err %',p_nhdplus_version;

   END IF;   
   
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.determine_grid_srid';
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

