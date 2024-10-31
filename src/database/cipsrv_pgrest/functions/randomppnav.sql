DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.randomppnav';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.randomppnav(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$
DECLARE
   rec                 RECORD;
   json_input          JSONB := $1;
   str_region          VARCHAR;
   str_nhdplus_version VARCHAR;
   boo_return_geometry BOOLEAN;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------   
   IF JSONB_PATH_EXISTS(json_input,'$.region')
   AND json_input->>'region' IS NOT NULL
   AND json_input->>'region' != ''
   THEN
      str_region := json_input->>'region';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   AND json_input->>'nhdplus_version' != ''
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      str_nhdplus_version := 'NHDPLUS_H';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_geometry')
   AND json_input->>'return_geometry' IS NOT NULL
   THEN
      boo_return_geometry := (json_input->>'return_geometry')::BOOLEAN;
      
   ELSE
      boo_return_geometry := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Get the results
   ----------------------------------------------------------------------------
   IF UPPER(str_nhdplus_version) IN ('NHDPLUS_M','MR')
   THEN
      rec := cipsrv_nhdplus_m.randomppnav(
          p_region          := str_region
         ,p_return_geometry := boo_return_geometry
      );
      str_nhdplus_version := 'nhdplus_m';
   
   ELSIF UPPER(str_nhdplus_version) IN ('NHDPLUS_H','HR')
   THEN
      rec := cipsrv_nhdplus_h.randomppnav(
          p_region          := str_region
         ,p_return_geometry := boo_return_geometry
      );
      str_nhdplus_version := 'nhdplus_h';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'nhdplusid1'     ,rec.out_nhdplusid1
      ,'reachcode1'     ,rec.out_reachcode1
      ,'measure1'       ,rec.out_measure1
      ,'shape1'         ,ST_AsGeoJSON(ST_Transform(rec.out_shape1,4326))::JSONB
      ,'nhdplusid2'     ,rec.out_nhdplusid2
      ,'reachcode2'     ,rec.out_reachcode2
      ,'measure2'       ,rec.out_measure2
      ,'shape2'         ,ST_AsGeoJSON(ST_Transform(rec.out_shape2,4326))::JSONB
      ,'nhdplus_version',str_nhdplus_version
      ,'return_code'    ,rec.out_return_code
      ,'status_message' ,rec.out_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.randomppnav(
   JSONB
) 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.randomppnav(
   JSONB
) 
TO PUBLIC;

