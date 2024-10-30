DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.randomhuc12';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.randomhuc12(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$
DECLARE
   rec                 RECORD;
   json_input          JSONB := $1;
   str_region          VARCHAR;
   str_source_dataset  VARCHAR;
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
   
   IF JSONB_PATH_EXISTS(json_input,'$.source_dataset')
   AND json_input->>'source_dataset' IS NOT NULL
   AND json_input->>'source_dataset' != ''
   THEN
      str_source_dataset := json_input->>'source_dataset';
      
   ELSE
      str_source_dataset := 'NP21';
      
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
   rec := cipsrv_support.randomhuc12(
       p_region          := str_region
      ,p_source_dataset  := str_source_dataset
      ,p_return_geometry := boo_return_geometry
   );
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'huc12'          ,rec.out_huc12
      ,'huc12_name'     ,rec.out_huc12_name
      ,'source_dataset' ,rec.out_source_dataset
      ,'shape'          ,ST_AsGeoJSON(ST_Transform(rec.out_shape,4326))::JSONB
      ,'return_code'    ,rec.out_return_code
      ,'status_message' ,rec.out_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.randomhuc12(
   JSONB
) 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.randomhuc12(
   JSONB
) 
TO PUBLIC;

