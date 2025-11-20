DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.randomcatchment';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.randomcatchment(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$
DECLARE
   rec                  RECORD;
   json_input           JSONB := $1;
   str_region           VARCHAR;
   str_nhdplus_version  VARCHAR;
   int_known_nhdplusid  BIGINT;
   boo_include_extended BOOLEAN;
   boo_return_geometry  BOOLEAN;
   
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
   
   IF JSONB_PATH_EXISTS(json_input,'$.include_extended')
   AND json_input->>'include_extended' IS NOT NULL
   THEN
      boo_include_extended := (json_input->>'include_extended')::BOOLEAN;
      
   ELSE
      boo_include_extended := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_geometry')
   AND json_input->>'return_geometry' IS NOT NULL
   THEN
      boo_return_geometry := (json_input->>'return_geometry')::BOOLEAN;
      
   ELSE
      boo_return_geometry := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.known_nhdplusid')
   AND json_input->'known_nhdplusid' IS NOT NULL
   AND json_input->>'known_nhdplusid' != ''
   THEN
      int_known_nhdplusid := cipsrv_engine.json2bigint(json_input->'known_nhdplusid');
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Get the results
   ----------------------------------------------------------------------------
   IF UPPER(str_nhdplus_version) IN ('NHDPLUS_M','MR')
   THEN
      rec := cipsrv_nhdplus_m.randomcatchment(
          p_region           := str_region
         ,p_include_extended := boo_include_extended
         ,p_return_geometry  := boo_return_geometry
         ,p_known_nhdplusid  := int_known_nhdplusid
      );
      str_nhdplus_version := 'nhdplus_m';
   
   ELSIF UPPER(str_nhdplus_version) IN ('NHDPLUS_H','HR')
   THEN
      rec := cipsrv_nhdplus_h.randomcatchment(
          p_region           := str_region
         ,p_include_extended := boo_include_extended
         ,p_return_geometry  := boo_return_geometry
         ,p_known_nhdplusid  := int_known_nhdplusid
      );
      str_nhdplus_version := 'nhdplus_h';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'catchment'          ,JSONB_BUILD_OBJECT(
          'type'        ,'Feature'
         ,'geometry'    ,ST_AsGeoJSON(ST_Transform(rec.out_shape,4326))::JSONB
         ,'properties'  ,JSONB_BUILD_OBJECT(
             'nhdplusid'          ,rec.out_nhdplusid
            ,'areasqkm'           ,rec.out_areasqkm
            ,'catchmentstatecodes',rec.out_catchmentstatecodes
          )
       )
      ,'centroid'           ,JSONB_BUILD_OBJECT(
          'type'        ,'Feature'
         ,'geometry'    ,ST_AsGeoJSON(ST_Transform(rec.out_centroid,4326))::JSONB
       )
      ,'nhdplus_version'    ,str_nhdplus_version
      ,'return_code'        ,rec.out_return_code
      ,'status_message'     ,rec.out_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.randomcatchment(
   JSONB
) 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.randomcatchment(
   JSONB
) 
TO PUBLIC;

