DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.pointindexing';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.pointindexing(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   rec1                              RECORD;
   rec2                              RECORD;
   json_input                        JSONB := $1;
   sdo_point                         GEOMETRY;
   str_nhdplus_version               VARCHAR;
   boo_return_link_path              BOOLEAN;
   str_known_region                  VARCHAR;
   str_indexing_engine               VARCHAR;
   ary_fcode_allow                   INTEGER[];
   ary_fcode_deny                    INTEGER[];
   num_distance_max_distkm           NUMERIC;
   num_raindrop_snap_max_distkm      NUMERIC;
   num_raindrop_path_max_distkm      NUMERIC;
   boo_limit_innetwork               BOOLEAN;
   boo_limit_navigable               BOOLEAN;
   ary_fallback_fcode_allow          INTEGER[];
   ary_fallback_fcode_deny           INTEGER[];
   num_fallback_distance_max_distkm  NUMERIC;
   boo_fallback_limit_innetwork      BOOLEAN;
   boo_fallback_limit_navigable      BOOLEAN;
   bint_known_catchment_nhdplusid    BIGINT;
   int_return_code                   INTEGER;
   str_status_message                VARCHAR;
   
BEGIN
   
   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   IF JSONB_PATH_EXISTS(json_input,'$.point')
   AND json_input->'point' IS NOT NULL 
   THEN
      sdo_point := cipsrv_engine.json2geometry(json_input->'point');
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    -10
         ,'status_message', 'input point is required.'
      );
   
   END IF;
   
   IF sdo_point IS NULL
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    -10
         ,'status_message', 'valid input point is required.'
      );
   
   ELSIF ST_GeometryType(sdo_point) != 'ST_Point'
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    -20
         ,'status_message', 'geometry must be single point.'
      );
   
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.indexing_engine')
   AND json_input->>'indexing_engine' IS NOT NULL
   THEN
      str_indexing_engine := json_input->>'indexing_engine';
      
   ELSE
      str_indexing_engine := 'DISTANCE';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fcode_allow')
   THEN
      IF jsonb_typeof(json_input->'fcode_allow') = 'array'
      THEN
         ary_fcode_allow := ARRAY(
            SELECT jsonb_array_elements(json_input->'fcode_allow')
         );
         
      END IF;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fcode_deny')
   THEN
      IF jsonb_typeof(json_input->'fcode_deny') = 'array'
      THEN
         ary_fcode_deny := ARRAY(
            SELECT jsonb_array_elements(json_input->'fcode_deny')
         );
         
      END IF;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.distance_max_distkm')
   AND json_input->'distance_max_distkm' IS NOT NULL
   AND json_input->>'distance_max_distkm' != ''
   THEN
      num_distance_max_distkm := cipsrv_engine.json2numeric(json_input->'distance_max_distkm');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.raindrop_snap_max_distkm')
   AND json_input->'raindrop_snap_max_distkm' IS NOT NULL
   AND json_input->>'raindrop_snap_max_distkm' != ''
   THEN
      num_raindrop_snap_max_distkm := cipsrv_engine.json2numeric(json_input->'raindrop_snap_max_distkm');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.raindrop_path_max_distkm')
   AND json_input->'raindrop_path_max_distkm' IS NOT NULL
   AND json_input->>'raindrop_path_max_distkm' != ''
   THEN
      num_raindrop_path_max_distkm := cipsrv_engine.json2numeric(json_input->'raindrop_path_max_distkm');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.limit_innetwork')
   AND json_input->>'limit_innetwork' IS NOT NULL
   THEN
      boo_limit_innetwork := (json_input->>'limit_innetwork')::BOOLEAN;
      
   ELSE
      boo_limit_innetwork := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.limit_navigable')
   AND json_input->>'limit_navigable' IS NOT NULL
   THEN
      boo_limit_navigable := (json_input->>'limit_navigable')::BOOLEAN;
      
   ELSE
      boo_limit_navigable := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fallback_fcode_allow')
   THEN
      IF jsonb_typeof(json_input->'fallback_fcode_allow') = 'array'
      THEN
         ary_fallback_fcode_allow := ARRAY(
            SELECT jsonb_array_elements(json_input->'fallback_fcode_allow')
         );
         
      END IF;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fallback_fcode_deny')
   THEN
      IF jsonb_typeof(json_input->'fallback_fcode_deny') = 'array'
      THEN
         ary_fallback_fcode_deny := ARRAY(
            SELECT jsonb_array_elements(json_input->'fallback_fcode_deny')
         );
         
      END IF;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fallback_distance_max_distkm')
   AND json_input->'fallback_distance_max_distkm' IS NOT NULL
   AND json_input->>'fallback_distance_max_distkm' != ''
   THEN
      num_fallback_distance_max_distkm := cipsrv_engine.json2numeric(json_input->'fallback_distance_max_distkm');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fallback_limit_innetwork')
   AND json_input->>'fallback_limit_innetwork' IS NOT NULL
   THEN
      boo_fallback_limit_innetwork := (json_input->>'fallback_limit_innetwork')::BOOLEAN;
      
   ELSE
      boo_fallback_limit_innetwork := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fallback_limit_navigable')
   AND json_input->>'fallback_limit_navigable' IS NOT NULL
   THEN
      boo_fallback_limit_navigable := (json_input->>'fallback_limit_navigable')::BOOLEAN;
      
   ELSE
      boo_fallback_limit_navigable := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_link_path')
   AND json_input->>'return_link_path' IS NOT NULL
   THEN
      boo_return_link_path := (json_input->>'return_link_path')::BOOLEAN;
      
   ELSE
      boo_return_link_path := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.known_region')
   AND json_input->>'known_region' IS NOT NULL
   THEN
      str_known_region := json_input->>'known_region';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.known_catchment_nhdplusid')
   AND json_input->'known_catchment_nhdplusid' IS NOT NULL 
   AND json_input->>'known_catchment_nhdplusid' != ''
   THEN
      bint_known_catchment_nhdplusid := cipsrv_engine.json2bigint(json_input->'known_catchment_nhdplusid');
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code', -10
         ,'status_message', 'nhdplus_version required.'
      );
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 20
   -- Call the engine
   --------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      rec1 := cipsrv_nhdplus_m.pointindexing(
          p_point                        := sdo_point
         ,p_indexing_engine              := str_indexing_engine
         ,p_fcode_allow                  := ary_fcode_allow
         ,p_fcode_deny                   := ary_fcode_deny
         ,p_distance_max_distkm          := num_distance_max_distkm
         ,p_raindrop_snap_max_distkm     := num_raindrop_snap_max_distkm
         ,p_raindrop_path_max_distkm     := num_raindrop_path_max_distkm
         ,p_limit_innetwork              := boo_limit_innetwork
         ,p_limit_navigable              := boo_limit_navigable
         ,p_fallback_fcode_allow         := ary_fallback_fcode_allow
         ,p_fallback_fcode_deny          := ary_fallback_fcode_deny
         ,p_fallback_distance_max_distkm := num_fallback_distance_max_distkm
         ,p_fallback_limit_innetwork     := boo_fallback_limit_innetwork
         ,p_fallback_limit_navigable     := boo_fallback_limit_navigable
         ,p_return_link_path             := boo_return_link_path
         ,p_known_region                 := str_known_region
         ,p_known_catchment_nhdplusid    := bint_known_catchment_nhdplusid
      );
      int_return_code    := rec1.out_return_code;
      str_status_message := rec1.out_status_message;
      
      IF int_return_code != 0
      THEN
         RETURN JSONB_BUILD_OBJECT(
             'return_code',    int_return_code
            ,'status_message', str_status_message
         );
      
      END IF;
      
      RETURN JSONB_BUILD_OBJECT(
          'flowlines'           ,cipsrv_nhdplus_m.snapflowlines2geojson(rec1.out_flowlines)
         ,'path_distance_km'    ,rec1.out_path_distance_km
         ,'end_point'           ,JSONB_BUILD_OBJECT(
             'type'    ,'Feature'
            ,'geometry',ST_AsGeoJSON(ST_Transform(rec1.out_end_point,4326))::JSONB
          )
         ,'indexing_line'       ,JSONB_BUILD_OBJECT(
             'type'    ,'Feature'
            ,'geometry',ST_AsGeoJSON(ST_Transform(rec1.out_indexing_line,4326))::JSONB
          )
         ,'region'              ,rec1.out_region
         ,'nhdplusid'           ,rec1.out_nhdplusid
         ,'return_code'         ,int_return_code
         ,'status_message'      ,str_status_message
      );
      
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      rec2 := cipsrv_nhdplus_h.pointindexing(
          p_point                        := sdo_point
         ,p_indexing_engine              := str_indexing_engine
         ,p_fcode_allow                  := ary_fcode_allow
         ,p_fcode_deny                   := ary_fcode_deny
         ,p_distance_max_distkm          := num_distance_max_distkm
         ,p_raindrop_snap_max_distkm     := num_raindrop_snap_max_distkm
         ,p_raindrop_path_max_distkm     := num_raindrop_path_max_distkm
         ,p_limit_innetwork              := boo_limit_innetwork
         ,p_limit_navigable              := boo_limit_navigable
         ,p_fallback_fcode_allow         := ary_fallback_fcode_allow
         ,p_fallback_fcode_deny          := ary_fallback_fcode_deny
         ,p_fallback_distance_max_distkm := num_fallback_distance_max_distkm
         ,p_fallback_limit_innetwork     := boo_fallback_limit_innetwork
         ,p_fallback_limit_navigable     := boo_fallback_limit_navigable
         ,p_return_link_path             := boo_return_link_path
         ,p_known_region                 := str_known_region
         ,p_known_catchment_nhdplusid    := bint_known_catchment_nhdplusid
      );
      int_return_code    := rec2.out_return_code;
      str_status_message := rec2.out_status_message;
      
      IF int_return_code != 0
      THEN
         RETURN JSONB_BUILD_OBJECT(
             'return_code',    int_return_code
            ,'status_message', str_status_message
         );
      
      END IF;
      
      RETURN JSONB_BUILD_OBJECT(
          'flowlines'           ,cipsrv_nhdplus_h.snapflowlines2geojson(rec2.out_flowlines)
         ,'path_distance_km'    ,rec2.out_path_distance_km
         ,'end_point'           ,JSONB_BUILD_OBJECT(
             'type'    ,'Feature'
            ,'geometry',ST_AsGeoJSON(ST_Transform(rec2.out_end_point,4326))::JSONB
          )
         ,'indexing_line'       ,JSONB_BUILD_OBJECT(
             'type'    ,'Feature'
            ,'geometry',ST_AsGeoJSON(ST_Transform(rec2.out_indexing_line,4326))::JSONB
          )
         ,'region'              ,rec2.out_region
         ,'nhdplusid'           ,rec2.out_nhdplusid
         ,'return_code'         ,int_return_code
         ,'status_message'      ,str_status_message
      );
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code', -10
         ,'status_message', 'invalid nhdplus_version.'
      );
   
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.pointindexing(
   JSONB
) OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.pointindexing(
   JSONB
) TO PUBLIC;

