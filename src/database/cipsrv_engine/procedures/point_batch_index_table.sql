DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.point_batch_index_table';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP PROCEDURE IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE PROCEDURE cipsrv_engine.point_batch_index_table(
    IN  p_cipsrv_upload_table              VARCHAR
   ,IN  p_nhdplus_version                  VARCHAR
   ,IN  p_point_indexing_method            VARCHAR
   ,OUT out_return_code                    INTEGER
   ,OUT out_status_message                 VARCHAR
   ,IN  p_fcode_allow                      INTEGER[] DEFAULT NULL
   ,IN  p_fcode_deny                       INTEGER[] DEFAULT NULL
   ,IN  p_distance_max_dist_km             NUMERIC   DEFAULT 15
   ,IN  p_return_link_path                 BOOLEAN   DEFAULT NULL
   ,IN  p_limit_innetwork                  BOOLEAN   DEFAULT FALSE
   ,IN  p_limit_navigable                  BOOLEAN   DEFAULT FALSE
   ,IN  p_known_region                     VARCHAR   DEFAULT NULL
   ,IN  p_commit_limit                     INTEGER   DEFAULT 2000
)
AS $BODY$ 
DECLARE
   rec                                RECORD;
   rec2                               RECORD;
   str_nhdplus_version                VARCHAR;
   str_point_indexing_method          VARCHAR;
   str_cipsrv_upload_table            VARCHAR;
   num_nhdplusid                      NUMERIC;
   str_reachcode                      VARCHAR;
   num_snap_measure                   NUMERIC;
   num_snap_distancekm                NUMERIC;
   int_return_code                    INTEGER;
   str_status_message                 VARCHAR;
   int_counter                        INTEGER;
   geo_snap_point                     GEOMETRY;
   
BEGIN

   out_return_code := cipsrv_engine.create_cip_batch_temp_tables();
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_cipsrv_upload_table IS NULL
   THEN
      RAISE EXCEPTION 'p_cipsrv_upload_table required';
   
   ELSE
      str_cipsrv_upload_table := LOWER(REPLACE(p_cipsrv_upload_table,';',''));
      
      IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_cipsrv_upload_table)
      THEN
         RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_cipsrv_upload_table;
      
      END IF;
      
   END IF;
   
   IF p_nhdplus_version IS NULL
   THEN
      RAISE EXCEPTION 'p_nhdplus_version required';
   
   ELSIF UPPER(p_nhdplus_version) IN ('MEDIUM','MR','3')
   THEN
      str_nhdplus_version := 'MR';
      
   ELSIF UPPER(p_nhdplus_version) IN ('HIGH','HR','2')
   THEN
      str_nhdplus_version := 'HR';
      
   END IF;
   
   IF p_point_indexing_method IS NULL
   THEN
      RAISE EXCEPTION 'p_point_indexing_method required';
   
   ELSIF UPPER(p_point_indexing_method) IN ('CATCHMENT CONSTRAINED','CATCONSTRAINED')
   THEN
      str_point_indexing_method := 'CATCONSTRAINED';
      
   ELSIF UPPER(p_point_indexing_method) IN ('DISTANCE')
   THEN
      str_point_indexing_method := 'DISTANCE';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Add or clear output fields
   ----------------------------------------------------------------------------
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_guid')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_guid VARCHAR(40)';
   
   END IF;
   
   EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_guid = ''{'' || uuid_generate_v1() || ''}''';
   COMMIT;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_cipsrv_upload_table,'idx_guid',TRUE)
   THEN
      EXECUTE 'CREATE UNIQUE INDEX ' || str_cipsrv_upload_table || '_u99 ON cipsrv_upload.' || str_cipsrv_upload_table || '(idx_guid)';
   
   END IF;
      
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_nhdplusid')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_nhdplusid NUMERIC';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_nhdplusid = NULL';
      COMMIT;
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_reachcode')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_reachcode VARCHAR(14)';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_reachcode = NULL';
      COMMIT;
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_snap_measure')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_snap_measure NUMERIC';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_snap_measure = NULL';
      COMMIT;
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_snap_distancekm')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_snap_distancekm NUMERIC';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_snap_distancekm = NULL';
      COMMIT;
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_return_code')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_return_code INTEGER';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_return_code = NULL';
      COMMIT;
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_cipsrv_upload_table,'idx_status_message')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_cipsrv_upload_table || ' ADD COLUMN idx_status_message VARCHAR';
   
   ELSE
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' SET idx_status_message = NULL';
      COMMIT;
      
   END IF;
   
   COMMIT;
   EXECUTE 'ANALYZE cipsrv_upload.' || str_cipsrv_upload_table;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Index Each record
   ----------------------------------------------------------------------------
   int_counter := 1;
   FOR rec IN EXECUTE 'SELECT a.* FROM cipsrv_upload.' || str_cipsrv_upload_table || ' a '
   LOOP
      IF rec.shape IS NOT NULL
      AND NOT ST_ISEMPTY(rec.shape)
      THEN
         num_nhdplusid       := NULL;
         str_reachcode       := NULL;
         num_snap_measure    := NULL;
         num_snap_distancekm := NULL;
         int_return_code     := NULL;
         str_status_message  := NULL;
         geo_snap_point      := NULL;
         
         IF str_nhdplus_version = 'MR'
         THEN
            IF str_point_indexing_method = 'CATCONSTRAINED'
            THEN
               rec2 := cipsrv_nhdplus_m.catconstrained_index(
                   p_point                     := rec.shape
                  ,p_return_link_path          := FALSE
                  ,p_known_catchment_nhdplusid := NULL
                  ,p_known_region              := p_known_region
               );
               num_nhdplusid       := rec2.out_nhdplusid;
               str_reachcode       := rec2.out_reachcode;
               num_snap_measure    := rec2.out_snap_measure;
               geo_snap_point      := rec2.out_snap_point;
               num_snap_distancekm := rec2.out_snap_distancekm;
               int_return_code     := rec2.out_return_code;
               str_status_message  := rec2.out_status_message;
            
            ELSIF str_point_indexing_method = 'DISTANCE'
            THEN
               rec2 := cipsrv_nhdplus_m.distance_index(
                   p_point                  := rec.shape
                  ,p_fcode_allow            := p_fcode_allow
                  ,p_fcode_deny             := p_fcode_deny
                  ,p_distance_max_dist_km   := p_distance_max_dist_km
                  ,p_limit_innetwork        := p_limit_innetwork
                  ,p_limit_navigable        := p_limit_navigable
                  ,p_return_link_path       := FALSE
                  ,p_known_region           := p_known_region
               );
               num_nhdplusid       := rec2.out_nhdplusid;
               str_reachcode       := rec2.out_reachcode;
               num_snap_measure    := rec2.out_snap_measure;
               geo_snap_point      := rec2.out_snap_point;
               num_snap_distancekm := rec2.out_snap_distancekm;
               int_return_code     := rec2.out_return_code;
               str_status_message  := rec2.out_status_message;
            
            ELSE
               RAISE EXCEPTION 'err2';

            END IF;
            
         ELSIF str_nhdplus_version = 'HR'
         THEN
            IF str_point_indexing_method = 'CATCONSTRAINED'
            THEN
               rec2 := cipsrv_nhdplus_h.catconstrained_reach_index(
                   p_geometry               := rec.shape
                  ,p_catchment_nhdplusid    := NULL
                  ,p_known_region           := p_known_region
               );
               num_nhdplusid       := rec2.out_nhdplusid;
               str_reachcode       := rec2.out_reachcode;
               num_snap_measure    := rec2.out_snap_measure;
               geo_snap_point      := rec2.out_snap_point;
               num_snap_distancekm := rec2.out_snap_distancekm;
               int_return_code     := rec2.out_return_code;
               str_status_message  := rec2.out_status_message;
            
            ELSIF str_point_indexing_method = 'DISTANCE'
            THEN
               rec2 := cipsrv_nhdplus_h.distance_reach_index(
                   p_geometry               := rec.shape
                  ,p_fcode_allow            := p_fcode_allow
                  ,p_fcode_deny             := p_fcode_deny
                  ,p_distance_max_dist_km   := p_distance_max_dist_km
                  ,p_limit_innetwork        := p_limit_innetwork
                  ,p_limit_navigable        := p_limit_navigable
                  ,p_known_region           := p_known_region
               );
               num_nhdplusid       := rec2.out_nhdplusid;
               str_reachcode       := rec2.out_reachcode;
               num_snap_measure    := rec2.out_snap_measure;
               geo_snap_point      := rec2.out_snap_point;
               num_snap_distancekm := rec2.out_snap_distancekm;
               int_return_code     := rec2.out_return_code;
               str_status_message  := rec2.out_status_message;
           
            ELSE
               RAISE EXCEPTION 'err2';

            END IF;
         
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;
         
      END IF;
      
      EXECUTE 'UPDATE cipsrv_upload.' || str_cipsrv_upload_table || ' a '
           || 'SET '
           || ' idx_nhdplusid       = $1 '
           || ',idx_reachcode       = $2 '
           || ',idx_snap_measure    = $3 '
           || ',idx_snap_distancekm = $4 '
           || ',shape               = $5 '
           || ',idx_return_code     = $6 '
           || ',idx_status_message  = $7 '
           || 'WHERE '
           || 'a.idx_guid = $8 '
      USING
       num_nhdplusid
      ,str_reachcode
      ,num_snap_measure
      ,num_snap_distancekm
      ,geo_snap_point
      ,int_return_code
      ,str_status_message     
      ,rec.idx_guid;
      
      int_counter := int_counter + 1;
      IF int_counter > p_commit_limit
      THEN
         COMMIT;
         int_counter := 1;
         
      END IF;

   END LOOP;
   
   COMMIT;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER PROCEDURE cipsrv_engine.point_batch_index_table(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON PROCEDURE cipsrv_engine.point_batch_index_table(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,INTEGER
) TO PUBLIC;

