DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.updn_batch_search';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP PROCEDURE IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE PROCEDURE cipsrv_engine.updn_batch_search(
    IN  p_cip_updn_prefix                  VARCHAR
   ,IN  p_nhdplus_version                  VARCHAR
   ,IN  p_points_to_process_id             VARCHAR
   ,IN  p_points_to_process_nhdplusid      VARCHAR
   ,IN  p_points_to_process_measure        VARCHAR
   ,IN  p_points_to_search_id              VARCHAR
   ,IN  p_points_to_search_nhdplusid       VARCHAR
   ,IN  p_points_to_search_measure         VARCHAR
   ,IN  p_updn_search_type                 VARCHAR
   ,IN  p_updn_maximum_dist_km             NUMERIC
   ,IN  p_updn_gather_streams              BOOLEAN
   ,IN  p_updn_streams_between             VARCHAR
   ,IN  p_ignore_matching_ids              BOOLEAN
   ,OUT out_return_code                    INTEGER
   ,OUT out_status_message                 VARCHAR
   ,IN  p_known_region                     VARCHAR   DEFAULT NULL
   ,IN  p_commit_limit                     INTEGER   DEFAULT 2000
)
AS $BODY$ 
DECLARE
   rec                                RECORD;
   rec2                               RECORD;
   str_sql                            VARCHAR;
   str_nhdplus_version                VARCHAR;
   str_dataset_prefix                 VARCHAR;
   str_points_to_process_id           VARCHAR;
   str_points_to_search_id            VARCHAR;
   str_search_type                    VARCHAR;
   boo_updn_gather_streams            BOOLEAN;
   num_max_distancekm                 NUMERIC;
   str_updn_streams_between           VARCHAR;
   boo_ignore_matching_ids            BOOLEAN;
   boo_return_flowline_details        BOOLEAN;
   boo_return_flowline_geometry       BOOLEAN;
   int_grid_srid                      INTEGER;
   int_flowline_count                 INTEGER;
   int_return_code                    INTEGER;
   str_status_message                 VARCHAR;
   int_commit_limit                   INTEGER;
   int_counter                        INTEGER;
   
   c_analyze_threshold                INTEGER := 500;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_cip_updn_prefix IS NULL
   THEN
      RAISE EXCEPTION 'err';
   
   END IF;
   
   str_dataset_prefix       := LOWER(p_cip_updn_prefix);
   str_points_to_process_id := LOWER(p_points_to_process_id);
   str_points_to_search_id  := LOWER(p_points_to_search_id);
   str_search_type          := UPPER(p_updn_search_type);
   num_max_distancekm       := p_updn_maximum_dist_km;
   int_commit_limit         := p_commit_limit;
   
   IF int_commit_limit IS NULL
   OR int_commit_limit < 1
   THEN
      int_commit_limit := 2000;
      
   END IF;
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_pointstoprocess')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_pointstoprocess';
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_pointstoprocess',str_points_to_process_id)
   THEN
      RAISE EXCEPTION 'missing %.%.%','cipsrv_upload',str_dataset_prefix || '_pointstoprocess',str_points_to_process_id;
      
   END IF;
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_pointstosearch')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_pointstosearch';
      
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_pointstosearch',str_points_to_search_id)
   THEN
      RAISE EXCEPTION 'missing %.%.%','cipsrv_upload',str_dataset_prefix || '_pointstosearch',str_points_to_search_id;
      
   END IF;
   
   IF UPPER(p_nhdplus_version) IN ('MR','NHDPLUS_M')
   THEN
      str_nhdplus_version := 'nhdplus_m';
      
   ELSIF UPPER(p_nhdplus_version) IN ('HR','NHDPLUS_H')
   THEN
      str_nhdplus_version := 'nhdplus_h';
   
   ELSE
      RAISE EXCEPTION 'unknown nhdplus resolution %',p_nhdplus_version;
   
   END IF;
   
   boo_updn_gather_streams := p_updn_gather_streams;
   str_updn_streams_between := UPPER(p_updn_streams_between);
   IF boo_updn_gather_streams
   OR str_updn_streams_between IN ('PP','PPALL')
   THEN
      boo_return_flowline_details  := TRUE;
      boo_return_flowline_geometry := TRUE;
      
   ELSE
      IF str_updn_streams_between IS NULL
      THEN
         str_updn_streams_between := 'NONE';
      
      
      END IF;
      
      boo_return_flowline_details  := FALSE;
      boo_return_flowline_geometry := FALSE;

   END IF;

   boo_ignore_matching_ids := p_ignore_matching_ids;
   IF boo_ignore_matching_ids IS NULL
   THEN
      boo_ignore_matching_ids := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Index ids if needed
   ----------------------------------------------------------------------------
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstoprocess',p_points_to_process_id,TRUE)
   THEN
      EXECUTE 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_pointstoprocess_u99 ON cipsrv_upload.' || str_dataset_prefix || '_pointstoprocess(' || str_points_to_process_id || ')';
   
   END IF;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstoprocess',p_points_to_process_nhdplusid,FALSE)
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_pointstoprocess_i88 ON cipsrv_upload.' || str_dataset_prefix || '_pointstoprocess(' || p_points_to_process_nhdplusid || ')';
   
   END IF;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstoprocess',p_points_to_process_measure,FALSE)
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_pointstoprocess_i89 ON cipsrv_upload.' || str_dataset_prefix || '_pointstoprocess(' || p_points_to_process_measure || ')';
   
   END IF;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstosearch',p_points_to_search_id,TRUE)
   THEN
      EXECUTE 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_pointstosearch_u99 ON cipsrv_upload.' || str_dataset_prefix || '_pointstosearch(' || str_points_to_search_id || ')';
   
   END IF;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstosearch',p_points_to_search_nhdplusid,FALSE)
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_pointstosearch_i88 ON cipsrv_upload.' || str_dataset_prefix || '_pointstosearch(' || p_points_to_search_nhdplusid || ')';
   
   END IF;
   
   IF NOT cipsrv_engine.column_has_single_index('cipsrv_upload',str_dataset_prefix || '_pointstosearch',p_points_to_search_measure,FALSE)
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_pointstosearch_i89 ON cipsrv_upload.' || str_dataset_prefix || '_pointstosearch(' || p_points_to_search_measure || ')';
   
   END IF;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Analyze incoming tables
   ----------------------------------------------------------------------------
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_pointstoprocess';
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_pointstosearch';
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Create the results table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_results';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_results_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_results( '
           || '    objectid                      INTEGER     NOT NULL '
           || '   ,process_pointid               VARCHAR     NOT NULL '
           || '   ,process_pointnhdplusid        NUMERIC     NOT NULL '
           || '   ,process_pointmeasure          NUMERIC     NOT NULL '
           || '   ,search_pointid                VARCHAR     NOT NULL '
           || '   ,search_pointnhdplusid         NUMERIC     NOT NULL '
           || '   ,search_pointmeasure           NUMERIC     NOT NULL '
           || '   ,network_distancekm            NUMERIC '
           || '   ,network_flowtimeday           NUMERIC '
           || '   ,shape                         GEOMETRY '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_results TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_results_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_results( '
           || '    process_pointid '
           || '   ,search_pointid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_results_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_results( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_results_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Create the streams table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_streams';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_streams_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_streams( '
           || '    objectid                      INTEGER     NOT NULL '
           || '   ,process_pointid               VARCHAR(80) NOT NULL '
           || '   ,nhdplusid                     NUMERIC     NOT NULL '
           || '   ,permanent_identifier          VARCHAR(40) NOT NULL '
           || '   ,reachcode                     VARCHAR(14) '
           || '   ,fmeasure                      NUMERIC '
           || '   ,tmeasure                      NUMERIC '
           || '   ,network_distancekm            NUMERIC '
           || '   ,network_flowtimeday           NUMERIC '
           || '   ,lengthkm                      NUMERIC '
           || '   ,flowtimeday                   NUMERIC '
           || '   ,ftype                         INTEGER '
           || '   ,fcode                         INTEGER '
           || '   ,gnis_id                       VARCHAR '
           || '   ,gnis_name                     VARCHAR '
           || '   ,wbarea_permanent_identifier   VARCHAR(40) '
           || '   ,navtermination_flag           INTEGER '
           || '   ,nav_order                     INTEGER '
           || '   ,shape                         GEOMETRY '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_streams TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_streams_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_streams( '
           || '    process_pointid '
           || '   ,nhdplusid '
           || '   ,fmeasure '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_streams_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_streams( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_streams_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Create the between table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_between';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_between_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_between( '
           || '    objectid                      INTEGER     NOT NULL '
           || '   ,process_pointid               VARCHAR(80) NOT NULL '
           || '   ,search_pointid                VARCHAR(80) NOT NULL '
           || '   ,nhdplusid                     NUMERIC     NOT NULL '
           || '   ,permanent_identifier          VARCHAR(40) NOT NULL '
           || '   ,reachcode                     VARCHAR(14) '
           || '   ,fmeasure                      NUMERIC '
           || '   ,tmeasure                      NUMERIC '
           || '   ,network_distancekm            NUMERIC '
           || '   ,network_flowtimeday           NUMERIC '
           || '   ,lengthkm                      NUMERIC '
           || '   ,flowtimeday                   NUMERIC '
           || '   ,ftype                         INTEGER '
           || '   ,fcode                         INTEGER '
           || '   ,gnis_id                       VARCHAR '
           || '   ,gnis_name                     VARCHAR '
           || '   ,wbarea_permanent_identifier   VARCHAR(40) '
           || '   ,navtermination_flag           INTEGER '
           || '   ,nav_order                     INTEGER '
           || '   ,shape                         GEOMETRY '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_between TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_between_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_between( '
           || '    process_pointid '
           || '   ,search_pointid '
           || '   ,nhdplusid '
           || '   ,fmeasure '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_between_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_between( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_between_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Roll through the points to process
   ----------------------------------------------------------------------------
   int_counter := 1;
   str_sql := 'SELECT '
   || ' a.' || p_points_to_process_id        || ' AS pointstoprocessid '
   || ',a.' || p_points_to_process_nhdplusid || ' AS pointstoprocessnhdplusid '
   || ',a.' || p_points_to_process_measure   || ' AS pointstoprocessmeasure '
   || 'FROM '
   || 'cipsrv_upload.' || str_dataset_prefix || '_pointstoprocess a ';
   
   FOR rec IN EXECUTE str_sql
   LOOP
raise warning '% %',boo_return_flowline_details,boo_return_flowline_geometry;
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      IF str_nhdplus_version = 'nhdplus_m'
      THEN
         rec2 := cipsrv_nhdplus_m.navigate(
             p_search_type                := str_search_type
            ,p_start_nhdplusid            := rec.pointstoprocessnhdplusid::BIGINT
            ,p_start_permanent_identifier := NULL
            ,p_start_reachcode            := NULL
            ,p_start_hydroseq             := NULL
            ,p_start_measure              := rec.pointstoprocessmeasure
            ,p_stop_nhdplusid             := NULL
            ,p_stop_permanent_identifier  := NULL
            ,p_stop_reachcode             := NULL
            ,p_stop_hydroseq              := NULL
            ,p_stop_measure               := NULL
            ,p_max_distancekm             := num_max_distancekm
            ,p_max_flowtimeday            := NULL
            ,p_return_flowline_details    := boo_return_flowline_details
            ,p_return_flowline_geometry   := boo_return_flowline_geometry
         );
         int_grid_srid       := rec2.out_grid_srid;
         int_flowline_count  := rec2.out_flowline_count;
         int_return_code     := rec2.out_return_code;
         str_status_message  := rec2.out_status_message;
         
      ELSIF str_nhdplus_version = 'nhdplus_h'
      THEN
         rec2 := cipsrv_nhdplus_h.navigate(
             p_search_type                := str_search_type
            ,p_start_nhdplusid            := rec.pointstoprocessnhdplusid::BIGINT
            ,p_start_permanent_identifier := NULL
            ,p_start_reachcode            := NULL
            ,p_start_hydroseq             := NULL
            ,p_start_measure              := rec.pointstoprocessmeasure
            ,p_stop_nhdplusid             := NULL
            ,p_stop_permanent_identifier  := NULL
            ,p_stop_reachcode             := NULL
            ,p_stop_hydroseq              := NULL
            ,p_stop_measure               := NULL
            ,p_max_distancekm             := num_max_distancekm
            ,p_max_flowtimeday            := NULL
            ,p_return_flowline_details    := boo_return_flowline_details
            ,p_return_flowline_geometry   := boo_return_flowline_geometry
         );
         int_grid_srid       := rec2.out_grid_srid;
         int_flowline_count  := rec2.out_flowline_count;
         int_return_code     := rec2.out_return_code;
         str_status_message  := rec2.out_status_message;
      
      ELSE
         RAISE EXCEPTION 'err %',str_nhdplus_version;
         
      END IF;
      
      IF int_return_code = 0
      THEN
         IF int_flowline_count > c_analyze_threshold
         THEN
            EXECUTE 'ANALYZE tmp_navigation_results';
      
         END IF;
      
         --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
         EXECUTE 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_results( '
         || '    objectid '
         || '   ,process_pointid '
         || '   ,process_pointnhdplusid '
         || '   ,process_pointmeasure '
         || '   ,search_pointid '
         || '   ,search_pointnhdplusid '
         || '   ,search_pointmeasure '
         || '   ,network_distancekm '
         || '   ,network_flowtimeday '
         || '   ,shape '
         || ') '
         || 'SELECT '
         || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_results_seq'') '
         || ',$1 '
         || ',$2 '
         || ',$3 '
         || ',b.' || p_points_to_search_id        || ' '
         || ',b.' || p_points_to_search_nhdplusid || ' '
         || ',b.' || p_points_to_search_measure   || ' '
         || ',cipsrv_engine.adjust_point_extent( '
         || '   p_extent_value      := a.network_distancekm '
         || '  ,p_direction         := $4 '
         || '  ,p_flowline_amount   := a.lengthkm '
         || '  ,p_flowline_fmeasure := a.fmeasure '
         || '  ,p_flowline_tmeasure := a.tmeasure '
         || '  ,p_event_measure     := b.' || p_points_to_search_measure   || ' '
         || ' ) AS network_distancekm '
         || ',cipsrv_engine.adjust_point_extent( '
         || '   p_extent_value      => a.network_flowtimeday '
         || '  ,p_direction         => $5 '
         || '  ,p_flowline_amount   => a.flowtimeday '
         || '  ,p_flowline_fmeasure => a.fmeasure '
         || '  ,p_flowline_tmeasure => a.tmeasure '
         || '  ,p_event_measure     => b.' || p_points_to_search_measure   || ' '
         || ' ) AS network_flowtimeday '
         || ',b.shape '
         || 'FROM '
         || 'tmp_navigation_results a '
         || 'JOIN '
         || 'cipsrv_upload.' || str_dataset_prefix || '_pointstosearch b '
         || 'ON '
         || '    b.' || p_points_to_search_nhdplusid || ' =  a.nhdplusid '
         || 'AND b.' || p_points_to_search_measure   || ' >= a.fmeasure '
         || 'AND b.' || p_points_to_search_measure   || ' <= a.tmeasure '
         || 'WHERE '
         || '$6 IS FALSE OR b.' || p_points_to_search_id || ' != $7 '
         USING
          rec.pointstoprocessid
         ,rec.pointstoprocessnhdplusid
         ,rec.pointstoprocessmeasure
         ,str_search_type
         ,str_search_type
         ,boo_ignore_matching_ids
         ,rec.pointstoprocessid;

         IF boo_updn_gather_streams
         THEN
            EXECUTE 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_streams( '
            || '    objectid '
            || '   ,process_pointid '
            || '   ,nhdplusid '
            || '   ,permanent_identifier '
            || '   ,reachcode '
            || '   ,fmeasure '
            || '   ,tmeasure '
            || '   ,network_distancekm '
            || '   ,network_flowtimeday '
            || '   ,lengthkm '
            || '   ,flowtimeday '
            || '   ,ftype '
            || '   ,fcode '
            || '   ,gnis_id '
            || '   ,gnis_name '
            || '   ,wbarea_permanent_identifier '
            || '   ,navtermination_flag '
            || '   ,nav_order '
            || '   ,shape '
            || ') '
            || 'SELECT '
            || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_streams_seq'') '
            || ',$1 '
            || ',a.nhdplusid '
            || ',a.permanent_identifier '
            || ',a.reachcode '
            || ',a.fmeasure '
            || ',a.tmeasure '
            || ',a.network_distancekm '
            || ',a.network_flowtimeday '
            || ',a.lengthkm '
            || ',a.flowtimeday '
            || ',SUBSTR(a.fcode::VARCHAR,1,3)::INTEGER '
            || ',a.fcode '
            || ',a.gnis_id '
            || ',a.gnis_name '
            || ',a.wbarea_permanent_identifier '
            || ',a.navtermination_flag '
            || ',a.nav_order '
            || ',a.shape '
            || 'FROM '
            || 'tmp_navigation_results a '
            USING
            rec.pointstoprocessid;
           
         END IF;
      
      END IF;
      
      --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      int_counter := int_counter + 1;
      IF int_counter > int_commit_limit
      THEN
         COMMIT;
         int_counter := 1;
         
      END IF;
   
   END LOOP;
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Roll through the results for further streams generation
   ----------------------------------------------------------------------------
   IF str_updn_streams_between != 'NONE'
   THEN
      str_sql := 'SELECT '
      || ' a.objectid '
      || ',a.process_pointid '
      || ',a.process_pointnhdplusid '
      || ',a.process_pointmeasure '
      || ',a.search_pointid '
      || ',a.search_pointnhdplusid '
      || ',a.search_pointmeasure '
      || ',a.network_distancekm '
      || ',a.network_flowtimeday '
      || 'FROM '
      || 'cipsrv_upload.' || str_dataset_prefix || '_results a '
      || 'WHERE '
      || '    a.network_distancekm IS NOT NULL '
      || 'AND a.network_distancekm > 0 ';
   
      FOR rec IN EXECUTE str_sql
      LOOP
      
         --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
         IF str_nhdplus_version = 'nhdplus_m'
         THEN
            rec2 := cipsrv_nhdplus_m.navigate(
                p_search_type                := str_updn_streams_between
               ,p_start_nhdplusid            := rec.process_pointnhdplusid::BIGINT
               ,p_start_permanent_identifier := NULL
               ,p_start_reachcode            := NULL
               ,p_start_hydroseq             := NULL
               ,p_start_measure              := rec.process_pointmeasure
               ,p_stop_nhdplusid             := rec.search_pointnhdplusid::BIGINT
               ,p_stop_permanent_identifier  := NULL
               ,p_stop_reachcode             := NULL
               ,p_stop_hydroseq              := NULL
               ,p_stop_measure               := rec.search_pointmeasure
               ,p_max_distancekm             := NULL
               ,p_max_flowtimeday            := NULL
               ,p_return_flowline_details    := boo_return_flowline_details
               ,p_return_flowline_geometry   := boo_return_flowline_geometry
            );
            int_grid_srid       := rec2.out_grid_srid;
            int_flowline_count  := rec2.out_flowline_count;
            int_return_code     := rec2.out_return_code;
            str_status_message  := rec2.out_status_message;
            
         ELSIF str_nhdplus_version = 'nhdplus_h'
         THEN
            rec2 := cipsrv_nhdplus_h.navigate(
                p_search_type                := str_updn_streams_between
               ,p_start_nhdplusid            := rec.process_pointnhdplusid::BIGINT
               ,p_start_permanent_identifier := NULL
               ,p_start_reachcode            := NULL
               ,p_start_hydroseq             := NULL
               ,p_start_measure              := rec.process_pointmeasure
               ,p_stop_nhdplusid             := rec.search_pointnhdplusid::BIGINT
               ,p_stop_permanent_identifier  := NULL
               ,p_stop_reachcode             := NULL
               ,p_stop_hydroseq              := NULL
               ,p_stop_measure               := rec.search_pointmeasure
               ,p_max_distancekm             := NULL
               ,p_max_flowtimeday            := NULL
               ,p_return_flowline_details    := boo_return_flowline_details
               ,p_return_flowline_geometry   := boo_return_flowline_geometry
            );
            int_grid_srid       := rec2.out_grid_srid;
            int_flowline_count  := rec2.out_flowline_count;
            int_return_code     := rec2.out_return_code;
            str_status_message  := rec2.out_status_message;
         
         ELSE
            RAISE EXCEPTION 'err %',str_nhdplus_version;
            
         END IF;
      
         IF int_return_code = 0
         THEN
            IF int_flowline_count > c_analyze_threshold
            THEN
               EXECUTE 'ANALYZE tmp_navigation_results';
         
            END IF;
            
            str_sql := 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_between( '
            || '    objectid '
            || '   ,process_pointid '
            || '   ,search_pointid '
            || '   ,nhdplusid '
            || '   ,permanent_identifier '
            || '   ,reachcode '
            || '   ,fmeasure '
            || '   ,tmeasure '
            || '   ,network_distancekm '
            || '   ,network_flowtimeday '
            || '   ,lengthkm '
            || '   ,flowtimeday '
            || '   ,ftype '
            || '   ,fcode '
            || '   ,gnis_id '
            || '   ,gnis_name '
            || '   ,wbarea_permanent_identifier '
            || '   ,navtermination_flag '
            || '   ,nav_order '
            || '   ,shape '
            || ') '
            || 'SELECT '
            || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_between_seq'') '
            || ',$1 '
            || ',$2 '
            || ',a.nhdplusid '
            || ',a.permanent_identifier '
            || ',a.reachcode '
            || ',a.fmeasure '
            || ',a.tmeasure '
            || ',a.network_distancekm '
            || ',a.network_flowtimeday '
            || ',a.lengthkm '
            || ',a.flowtimeday '
            || ',SUBSTR(a.fcode::VARCHAR,1,3)::INTEGER '
            || ',a.fcode '
            || ',a.gnis_id '
            || ',a.gnis_name '
            || ',a.wbarea_permanent_identifier '
            || ',a.navtermination_flag '
            || ',a.nav_order '
            || ',a.shape '
            || 'FROM '
            || 'tmp_navigation_results a ';
            
            EXECUTE str_sql 
            USING
             rec.process_pointid
            ,rec.search_pointid;
            
         END IF;
      
      END LOOP;
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER PROCEDURE cipsrv_engine.updn_batch_search(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,BOOLEAN
   ,VARCHAR
   ,BOOLEAN
   ,VARCHAR
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON PROCEDURE cipsrv_engine.updn_batch_search(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,BOOLEAN
   ,VARCHAR
   ,BOOLEAN
   ,VARCHAR
   ,INTEGER
) TO PUBLIC;

