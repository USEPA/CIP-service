DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.cipsrv_batch_index';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP PROCEDURE IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE PROCEDURE cipsrv_engine.cipsrv_batch_index(
    IN  p_dataset_prefix                   VARCHAR
   ,OUT out_return_code                    INTEGER
   ,OUT out_status_message                 VARCHAR
   ,IN  p_override_default_nhdplus_version VARCHAR DEFAULT NULL
)
AS $BODY$ 
DECLARE
   rec                                RECORD;
   rec2                               RECORD;
   rec3                               RECORD;
   str_sql                            VARCHAR;
   str_dataset_prefix                 VARCHAR;
   str_control_dataset_prefix         VARCHAR;
   str_control_source_originator      VARCHAR;
   str_control_source_series          VARCHAR;
   str_geometry_clip                  VARCHAR;
   ary_geometry_clip                  VARCHAR[];
   str_geometry_clip_stage            VARCHAR;
   str_catchment_filter               VARCHAR;
   ary_catchment_filter               VARCHAR[];
   str_nhdplus_version                VARCHAR;
   str_default_nhdplus_version        VARCHAR;
   str_catchment_resolution           VARCHAR;
   str_xwalk_huc12_version            VARCHAR;
   
   str_point_indexing_method          VARCHAR;
   str_default_point_indexing_method  VARCHAR;
   
   str_line_indexing_method           VARCHAR;
   str_default_line_indexing_method   VARCHAR;
   num_line_threshold                 NUMERIC;
   num_default_line_threshold         NUMERIC;
   
   str_ring_indexing_method           VARCHAR;
   str_default_ring_indexing_method   VARCHAR;
   num_ring_areacat_threshold         NUMERIC;
   num_default_ring_areacat_threshold NUMERIC;
   num_ring_areaevt_threshold         NUMERIC;
   num_default_ring_areaevt_threshold NUMERIC;
   
   str_area_indexing_method           VARCHAR;
   str_default_area_indexing_method   VARCHAR;
   num_areacat_threshold              NUMERIC;
   num_default_areacat_threshold      NUMERIC;
   num_areaevt_threshold              NUMERIC;
   num_default_areaevt_threshold      NUMERIC;
   
   str_known_region                   VARCHAR;
   str_default_known_region           VARCHAR;
   str_username                       VARCHAR;
   dat_datecreated                    DATE;
   boo_filter_by_state                BOOLEAN;
   ary_state_filters                  VARCHAR[];
   boo_filter_by_tribal               BOOLEAN;
   boo_filter_by_notribal             BOOLEAN;
   int_count                          INTEGER;
   boo_isring                         BOOLEAN;
   num_point_indexing_return_code     INTEGER;
   str_point_indexing_status_message  VARCHAR;
   num_line_indexing_return_code      INTEGER;
   str_line_indexing_status_message   VARCHAR;
   num_ring_indexing_return_code      INTEGER;
   str_ring_indexing_status_message   VARCHAR;
   num_area_indexing_return_code      INTEGER;
   str_area_indexing_status_message   VARCHAR;
   geom_part                          GEOMETRY;
   num_line_lengthkm                  NUMERIC;
   num_area_areasqkm                  NUMERIC;
   int_cat_mr_count                   INTEGER;
   int_cat_hr_count                   INTEGER;
   
BEGIN

   out_return_code := cipsrv_engine.create_cip_batch_temp_tables();
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_dataset_prefix IS NULL
   THEN
      RAISE EXCEPTION 'err';
   
   END IF;
   
   str_dataset_prefix := LOWER(p_dataset_prefix);
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_control')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_control';
      
   END IF;
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_points')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_points';
      
   END IF;
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_lines')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_lines';
      
   END IF;
   
   IF NOT cipsrv_engine.table_exists('cipsrv_upload',str_dataset_prefix || '_areas')
   THEN
      RAISE EXCEPTION 'missing %.%','cipsrv_upload',str_dataset_prefix || '_areas';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Index source_featureid if needed
   ----------------------------------------------------------------------------
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_points',str_dataset_prefix || '_points_sfid')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_points_sfid ON cipsrv_upload.' || str_dataset_prefix || '_points(source_featureid)';
   
   END IF;
   
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_lines',str_dataset_prefix || '_lines_sfid')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_lines_sfid ON cipsrv_upload.' || str_dataset_prefix || '_lines(source_featureid)';
   
   END IF;
   
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_areas',str_dataset_prefix || '_areas_sfid')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_areas_sfid ON cipsrv_upload.' || str_dataset_prefix || '_areas(source_featureid)';
   
   END IF;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Add source_joinkey field if needed and index if needed
   ----------------------------------------------------------------------------
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_points','source_joinkey')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_dataset_prefix || '_points ADD COLUMN source_joinkey VARCHAR(40)';
   
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_lines','source_joinkey')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_dataset_prefix || '_lines ADD COLUMN source_joinkey VARCHAR(40)';
   
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_areas','source_joinkey')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_dataset_prefix || '_areas ADD COLUMN source_joinkey VARCHAR(40)';
   
   END IF;
   
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_points',str_dataset_prefix || '_points_sjk')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_points_sjk ON cipsrv_upload.' || str_dataset_prefix || '_points(source_joinkey)';
   
   END IF;
   
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_lines',str_dataset_prefix || '_lines_sjk')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_lines_sjk ON cipsrv_upload.' || str_dataset_prefix || '_lines(source_joinkey)';
   
   END IF;
   
   IF NOT cipsrv_engine.index_exists('cipsrv_upload',str_dataset_prefix || '_areas',str_dataset_prefix || '_areas_sjk')
   THEN
      EXECUTE 'CREATE INDEX ' || str_dataset_prefix || '_areas_sjk ON cipsrv_upload.' || str_dataset_prefix || '_areas(source_joinkey)';
   
   END IF;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Add measurements field if needed
   ----------------------------------------------------------------------------
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_lines','lengthkm')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_dataset_prefix || '_lines ADD COLUMN lengthkm NUMERIC';
   
   END IF;
   
   IF NOT cipsrv_engine.field_exists('cipsrv_upload',str_dataset_prefix || '_areas','areasqkm')
   THEN
      EXECUTE 'ALTER TABLE cipsrv_upload.' || str_dataset_prefix || '_areas ADD COLUMN areasqkm NUMERIC';
   
   END IF;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Analyze incoming tables
   ----------------------------------------------------------------------------
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_points';
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_lines';
   EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_areas';
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Create the sfid table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_sfid';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_sfid_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    objectid                      INTEGER     NOT NULL '
           || '   ,source_originator             VARCHAR(130) '
           || '   ,source_featureid              VARCHAR(100) '
           || '   ,source_featureid2             VARCHAR(100) '
           || '   ,source_series                 VARCHAR(100) '
           || '   ,source_subdivision            VARCHAR(100) '
           || '   ,source_joinkey                VARCHAR(40) NOT NULL '
           || '   ,start_date                    DATE '
           || '   ,end_date                      DATE '
           || '   ,src_point_count               INTEGER '
           || '   ,point_indexing_method_used    VARCHAR(40) '
           || '   ,point_indexing_return_code    INTEGER '
           || '   ,point_indexing_status_message VARCHAR(40) '
           || '   ,src_line_count                INTEGER '
           || '   ,line_indexing_method_used     VARCHAR(40) '
           || '   ,line_indexing_return_code     INTEGER '
           || '   ,line_indexing_status_message  VARCHAR(40) '
           || '   ,ring_indexing_method_used     VARCHAR(40) '
           || '   ,ring_indexing_return_code     INTEGER '
           || '   ,ring_indexing_status_message  VARCHAR(40) '
           || '   ,src_area_count                INTEGER '
           || '   ,area_indexing_method_used     VARCHAR(40) '
           || '   ,area_indexing_return_code     INTEGER '
           || '   ,area_indexing_status_message  VARCHAR(40) '
           || '   ,cat_mr_count                  INTEGER '
           || '   ,cat_hr_count                  INTEGER '
           || '   ,globalid                      VARCHAR(40) NOT NULL '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_sfid TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_sfid_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    source_featureid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_sfid_pk2 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    source_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_sfid_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_sfid_u02 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    globalid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_sfid_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Load the sfid table
   ----------------------------------------------------------------------------
   str_sql := 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_sfid( '
           || '    objectid '
           || '   ,source_originator '
           || '   ,source_series '
           || '   ,source_featureid '
           || '   ,source_joinkey '
           || '   ,src_point_count '
           || '   ,src_line_count '
           || '   ,src_area_count '
           || '   ,globalid '
           || ') '
           || 'SELECT '
           || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_sfid_seq'') '
           || ',a.source_originator '
           || ',a.source_series '
           || ',a.source_featureid '
           || ',''{'' || uuid_generate_v1() || ''}'' '
           || ',a.src_point_count '
           || ',a.src_line_count '
           || ',a.src_area_count '
           || ',''{'' || uuid_generate_v1() || ''}'' '
           || 'FROM ( '
           || '   SELECT '
           || '    aa.source_originator '
           || '   ,aa.source_series '
           || '   ,aa.source_featureid '
           || '   ,bb.src_point_count '
           || '   ,cc.src_line_count '
           || '   ,dd.src_area_count '
           || '   FROM ( '
           || '      SELECT '
           || '       aa1.source_originator '
           || '      ,aa1.source_series '
           || '      ,aa1.source_featureid '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_points aa1 '
           || '      UNION SELECT '
           || '       aa2.source_originator '
           || '      ,aa2.source_series '
           || '      ,aa2.source_featureid '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_lines aa2 '
           || '      UNION SELECT '
           || '       aa3.source_originator '
           || '      ,aa3.source_series '
           || '      ,aa3.source_featureid '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_areas aa3 '
           || '      GROUP BY 1,2,3 '
           || '   ) aa '
           || '   LEFT JOIN ( '
           || '      SELECT '
           || '       bbb.source_originator '
           || '      ,bbb.source_series '
           || '      ,bbb.source_featureid '
           || '      ,COUNT(*) AS src_point_count '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_points bbb '
           || '      GROUP BY '
           || '       bbb.source_originator '
           || '      ,bbb.source_series '
           || '      ,bbb.source_featureid '
           || '   ) bb '
           || '   ON '
           || '       aa.source_originator = bb.source_originator '
           || '   AND aa.source_series     = bb.source_series '
           || '   AND aa.source_featureid  = bb.source_featureid '
           || '   LEFT JOIN ( '
           || '      SELECT '
           || '       ccc.source_originator '
           || '      ,ccc.source_series '
           || '      ,ccc.source_featureid '
           || '      ,COUNT(*) AS src_line_count '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_lines ccc '
           || '      GROUP BY '
           || '       ccc.source_originator '
           || '      ,ccc.source_series '
           || '      ,ccc.source_featureid '
           || '   ) cc '
           || '   ON '
           || '       aa.source_originator = cc.source_originator '
           || '   AND aa.source_series     = cc.source_series '
           || '   AND aa.source_featureid  = cc.source_featureid '
           || '   LEFT JOIN ( '
           || '      SELECT '
           || '       ddd.source_originator '
           || '      ,ddd.source_series '
           || '      ,ddd.source_featureid '
           || '      ,COUNT(*) AS src_area_count '
           || '      FROM '
           || '      cipsrv_upload.' || str_dataset_prefix || '_areas ddd '
           || '      GROUP BY '
           || '       ddd.source_originator '
           || '      ,ddd.source_series '
           || '      ,ddd.source_featureid '
           || '   ) dd '
           || '   ON '
           || '       aa.source_originator = dd.source_originator '
           || '   AND aa.source_series     = dd.source_series '
           || '   AND aa.source_featureid  = dd.source_featureid '
           || ') a ';
           
   EXECUTE str_sql;
   
   COMMIT;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Update the feature tables with new source_joinkeys
   ----------------------------------------------------------------------------
   str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_points a '
           || 'SET source_joinkey = ( '
           || '   SELECT '
           || '   b.source_joinkey '
           || '   FROM '
           || '   cipsrv_upload.' || str_dataset_prefix || '_sfid b '
           || '   WHERE '
           || '   b.source_featureid = a.source_featureid '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_lines a '
           || 'SET source_joinkey = ( '
           || '   SELECT '
           || '   c.source_joinkey '
           || '   FROM '
           || '   cipsrv_upload.' || str_dataset_prefix || '_sfid c '
           || '   WHERE '
           || '   c.source_featureid = a.source_featureid '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_areas a '
           || 'SET source_joinkey = ( '
           || '   SELECT '
           || '   d.source_joinkey '
           || '   FROM '
           || '   cipsrv_upload.' || str_dataset_prefix || '_sfid d '
           || '   WHERE '
           || '   d.source_featureid = a.source_featureid '
           || ') ';
           
   EXECUTE str_sql;
           
   ----------------------------------------------------------------------------
   -- Step 90
   -- Create the cip results table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_cip';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_cip_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    objectid              INTEGER     NOT NULL '
           || '   ,source_originator     VARCHAR(130) '
           || '   ,source_featureid      VARCHAR(100) '
           || '   ,source_featureid2     VARCHAR(100) '
           || '   ,source_series         VARCHAR(100) '
           || '   ,source_subdivision    VARCHAR(100) '
           || '   ,source_joinkey        VARCHAR(40) '
           || '   ,start_date            DATE '
           || '   ,end_date              DATE '
           || '   ,cat_joinkey           VARCHAR(40) NOT NULL '
           || '   ,catchmentstatecode    VARCHAR(2)  NOT NULL '
           || '   ,nhdplusid             NUMERIC     NOT NULL '
           || '   ,istribal              VARCHAR(1)  NOT NULL '
           || '   ,catchmentresolution   VARCHAR(2)  NOT NULL '
           || '   ,catchmentareasqkm     NUMERIC     NOT NULL '
           || '   ,xwalk_huc12           VARCHAR(12) '
           || '   ,xwalk_method          VARCHAR(18) '
           || '   ,xwalk_huc12_version   VARCHAR(16) '
           || '   ,isnavigable           VARCHAR(1)  NOT NULL '
           || '   ,hasvaa                VARCHAR(1)  NOT NULL '
           || '   ,issink                VARCHAR(1)  NOT NULL '
           || '   ,isheadwater           VARCHAR(1)  NOT NULL '
           || '   ,iscoastal             VARCHAR(1)  NOT NULL '
           || '   ,isocean               VARCHAR(1)  NOT NULL '
           || '   ,isalaskan             VARCHAR(1) '
           || '   ,h3hexagonaddr         VARCHAR(64) '
           || '   ,globalid              VARCHAR(40) NOT NULL '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_cip TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_cip_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    source_joinkey ' 
           || '   ,cat_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_cip_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_cip_u02 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    globalid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_cip_u03 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    source_joinkey ' 
           || '   ,catchmentstatecode '
           || '   ,nhdplusid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE INDEX ' || str_dataset_prefix || '_cip_i01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    source_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE INDEX ' || str_dataset_prefix || '_cip_i02 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_cip( '
           || '    catchmentstatecode '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_cip_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 100
   -- Create the src2cip results table
   ----------------------------------------------------------------------------
   EXECUTE 'DROP TABLE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_src2cip';
      
   EXECUTE 'DROP SEQUENCE IF EXISTS cipsrv_upload.' || str_dataset_prefix || '_src2cip_seq';
   
   str_sql := 'CREATE TABLE cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    objectid              INTEGER     NOT NULL '
           || '   ,source_joinkey        VARCHAR(40) NOT NULL '
           || '   ,permid_joinkey        VARCHAR(40) NOT NULL '
           || '   ,cat_joinkey           VARCHAR(40) NOT NULL '
           || '   ,catchmentstatecode    VARCHAR(2)  NOT NULL '
           || '   ,nhdplusid             NUMERIC     NOT NULL '
           || '   ,overlap_measure       NUMERIC '
           || '   ,cip_method            VARCHAR(255) '
           || '   ,cip_parms             VARCHAR(255) '
           || '   ,cip_date              DATE '
           || '   ,cip_version           VARCHAR(255) '
           || '   ,globalid              VARCHAR(40) NOT NULL '
           || ') ';
           
   EXECUTE str_sql;
   
   str_sql := 'GRANT SELECT ON TABLE cipsrv_upload.' || str_dataset_prefix || '_src2cip TO PUBLIC';
   
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_src2cip_pk '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    source_joinkey '
           || '   ,permid_joinkey '           
           || '   ,cat_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_src2cip_u01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    objectid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE UNIQUE INDEX ' || str_dataset_prefix || '_src2cip_u02 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    globalid '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE INDEX ' || str_dataset_prefix || '_src2cip_i01 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    source_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE INDEX ' || str_dataset_prefix || '_src2cip_i02 '
           || 'ON cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
           || '    permid_joinkey '
           || ') ';
           
   EXECUTE str_sql;

   str_sql := 'CREATE SEQUENCE cipsrv_upload.' || str_dataset_prefix || '_src2cip_seq START WITH 1';
           
   EXECUTE str_sql;
   
   COMMIT;

   ----------------------------------------------------------------------------
   -- Step 110
   -- Read the control table
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || ' a.source_originator '
           || ',a.source_series '
           || ',a.dataset_prefix '
           || ',a.geometry_clip '
           || ',a.geometry_clip_stage '
           || ',a.catchment_filter '
           || ',a.nhdplus_version '
           || ',a.xwalk_huc12_version '
           
           || ',a.default_point_indexing_method '
           
           || ',a.default_line_indexing_method '
           || ',a.default_line_threshold '
           
           || ',a.default_ring_indexing_method '
           || ',a.default_ring_areacat_threshold '
           || ',a.default_ring_areaevt_threshold '
           
           || ',a.default_area_indexing_method '
           || ',a.default_areacat_threshold '
           || ',a.default_areaevt_threshold '
           
           || ',a.known_region '
           || ',a.username '
           || ',a.datecreated '
           || 'FROM '
           || 'cipsrv_upload.' || str_dataset_prefix || '_control a '
           || 'LIMIT 1';
           
   EXECUTE str_sql INTO
    str_control_source_originator
   ,str_control_source_series
   ,str_control_dataset_prefix
   ,str_geometry_clip
   ,str_geometry_clip_stage
   ,str_catchment_filter
   ,str_default_nhdplus_version
   ,str_xwalk_huc12_version
   
   ,str_default_point_indexing_method
   
   ,str_default_line_indexing_method
   ,num_default_line_threshold
   
   ,str_default_ring_indexing_method
   ,num_default_ring_areacat_threshold
   ,num_default_ring_areaevt_threshold
   
   ,str_default_area_indexing_method
   ,num_default_areacat_threshold
   ,num_default_areaevt_threshold
   
   ,str_default_known_region
   ,str_username
   ,dat_datecreated;
   
   IF p_override_default_nhdplus_version IS NOT NULL
   THEN
      str_default_nhdplus_version := p_override_default_nhdplus_version;
   
   END IF;
   
   IF str_dataset_prefix != LOWER(str_control_dataset_prefix)
   THEN
      RAISE WARNING 'mismatch between parameter and control dataset prefixes: % <> %',str_dataset_prefix,LOWER(str_control_dataset_prefix);
      
   END IF;
   
   ary_catchment_filter := string_to_array(str_catchment_filter,',');
   rec := cipsrv_engine.parse_catchment_filter(
      p_catchment_filter := ary_catchment_filter
   );
   boo_filter_by_state    := rec.out_filter_by_state;
   ary_state_filters      := rec.out_state_filters;
   boo_filter_by_tribal   := rec.out_filter_by_tribal;
   boo_filter_by_notribal := rec.out_filter_by_notribal;
   
   ary_geometry_clip := string_to_array(str_geometry_clip,',');
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 120
   -- Update the feature tables with measures
   ----------------------------------------------------------------------------
   str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_lines a '
           || 'SET lengthkm = cipsrv_engine.measure_lengthkm( '
           || '    a.shape '
           || '   ,a.nhdplus_version '
           || '   ,$1 '
           || '   ,a.known_region '
           || '   ,$2 '
           || ') ';
           
   EXECUTE str_sql 
   USING str_default_nhdplus_version,str_default_known_region;
   
   str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_areas a '
           || 'SET areasqkm = cipsrv_engine.measure_areasqkm( '
           || '    a.shape '
           || '   ,a.nhdplus_version '
           || '   ,$1 '
           || '   ,a.known_region '
           || '   ,$2 '
           || ') ';
           
   EXECUTE str_sql 
   USING str_default_nhdplus_version,str_default_known_region;
   
   COMMIT;
   
   ----------------------------------------------------------------------------
   -- Step 130
   -- Clip features if requested
   ----------------------------------------------------------------------------
   IF str_geometry_clip_stage = 'BEFORE'
   AND str_geometry_clip IS NOT NULL
   AND array_length(ary_geometry_clip,1) > 0
   THEN
      rec := cipsrv_engine.feature_batch_clip(
          p_dataset_prefix        := str_dataset_prefix
         ,p_clippers              := ary_geometry_clip
         ,p_known_region          := str_default_known_region
      );
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      COMMIT;

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 140
   -- Step through each sfid
   ----------------------------------------------------------------------------
   FOR rec IN EXECUTE 'SELECT a.* FROM cipsrv_upload.' || str_dataset_prefix || '_sfid a '
   LOOP
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      str_nhdplus_version               := NULL;
            
      num_point_indexing_return_code    := NULL;
      str_point_indexing_status_message := NULL;
      
      str_line_indexing_method          := NULL;
      num_line_indexing_return_code     := NULL;
      str_line_indexing_status_message  := NULL;
      num_line_threshold                := NULL;
            
      str_ring_indexing_method          := NULL;
      num_ring_indexing_return_code     := NULL;
      str_ring_indexing_status_message  := NULL;
      num_ring_areacat_threshold        := NULL;
      num_ring_areaevt_threshold        := NULL;
      
      str_area_indexing_method          := NULL;
      num_areacat_threshold             := NULL;
      num_areaevt_threshold             := NULL;
      num_area_indexing_return_code     := NULL;
      str_area_indexing_status_message  := NULL;
      
      str_known_region                  := NULL;

      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      IF rec.src_point_count > 0
      THEN
         FOR rec2 IN EXECUTE 'SELECT b.* FROM cipsrv_upload.' || str_dataset_prefix || '_points b WHERE b.source_joinkey = $1' USING rec.source_joinkey
         LOOP
            --###############################################################--
            str_nhdplus_version := NULL;
            IF rec2.nhdplus_version IS NOT NULL
            THEN
               str_nhdplus_version := rec2.nhdplus_version;
               
            ELSE
               IF str_default_nhdplus_version IS NOT NULL
               THEN
                  str_nhdplus_version := str_default_nhdplus_version;
                  
               END IF;
               
            END IF;

            --###############################################################--
            str_known_region := NULL;
            IF rec2.known_region IS NOT NULL
            THEN
               str_known_region := rec2.known_region;
               
            ELSE
               IF str_default_known_region IS NOT NULL
               THEN
                  str_known_region := str_default_known_region;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            str_point_indexing_method := NULL;
            IF rec2.point_indexing_method IS NOT NULL
            THEN
               str_point_indexing_method := rec2.point_indexing_method;
               
            ELSE
               IF str_default_point_indexing_method IS NOT NULL
               THEN
                  str_point_indexing_method := str_default_point_indexing_method;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            IF str_point_indexing_method IS NULL
            OR str_point_indexing_method = 'point_simple'
            THEN
               IF str_nhdplus_version = 'nhdplus_m'
               THEN
                  rec3 := cipsrv_nhdplus_m.index_point_simple(
                      p_geometry             := rec2.shape
                     ,p_known_region         := str_known_region
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_point_indexing_return_code    := rec3.out_return_code;
                  str_point_indexing_status_message := rec3.out_status_message;
                  
               ELSIF str_nhdplus_version = 'nhdplus_h'
               THEN
                  rec3 := cipsrv_nhdplus_h.index_point_simple(
                      p_geometry             := rec2.shape
                     ,p_known_region         := str_known_region
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_point_indexing_return_code    := rec3.out_return_code;
                  str_point_indexing_status_message := rec3.out_status_message;
               
               ELSE
                  RAISE EXCEPTION 'err %',str_nhdplus_version;
               
               END IF;               
            
            ELSE
               RAISE EXCEPTION 'err %',str_point_indexing_method;
               
            END IF;

            INSERT INTO tmp_src2cip(
                permid_joinkey
               ,nhdplusid
               ,cip_method
               ,cip_parms
               ,overlap_measure               
            ) 
            SELECT
             a.permid_joinkey
            ,a.nhdplusid
            ,str_point_indexing_method
            ,NULL
            ,a.overlap_measure
            FROM
            tmp_cip a
            WHERE
            a.permid_joinkey = rec2.permid_joinkey::UUID;            
            
         END LOOP;
         
      END IF;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      IF rec.src_line_count > 0
      THEN
         FOR rec2 IN EXECUTE 'SELECT c.* FROM cipsrv_upload.' || str_dataset_prefix || '_lines c WHERE c.source_joinkey = $1' USING rec.source_joinkey
         LOOP
            --###############################################################--
            str_nhdplus_version := NULL;
            IF rec2.nhdplus_version IS NOT NULL
            THEN
               str_nhdplus_version := rec2.nhdplus_version;
               
            ELSE
               IF str_default_nhdplus_version IS NOT NULL
               THEN
                  str_nhdplus_version := str_default_nhdplus_version;
                  
               END IF;
               
            END IF;
         
            --###############################################################--
            str_known_region := NULL;
            IF rec2.known_region IS NOT NULL
            THEN
               str_known_region := rec2.known_region;
               
            ELSE
               IF str_default_known_region IS NOT NULL
               THEN
                  str_known_region := str_default_known_region;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            str_ring_indexing_method := NULL;
            IF rec2.ring_indexing_method IS NOT NULL
            THEN
               str_ring_indexing_method := rec2.ring_indexing_method;
               
            ELSE
               IF str_default_ring_indexing_method IS NOT NULL
               THEN
                  str_ring_indexing_method := str_default_ring_indexing_method;
               
               END IF;
               
            END IF;
            
            --###*********************************************************###--
            FOR i IN 1 .. ST_NumGeometries(rec2.shape)
            LOOP
               geom_part := ST_GeometryN(rec2.shape,i);
               
               IF str_ring_indexing_method = 'treat_as_lines'
               THEN
                  boo_isring := FALSE;
                  
               ELSE
                  boo_isring := ST_IsRing(geom_part);
               
               END IF;
               
               --- Branch for ring handling -----------------------------------
               IF boo_isring
               THEN
                  str_line_indexing_method := NULL;
                  
                  --###############################################################--
                  num_ring_areacat_threshold := NULL;
                  IF rec2.ring_areacat_threshold IS NOT NULL
                  THEN
                     num_ring_areacat_threshold := rec2.ring_areacat_threshold;
                     
                  ELSE
                     IF num_default_ring_areacat_threshold IS NOT NULL
                     THEN
                        num_ring_areacat_threshold := num_default_ring_areacat_threshold;
                     
                     END IF;
                     
                  END IF;
                  
                  --###############################################################--
                  num_ring_areaevt_threshold := NULL;
                  IF rec2.ring_areaevt_threshold IS NOT NULL
                  THEN
                     num_ring_areaevt_threshold := rec2.ring_areaevt_threshold;
                     
                  ELSE
                     IF num_default_ring_areaevt_threshold IS NOT NULL
                     THEN
                        num_ring_areaevt_threshold := num_default_ring_areaevt_threshold;
                     
                     END IF;
                     
                  END IF;
                  
                  --###############################################################--
                  IF str_ring_indexing_method IS NULL
                  OR str_ring_indexing_method = 'area_simple'
                  THEN
                     IF str_nhdplus_version = 'nhdplus_m'
                     THEN
                        rec3 := cipsrv_nhdplus_m.index_area_simple(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                        
                     ELSIF str_nhdplus_version IN ('nhdplus_h','HR')
                     THEN
                        rec3 := cipsrv_nhdplus_h.index_area_simple(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                     
                     ELSE
                        RAISE EXCEPTION 'err %',str_nhdplus_version;
                     
                     END IF;
                  
                  ELSIF str_ring_indexing_method = 'area_centroid'
                  THEN
                     IF str_nhdplus_version = 'nhdplus_m'
                     THEN
                        rec3 := cipsrv_nhdplus_m.index_area_centroid(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                        
                     ELSIF str_nhdplus_version = 'nhdplus_h'
                     THEN
                        rec3 := cipsrv_nhdplus_h.index_area_centroid(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                     
                     ELSE
                        RAISE EXCEPTION 'err %',str_nhdplus_version;
                     
                     END IF;
                     
                  ELSIF str_ring_indexing_method = 'area_artpath'
                  THEN
                     IF str_nhdplus_version = 'nhdplus_m'
                     THEN
                        rec3 := cipsrv_nhdplus_m.index_area_artpath(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                        
                     ELSIF str_nhdplus_version = 'nhdplus_h'
                     THEN
                        rec3 := cipsrv_nhdplus_h.index_area_artpath(
                            p_geometry             := ST_MakePolygon(geom_part)
                           ,p_geometry_areasqkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_cat_threshold_perc   := num_ring_areacat_threshold
                           ,p_evt_threshold_perc   := num_ring_areaevt_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := ST_MakePolygon(geom_part)
                        );
                        num_ring_indexing_return_code    := rec3.out_return_code;
                        str_ring_indexing_status_message := rec3.out_status_message;
                     
                     ELSE
                        RAISE EXCEPTION 'err %',str_nhdplus_version;
                     
                     END IF;
                     
                  ELSE
                     RAISE EXCEPTION 'err %',str_ring_indexing_method;
                     
                  END IF;
               
               -- Normal line handling here --------------------------------------
               ELSE
                  str_ring_indexing_method := NULL;
                  
                  --###############################################################--
                  str_line_indexing_method := NULL;
                  IF rec2.line_indexing_method IS NOT NULL
                  THEN
                     str_line_indexing_method := rec2.line_indexing_method;
                     
                  ELSE
                     IF str_default_line_indexing_method IS NOT NULL
                     THEN
                        str_line_indexing_method := str_default_line_indexing_method;
                     
                     END IF;
                     
                  END IF;
               
                  --###############################################################--
                  num_line_threshold := NULL;
                  IF rec2.line_threshold IS NOT NULL
                  THEN
                     num_line_threshold := rec2.line_threshold;
                     
                  ELSE
                     IF num_default_line_threshold IS NOT NULL
                     THEN
                        num_line_threshold := num_default_line_threshold;
                     
                     END IF;
                     
                  END IF;

                  --###############################################################--
                  IF str_line_indexing_method IS NULL
                  OR str_line_indexing_method = 'line_simple'
                  THEN
                     IF str_nhdplus_version = 'nhdplus_m'
                     THEN
                        rec3 := cipsrv_nhdplus_m.index_line_simple(
                            p_geometry             := geom_part
                           ,p_geometry_lengthkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_line_threshold_perc  := num_line_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := rec2.shape
                        );
                        num_line_indexing_return_code    := rec3.out_return_code;
                        str_line_indexing_status_message := rec3.out_status_message;
                        
                     ELSIF str_nhdplus_version = 'nhdplus_h'
                     THEN
                        rec3 := cipsrv_nhdplus_h.index_line_simple(
                            p_geometry             := geom_part
                           ,p_geometry_lengthkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_line_threshold_perc  := num_line_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := rec2.shape
                        );
                        num_line_indexing_return_code    := rec3.out_return_code;
                        str_line_indexing_status_message := rec3.out_status_message;
                     
                     ELSE
                        RAISE EXCEPTION 'err %',str_nhdplus_version;
                     
                     END IF;
                  
                  ELSIF str_line_indexing_method = 'line_levelpath'
                  THEN
                     IF str_nhdplus_version = 'nhdplus_m'
                     THEN
                        rec3 := cipsrv_nhdplus_m.index_line_levelpath(
                            p_geometry             := geom_part
                           ,p_geometry_lengthkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_line_threshold_perc  := num_line_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := rec2.shape
                        );
                        num_line_indexing_return_code    := rec3.out_return_code;
                        str_line_indexing_status_message := rec3.out_status_message;
                        
                     ELSIF str_nhdplus_version = 'nhdplus_h'
                     THEN
                        rec3 := cipsrv_nhdplus_h.index_line_levelpath(
                            p_geometry             := geom_part
                           ,p_geometry_lengthkm    := NULL
                           ,p_known_region         := str_known_region
                           ,p_line_threshold_perc  := num_line_threshold
                           ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                           ,p_permid_geometry      := rec2.shape
                        );
                        num_line_indexing_return_code    := rec3.out_return_code;
                        str_line_indexing_status_message := rec3.out_status_message;
                     
                     ELSE
                        RAISE EXCEPTION 'err %',str_nhdplus_version;
                     
                     END IF;
                     
                  ELSE
                     RAISE EXCEPTION 'err %',str_line_indexing_method;
                     
                  END IF;

               END IF;

            END LOOP;
            
            INSERT INTO tmp_src2cip(
                permid_joinkey
               ,nhdplusid
               ,cip_method
               ,cip_parms
               ,overlap_measure               
            ) 
            SELECT
             a.permid_joinkey
            ,a.nhdplusid
            ,str_line_indexing_method
            ,num_line_threshold::VARCHAR
            ,a.overlap_measure
            FROM
            tmp_cip a
            WHERE
            a.permid_joinkey = rec2.permid_joinkey::UUID; 

         END LOOP;
      
      END IF;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
      IF rec.src_area_count > 0
      THEN
         FOR rec2 IN EXECUTE 'SELECT d.* FROM cipsrv_upload.' || str_dataset_prefix || '_areas d WHERE d.source_joinkey = $1' USING rec.source_joinkey
         LOOP
            --###############################################################--
            str_nhdplus_version := NULL;
            IF rec2.nhdplus_version IS NOT NULL
            THEN
               str_nhdplus_version := rec2.nhdplus_version;
               
            ELSE
               IF str_default_nhdplus_version IS NOT NULL
               THEN
                  str_nhdplus_version := str_default_nhdplus_version;
                  
               END IF;
               
            END IF;
            
            --###############################################################--
            str_known_region := NULL;
            IF rec2.known_region IS NOT NULL
            THEN
               str_known_region := rec2.known_region;
               
            ELSE
               IF str_default_known_region IS NOT NULL
               THEN
                  str_known_region := str_default_known_region;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            str_area_indexing_method := NULL;
            IF rec2.area_indexing_method IS NOT NULL
            THEN
               str_area_indexing_method := rec2.area_indexing_method;
               
            ELSE
               IF str_default_area_indexing_method IS NOT NULL
               THEN
                  str_area_indexing_method := str_default_area_indexing_method;
               
               END IF;
               
            END IF;
  
            --###############################################################--
            num_areacat_threshold := NULL;
            IF rec2.areacat_threshold IS NOT NULL
            THEN
               num_areacat_threshold := rec2.areacat_threshold;
               
            ELSE
               IF num_default_areacat_threshold IS NOT NULL
               THEN
                  num_areacat_threshold := num_default_areacat_threshold;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            num_areaevt_threshold := NULL;
            IF rec2.areaevt_threshold IS NOT NULL
            THEN
               num_areaevt_threshold := rec2.areaevt_threshold;
               
            ELSE
               IF num_default_areaevt_threshold IS NOT NULL
               THEN
                  num_areaevt_threshold := num_default_areaevt_threshold;
               
               END IF;
               
            END IF;
            
            --###############################################################--
            IF str_area_indexing_method IS NULL
            OR str_area_indexing_method = 'area_simple'
            THEN
               IF str_nhdplus_version = 'nhdplus_m'
               THEN
                  rec3 := cipsrv_nhdplus_m.index_area_simple(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;
                  
               ELSIF str_nhdplus_version = 'nhdplus_h'
               THEN
                  rec3 := cipsrv_nhdplus_h.index_area_simple(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;
               
               ELSE
                  RAISE EXCEPTION 'err %',str_nhdplus_version;
               
               END IF;
            
            ELSIF str_area_indexing_method = 'area_centroid'
            THEN
               IF str_nhdplus_version = 'nhdplus_m'
               THEN
                  rec3 := cipsrv_nhdplus_m.index_area_centroid(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;
                  
               ELSIF str_nhdplus_version = 'nhdplus_h'
               THEN
                  rec3 := cipsrv_nhdplus_h.index_area_centroid(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;
               
               ELSE
                  RAISE EXCEPTION 'err %',str_nhdplus_version;
               
               END IF;
               
            ELSIF str_area_indexing_method = 'area_artpath'
            THEN
               IF str_nhdplus_version = 'nhdplus_m'
               THEN
                  rec3 := cipsrv_nhdplus_m.index_area_artpath(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;
                  
               ELSIF str_nhdplus_version = 'nhdplus_h'
               THEN
                  rec3 := cipsrv_nhdplus_h.index_area_artpath(
                      p_geometry             := rec2.shape
                     ,p_geometry_areasqkm    := NULL
                     ,p_known_region         := str_known_region
                     ,p_cat_threshold_perc   := num_areacat_threshold
                     ,p_evt_threshold_perc   := num_areaevt_threshold
                     ,p_permid_joinkey       := rec2.permid_joinkey::UUID
                     ,p_permid_geometry      := rec2.shape
                  );
                  num_area_indexing_return_code    := rec3.out_return_code;
                  str_area_indexing_status_message := rec3.out_status_message;

               ELSE
                  RAISE EXCEPTION 'err %',str_nhdplus_version;
               
               END IF;
               
            ELSE
               RAISE EXCEPTION 'err %',str_area_indexing_method;
               
            END IF;
            
            INSERT INTO tmp_src2cip(
                permid_joinkey
               ,nhdplusid
               ,cip_method
               ,cip_parms
               ,overlap_measure               
            ) 
            SELECT
             a.permid_joinkey
            ,a.nhdplusid
            ,str_area_indexing_method
            ,num_areacat_threshold::VARCHAR || ',' || num_areaevt_threshold::VARCHAR
            ,a.overlap_measure
            FROM
            tmp_cip a
            WHERE
            a.permid_joinkey = rec2.permid_joinkey::UUID; 
            
         END LOOP;
      
      END IF;
      
      IF str_nhdplus_version = 'nhdplus_m'
      THEN
         str_catchment_resolution := 'MR';
         
      ELSIF str_nhdplus_version = 'nhdplus_h'
      THEN
         str_catchment_resolution := 'HR';
         
      ELSE
         str_catchment_resolution := str_nhdplus_version;
      
      END IF;
      
      --************************************************************--
      str_sql := 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_cip( '
              || '    objectid '
              || '   ,source_originator '
              || '   ,source_featureid '
              || '   ,source_featureid2 '
              || '   ,source_series '
              || '   ,source_subdivision '
              || '   ,source_joinkey '
              || '   ,start_date '
              || '   ,end_date '
              || '   ,cat_joinkey '
              || '   ,catchmentstatecode '
              || '   ,nhdplusid '
              || '   ,istribal '
              || '   ,catchmentresolution '
              || '   ,catchmentareasqkm '
              || '   ,xwalk_huc12 '
              || '   ,xwalk_method '
              || '   ,xwalk_huc12_version '
              || '   ,isnavigable '
              || '   ,hasvaa '
              || '   ,issink '
              || '   ,isheadwater '
              || '   ,iscoastal '
              || '   ,isocean '
              || '   ,isalaskan '
              || '   ,h3hexagonaddr '
              || '   ,globalid '
              || ') '
              || 'SELECT '
              || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_cip_seq'') AS objectid '
              || ',$1 '
              || ',$2 '
              || ',$3 '
              || ',$4 '
              || ',$5 '
              || ',$6 '
              || ',$7 '
              || ',$8 '
              || ',a.catchmentstatecode || a.nhdplusid::BIGINT::VARCHAR '
              || ',a.catchmentstatecode '
              || ',a.nhdplusid '
              || ',a.istribal '
              || ',$9 '
              || ',a.areasqkm AS catchmentareasqkm '
              || ',a.xwalk_huc12_mr '
              || ',NULL '
              || ',NULL '
              || ',a.isnavigable '
              || ',a.hasvaa '
              || ',a.issink '
              || ',a.isheadwater '
              || ',a.iscoastal '
              || ',a.isocean '
              || ',CASE '
              || ' WHEN a.h3hexagonaddr IS NOT NULL '
              || ' THEN '
              || '    ''Y'' '
              || ' ELSE '
              || '    ''N'' '
              || ' END AS isalaskan '
              || ',a.h3hexagonaddr '
              || ',''{'' || uuid_generate_v1() || ''}'' '
              || 'FROM '
              || 'cipsrv_' || str_nhdplus_version || '.catchment_fabric a '
              || 'WHERE '
              || 'EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid) '
              || 'AND (NOT $10 OR a.catchmentstatecode = ANY($11) ) '
              || 'AND (NOT $12 OR a.istribal = ''Y'')'
              || 'AND (NOT $13 OR a.istribal = ''N'')';
              
      EXECUTE str_sql 
      USING 
       rec.source_originator
      ,rec.source_featureid
      ,rec.source_featureid2
      ,rec.source_series
      ,rec.source_subdivision
      ,rec.source_joinkey
      ,rec.start_date
      ,rec.end_date
      ,str_catchment_resolution
      ,boo_filter_by_state
      ,ary_state_filters
      ,boo_filter_by_tribal
      ,boo_filter_by_notribal;

      GET DIAGNOSTICS int_count = ROW_COUNT;

      IF str_nhdplus_version = 'nhdplus_m'
      THEN
         int_cat_mr_count := int_count;
         int_cat_hr_count := 0;
         
      ELSIF str_nhdplus_version = 'nhdplus_h'
      THEN
         int_cat_mr_count := 0;
         int_cat_hr_count := int_count;
         
      END IF;
      
      COMMIT;
      
      EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_cip';
      
      --************************************************************--
      str_sql := 'INSERT INTO cipsrv_upload.' || str_dataset_prefix || '_src2cip( '
              || '    objectid '
              || '   ,source_joinkey '
              || '   ,permid_joinkey '
              || '   ,cat_joinkey '
              || '   ,catchmentstatecode '
              || '   ,nhdplusid '
              || '   ,overlap_measure '
              || '   ,cip_method '
              || '   ,cip_parms '
              || '   ,cip_date '
              || '   ,cip_version '
              || '   ,globalid '
              || ') '
              || 'SELECT '
              || ' NEXTVAL(''cipsrv_upload.' || str_dataset_prefix || '_src2cip_seq'') AS objectid '
              || ',b.source_joinkey '
              || ',''{'' || a.permid_joinkey::VARCHAR || ''}'' '
              || ',b.cat_joinkey '
              || ',b.catchmentstatecode '
              || ',b.nhdplusid '
              || ',a.overlap_measure '
              || ',a.cip_method '
              || ',a.cip_parms '
              || ',$1 '
              || ',$2 '
              || ',''{'' || uuid_generate_v1() || ''}'' '
              || 'FROM '
              || 'tmp_src2cip a '
              || 'JOIN '
              || 'cipsrv_upload.' || str_dataset_prefix || '_cip b '
              || 'ON '
              || 'a.nhdplusid = b.nhdplusid '
              || 'WHERE '
              || 'b.source_joinkey = $3 '
              || 'ON CONFLICT DO NOTHING ';
              
      EXECUTE str_sql 
      USING
       CURRENT_TIMESTAMP
      ,cipsrv_engine.cipsrv_version()
      ,rec.source_joinkey;

      GET DIAGNOSTICS int_count = ROW_COUNT;
      
      COMMIT;
      
      EXECUTE 'ANALYZE cipsrv_upload.' || str_dataset_prefix || '_src2cip';
      
      --************************************************************--
      EXECUTE 'TRUNCATE TABLE tmp_cip';
      EXECUTE 'TRUNCATE TABLE tmp_src2cip';
      
      --************************************************************--
      str_sql := 'UPDATE cipsrv_upload.' || str_dataset_prefix || '_sfid a '
              || 'SET '
              || ' point_indexing_method_used    = $1 '
              || ',point_indexing_return_code    = $2 '
              || ',point_indexing_status_message = $3 '
              || ',line_indexing_method_used     = $4 '
              || ',line_indexing_return_code     = $5 '
              || ',line_indexing_status_message  = $6 '
              || ',ring_indexing_method_used     = $7 '
              || ',ring_indexing_return_code     = $8 '
              || ',ring_indexing_status_message  = $9 '
              || ',area_indexing_method_used     = $10 '
              || ',area_indexing_return_code     = $11 '
              || ',area_indexing_status_message  = $12 '
              || ',cat_mr_count                  = $13 '
              || ',cat_hr_count                  = $14 '
              || 'WHERE '
              || 'a.source_joinkey = $15 ';
      
      EXECUTE str_sql
      USING
       str_point_indexing_method
      ,num_point_indexing_return_code
      ,str_point_indexing_status_message
      ,str_line_indexing_method
      ,num_line_indexing_return_code
      ,str_line_indexing_status_message
      ,str_ring_indexing_method
      ,num_ring_indexing_return_code
      ,str_ring_indexing_status_message
      ,str_area_indexing_method
      ,num_area_indexing_return_code
      ,str_area_indexing_status_message
      ,int_cat_mr_count
      ,int_cat_hr_count
      ,rec.source_joinkey;
      
      COMMIT;

   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 150
   -- Clip features AFTER if requested
   ----------------------------------------------------------------------------
   IF str_geometry_clip_stage = 'AFTER'
   AND str_geometry_clip IS NOT NULL
   AND array_length(ary_geometry_clip,1) > 0
   THEN
      rec := cipsrv_engine.feature_batch_clip(
          p_dataset_prefix        := str_dataset_prefix
         ,p_clippers              := ary_geometry_clip
         ,p_known_region          := str_default_known_region
      );
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      COMMIT;

   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER PROCEDURE cipsrv_engine.cipsrv_batch_index(
    VARCHAR
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON PROCEDURE cipsrv_engine.cipsrv_batch_index(
    VARCHAR
   ,VARCHAR
) TO PUBLIC;

