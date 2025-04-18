--******************************--
----- functions/create_updn_temp_tables.sql 

CREATE OR REPLACE FUNCTION cipsrv_owld.create_updn_temp_tables()
RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_catchments temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_sfid_found')
   THEN
      TRUNCATE TABLE tmp_sfid_found;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_sfid_found(
          nhdplusid                       BIGINT
         ,eventtype                       INTEGER
         ,permid_joinkey                  VARCHAR(40)
         ,source_originator               VARCHAR(130)
         ,source_featureid                VARCHAR(100)
         ,source_featureid2               VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,source_joinkey                  VARCHAR(40)
         ,start_date                      DATE
         ,end_date                        DATE
         ,sfiddetailurl                   VARCHAR(255)
         ,src_event_count                 INTEGER
         ,rad_event_count                 INTEGER
         ,src_cat_joinkey_count           INTEGER
         ,nearest_cip_distancekm_permid   VARCHAR(40)
         ,nearest_cip_distancekm_cat      VARCHAR(40)
         ,nearest_cip_network_distancekm  NUMERIC
         ,nearest_rad_distancekm_permid   VARCHAR(40)
         ,nearest_rad_network_distancekm  NUMERIC
         ,nearest_cip_flowtimeday_permid  VARCHAR(40)
         ,nearest_cip_flowtimeday_cat     VARCHAR(40)
         ,nearest_cip_network_flowtimeday NUMERIC
         ,nearest_rad_flowtimeday_permid  VARCHAR(40)
         ,nearest_rad_network_flowtimeday NUMERIC     
      );

      CREATE INDEX tmp_sfid_found_01i
      ON tmp_sfid_found(eventtype);
      
      CREATE INDEX tmp_sfid_found_02i
      ON tmp_sfid_found(source_joinkey);
      
      CREATE INDEX tmp_sfid_found_03i
      ON tmp_sfid_found(source_featureid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_catchments temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_rad_points')
   THEN
      TRUNCATE TABLE tmp_rad_points;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_rad_points(            
          eventtype                    INTEGER NOT NULL
         ,permanent_identifier         VARCHAR(40) NOT NULL
         ,eventdate                    DATE
         ,reachcode                    VARCHAR(14)
         ,reachsmdate                  DATE
         ,reachresolution              VARCHAR(2)
         ,feature_permanent_identifier VARCHAR(40)
         ,featureclassref              INTEGER
         ,source_originator            VARCHAR(130) NOT NULL
         ,source_featureid             VARCHAR(100) NOT NULL
         ,source_featureid2            VARCHAR(100)
         ,source_datadesc              VARCHAR(100)
         ,source_series                VARCHAR(100)
         ,source_subdivision           VARCHAR(100)
         ,source_joinkey               VARCHAR(40)  NOT NULL
         ,permid_joinkey               VARCHAR(40)  NOT NULL
         ,start_date                   DATE
         ,end_date                     DATE
         ,featuredetailurl             VARCHAR(255)
         ,measure                      NUMERIC
         ,eventoffset                  NUMERIC
         ,geogstate                    VARCHAR(2)
         ,xwalk_huc12                  VARCHAR(12)
         ,navigable                    VARCHAR(1)
         ,network_distancekm           NUMERIC
         ,network_flowtimeday          NUMERIC
         ,shape                        GEOMETRY         
      );

      CREATE INDEX tmp_rad_points_01i
      ON tmp_rad_points(eventtype);
      
      CREATE INDEX tmp_rad_points_02i
      ON tmp_rad_points(source_joinkey);
      
      CREATE INDEX tmp_rad_points_03i
      ON tmp_rad_points(permid_joinkey);
      
      CREATE INDEX tmp_rad_points_04i
      ON tmp_rad_points(source_featureid);
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_owld.create_updn_temp_tables()
OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_owld.create_updn_temp_tables()
TO PUBLIC;
--******************************--
----- functions/adjust_point_extent.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.adjust_point_extent';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_owld.adjust_point_extent(
    IN  p_extent_value                   NUMERIC
   ,IN  p_direction                      VARCHAR
   ,IN  p_flowline_amount                NUMERIC
   ,IN  p_flowline_fmeasure              NUMERIC
   ,IN  p_flowline_tmeasure              NUMERIC
   ,IN  p_event_measure                  NUMERIC
)
RETURNS NUMERIC
IMMUTABLE
AS $BODY$
DECLARE
   str_direction VARCHAR := UPPER(p_direction);
   num_amount    NUMERIC;
 
BEGIN
   
   IF str_direction IN ('UT','UM','U')
   THEN
      IF p_flowline_tmeasure = p_event_measure
      THEN
         RETURN p_extent_value;

      END IF;

      num_amount := p_flowline_amount * ((p_flowline_tmeasure - p_event_measure) / (p_flowline_tmeasure - p_flowline_fmeasure));

   ELSIF str_direction IN ('DM','DD','PP','D')
   THEN
      IF p_flowline_fmeasure = p_event_measure
      THEN
         RETURN p_extent_value;

      END IF;

      num_amount := p_flowline_amount * ((p_event_measure - p_flowline_fmeasure) / (p_flowline_tmeasure - p_flowline_fmeasure));

   ELSE
      RAISE EXCEPTION 'err';

   END IF;

   RETURN p_extent_value - num_amount;
   
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.adjust_point_extent';
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
--******************************--
----- functions/owld_programs.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.owld_programs';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_owld.owld_programs()
RETURNS TABLE(
    program_id          VARCHAR
   ,program_name        VARCHAR
   ,program_description VARCHAR
   ,program_url         VARCHAR
   ,program_eventtype   INTEGER
   ,program_vintage     DATE
   ,program_resolutions VARCHAR[]
   ,program_precisions  VARCHAR[]
)
VOLATILE
AS $BODY$
DECLARE
   rec         RECORD;
   rec2        RECORD;
   int_counter INTEGER;
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Collect available programs
   ----------------------------------------------------------------------------
   FOR rec IN 
      SELECT
       a.table_name
      ,UPPER(REPLACE(a.table_name,'_control','')) AS program_id 
      FROM 
      information_schema.tables a
      WHERE 
          a.table_schema  = 'cipsrv_owld'
      AND a.table_name LIKE '%_control'
   LOOP
      program_id          := rec.program_id;
      program_name        := NULL;
      program_description := NULL;
      program_url         := NULL;
      program_eventtype   := NULL;
      program_vintage     := NULL;
      program_resolutions := NULL;
      
      BEGIN
         EXECUTE 'SELECT a.value_str FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''NAME'' LIMIT 1' 
         INTO program_name; 
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      BEGIN
         EXECUTE 'SELECT a.value_str FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''DESCRIPTION'' LIMIT 1' 
         INTO program_description; 
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      BEGIN
         EXECUTE 'SELECT a.value_str FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''URL'' LIMIT 1' 
         INTO program_url; 
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      BEGIN
         EXECUTE 'SELECT a.value_num::INT FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''EVENTTYPE'' LIMIT 1' 
         INTO program_eventtype; 
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      BEGIN
         EXECUTE 'SELECT a.value_date FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''VINTAGE'' LIMIT 1' 
         INTO program_vintage;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
         WHEN OTHERS THEN RAISE;
      END;
      
      int_counter := 1;
      FOR rec2 IN EXECUTE '
         SELECT
         a.value_str
         FROM cipsrv_owld.' || rec.table_name || ' a
         WHERE UPPER(a.keyword) = ''RESOLUTION'' 
         ORDER BY a.value_str'
      LOOP
         program_resolutions[int_counter] := rec2.value_str;
         int_counter := int_counter + 1;
      
      END LOOP;
      
      int_counter := 1;
      FOR rec2 IN EXECUTE '
         SELECT
         a.value_str
         FROM cipsrv_owld.' || rec.table_name || ' a
         WHERE UPPER(a.keyword) = ''PRECISION'' 
         ORDER BY a.value_str'
      LOOP
         program_precisions[int_counter] := rec2.value_str;
         int_counter := int_counter + 1;
      
      END LOOP;
      
      RETURN NEXT;
      
   END LOOP;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.owld_programs';
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
--******************************--
----- functions/owld_locator.sql 

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
--******************************--
----- functions/upstreamdownstream.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.upstreamdownstream';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_owld.upstreamdownstream(
    IN  p_nhdplus_version               VARCHAR
   ,IN  p_search_type                   VARCHAR
   
   ,IN  p_start_nhdplusid               BIGINT
   ,IN  p_start_permanent_identifier    VARCHAR
   ,IN  p_start_reachcode               VARCHAR
   ,IN  p_start_hydroseq                BIGINT
   ,IN  p_start_measure                 NUMERIC
   ,IN  p_start_source_featureid        VARCHAR
   ,IN  p_start_source_featureid2       VARCHAR
   ,IN  p_start_source_originator       VARCHAR
   ,IN  p_start_source_series           VARCHAR
   ,IN  p_start_start_date              DATE
   ,IN  p_start_end_date                DATE
   ,IN  p_start_permid_joinkey          VARCHAR
   ,IN  p_start_source_joinkey          VARCHAR
   ,IN  p_start_cat_joinkey             VARCHAR
   ,IN  p_start_linked_data_program     VARCHAR
   ,IN  p_start_search_precision        VARCHAR
      
   ,IN  p_stop_nhdplusid                BIGINT
   ,IN  p_stop_permanent_identifier     VARCHAR
   ,IN  p_stop_reachcode                VARCHAR
   ,IN  p_stop_hydroseq                 BIGINT
   ,IN  p_stop_measure                  NUMERIC
   ,IN  p_stop_source_featureid         VARCHAR
   ,IN  p_stop_source_featureid2        VARCHAR
   ,IN  p_stop_source_originator        VARCHAR
   ,IN  p_stop_source_series            VARCHAR
   ,IN  p_stop_start_date               DATE
   ,IN  p_stop_end_date                 DATE
   ,IN  p_stop_permid_joinkey           VARCHAR
   ,IN  p_stop_source_joinkey           VARCHAR
   ,IN  p_stop_cat_joinkey              VARCHAR
   ,IN  p_stop_linked_data_program      VARCHAR
   ,IN  p_stop_search_precision         VARCHAR
   
   ,IN  p_max_distancekm                NUMERIC
   ,IN  p_max_flowtimeday               NUMERIC
   ,IN  p_linked_data_search_list       VARCHAR[]
   ,IN  p_search_precision              VARCHAR
   
   ,IN  p_return_flowlines              BOOLEAN   
   ,IN  p_return_flowline_details       BOOLEAN
   ,IN  p_return_flowline_geometry      BOOLEAN
   ,IN  p_return_catchments             BOOLEAN
   ,IN  p_return_linked_data_cip        BOOLEAN
   ,IN  p_return_linked_data_huc12      BOOLEAN
   ,IN  p_return_linked_data_source     BOOLEAN
   ,IN  p_return_linked_data_rad        BOOLEAN
   ,IN  p_return_linked_data_attributes BOOLEAN
   ,IN  p_remove_stop_start_sfids       BOOLEAN
   ,IN  p_push_source_geometry_as_rad   BOOLEAN
   
   ,IN  p_known_region                  VARCHAR DEFAULT NULL
   
   ,OUT out_start_nhdplusid             BIGINT
   ,OUT out_start_permanent_identifier  VARCHAR
   ,OUT out_start_measure               NUMERIC
   ,OUT out_grid_srid                   INTEGER
   ,OUT out_stop_nhdplusid              BIGINT
   ,OUT out_stop_measure                NUMERIC
   ,OUT out_flowline_count              INTEGER
   ,OUT out_rad_found_count             INTEGER
   ,OUT out_sfid_found_count            INTEGER 
   ,OUT out_return_flowlines            BOOLEAN
   ,OUT out_return_code                 NUMERIC
   ,OUT out_status_message              VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                            RECORD;
   str_nhdplus_version            VARCHAR := p_nhdplus_version;
   str_search_type                VARCHAR := p_search_type;
   num_max_distancekm             NUMERIC := p_max_distancekm;
   num_max_flowtimeday            NUMERIC := p_max_flowtimeday;
   boo_return_flowline_details    BOOLEAN := p_return_flowline_details;
   boo_return_flowline_geometry   BOOLEAN := p_return_flowline_geometry;
   str_known_region               VARCHAR := p_known_region;

   int_start_nhdplusid            BIGINT  := p_start_nhdplusid;
   str_start_permanent_identifier VARCHAR := p_start_permanent_identifier;
   int_start_hydroseq             BIGINT  := p_start_hydroseq;
   str_start_reachcode            VARCHAR := p_start_reachcode;
   int_start_catnhdplusid         BIGINT;
   num_start_measure              NUMERIC := p_start_measure;
   sdo_start_shape                GEOMETRY;
   str_start_source_featureid     VARCHAR;
   str_start_source_featureid2    VARCHAR;
   str_start_source_originator    VARCHAR;
   str_start_source_series        VARCHAR;
   str_start_source_subdivision   VARCHAR;
   dat_start_start_date           DATE;
   dat_start_end_date             DATE;
   str_start_source_joinkey       VARCHAR;
   str_start_permid_joinkey       VARCHAR;
   str_start_cat_joinkey          VARCHAR;
   str_start_search_precision     VARCHAR := p_start_search_precision;
   
   int_stop_nhdplusid             BIGINT  := p_stop_nhdplusid;
   str_stop_permanent_identifier  VARCHAR := p_stop_permanent_identifier;
   int_stop_hydroseq              BIGINT  := p_stop_hydroseq;
   str_stop_reachcode             VARCHAR := p_stop_reachcode;
   int_stop_catnhdplusid          BIGINT;
   num_stop_measure               NUMERIC := p_stop_measure;
   sdo_stop_shape                 GEOMETRY;
   str_stop_source_featureid      VARCHAR;
   str_stop_source_featureid2     VARCHAR;
   str_stop_source_originator     VARCHAR;
   str_stop_source_series         VARCHAR;
   str_stop_source_subdivision    VARCHAR;
   dat_stop_start_date            DATE;
   dat_stop_end_date              DATE;
   str_stop_source_joinkey        VARCHAR;
   str_stop_permid_joinkey        VARCHAR;
   str_stop_cat_joinkey           VARCHAR;
   str_stop_search_precision      VARCHAR := p_stop_search_precision;
   
   str_search_precision           VARCHAR := p_search_precision;   
   int_wqp_eventtype              INTEGER := 10032;
   boo_remove_stop_start_sfids    BOOLEAN;
   str_remove_start_permid        VARCHAR(40);
   str_remove_stop_permid         VARCHAR(40);
      
BEGIN

   out_return_code := cipsrv_engine.create_navigation_temp_tables();
   out_return_code := cipsrv_owld.create_updn_temp_tables();

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   out_return_code    := 0;
   
   IF str_nhdplus_version IS NULL
   THEN
      str_nhdplus_version := 'nhdplus_m';
      
   END IF;
   
   IF str_search_type IS NULL
   THEN
      str_search_type := 'UT';
      
   END IF;
   
   IF str_search_precision IS NULL
   THEN
      str_search_precision := 'CATCHMENT';
      
   END IF;
   
   IF p_remove_stop_start_sfids IS NULL
   THEN
      boo_remove_stop_start_sfids := TRUE;
      
   ELSE
      boo_remove_stop_start_sfids := p_remove_stop_start_sfids;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Convert any program locations to network locations
   ----------------------------------------------------------------------------
   IF p_start_linked_data_program IS NOT NULL
   THEN
   
      rec := cipsrv_owld.owld_locator(
          p_nhdplus_version               := str_nhdplus_version
         ,p_source_featureid              := p_start_source_featureid
         ,p_source_featureid2             := p_start_source_featureid2
         ,p_source_originator             := p_start_source_originator
         ,p_source_series                 := p_start_source_series
         ,p_start_date                    := p_start_start_date
         ,p_end_date                      := p_start_end_date
         ,p_linked_data_program           := p_start_linked_data_program
         ,p_source_joinkey                := p_start_source_joinkey
         ,p_permid_joinkey                := p_start_permid_joinkey
         ,p_cat_joinkey                   := p_start_cat_joinkey
         ,p_search_direction              := str_search_type
         ,p_reference_catchment_nhdplusid := NULL
         ,p_reference_reachcode           := NULL
         ,p_reference_flowline_nhdplusid  := NULL
         ,p_reference_hydroseq            := NULL
         ,p_reference_measure             := NULL
         ,p_reference_point               := NULL
         ,p_search_precision              := str_start_search_precision
         ,p_known_region                  := str_known_region
      );
      int_start_catnhdplusid         := rec.out_catchment_nhdplusid;
      str_start_permanent_identifier := rec.out_permanent_identifier;
      str_start_reachcode            := rec.out_reachcode;
      int_start_nhdplusid            := rec.out_flowline_nhdplusid;
      int_start_hydroseq             := rec.out_hydroseq;
      num_start_measure              := rec.out_measure;
      sdo_start_shape                := rec.out_shape;
      str_start_source_featureid     := rec.out_source_featureid;
      str_start_source_featureid2    := rec.out_source_featureid2;
      str_start_source_originator    := rec.out_source_originator;
      str_start_source_series        := rec.out_source_series;
      str_start_source_subdivision   := rec.out_source_subdivision;
      dat_start_start_date           := rec.out_start_date;
      dat_start_end_date             := rec.out_end_date;
      str_start_source_joinkey       := rec.out_source_joinkey;
      str_start_permid_joinkey       := rec.out_permid_joinkey;
      str_start_cat_joinkey          := rec.out_cat_joinkey;
      out_return_code                := rec.out_return_code;
      out_status_message             := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
         
      END IF;
      
      IF boo_remove_stop_start_sfids
      THEN
         str_remove_start_permid := str_start_permid_joinkey;
      
      END IF;
   
   END IF;
   
   IF p_stop_linked_data_program IS NOT NULL
   THEN
   
      rec := cipsrv_owld.owld_locator(
          p_nhdplus_version               := str_nhdplus_version
         ,p_source_featureid              := p_stop_source_featureid
         ,p_source_featureid2             := p_stop_source_featureid2
         ,p_source_originator             := p_stop_source_originator
         ,p_source_series                 := p_stop_source_series
         ,p_start_date                    := p_stop_start_date
         ,p_end_date                      := p_stop_end_date
         ,p_linked_data_program           := p_stop_linked_data_program
         ,p_source_joinkey                := p_stop_source_joinkey
         ,p_permid_joinkey                := p_stop_permid_joinkey
         ,p_cat_joinkey                   := p_stop_cat_joinkey
         ,p_search_direction              := str_search_type
         ,p_reference_catchment_nhdplusid := NULL
         ,p_reference_reachcode           := NULL
         ,p_reference_flowline_nhdplusid  := NULL
         ,p_reference_hydroseq            := NULL
         ,p_reference_measure             := NULL
         ,p_reference_point               := NULL
         ,p_search_precision              := str_stop_search_precision
         ,p_known_region                  := str_known_region
      );
      int_stop_catnhdplusid          := rec.out_catchment_nhdplusid;
      str_stop_permanent_identifier  := rec.out_permanent_identifier;
      str_stop_reachcode             := rec.out_reachcode;
      int_stop_nhdplusid             := rec.out_flowline_nhdplusid;
      int_stop_hydroseq              := rec.out_hydroseq;
      num_stop_measure               := rec.out_measure;
      sdo_stop_shape                 := rec.out_shape;
      str_stop_source_featureid      := rec.out_source_featureid;
      str_stop_source_featureid2     := rec.out_source_featureid2;
      str_stop_source_originator     := rec.out_source_originator;
      str_stop_source_series         := rec.out_source_series;
      str_stop_source_subdivision    := rec.out_source_subdivision;
      dat_stop_start_date            := rec.out_start_date;
      dat_stop_end_date              := rec.out_end_date;
      str_stop_source_joinkey        := rec.out_source_joinkey;
      str_stop_permid_joinkey        := rec.out_permid_joinkey;
      str_stop_cat_joinkey           := rec.out_cat_joinkey;
      out_return_code                := rec.out_return_code;
      out_status_message             := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
         
      END IF;
      
      IF boo_remove_stop_start_sfids
      THEN
         str_remove_stop_permid := str_stop_permid_joinkey;
      
      END IF;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Call the navigation engine
   ----------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cipsrv_nhdplus_m.navigate(
          p_search_type                 := str_search_type
         ,p_start_nhdplusid             := int_start_nhdplusid
         ,p_start_permanent_identifier  := str_start_permanent_identifier
         ,p_start_reachcode             := str_start_reachcode
         ,p_start_hydroseq              := int_start_hydroseq
         ,p_start_measure               := num_start_measure
         ,p_stop_nhdplusid              := int_stop_nhdplusid
         ,p_stop_permanent_identifier   := str_stop_permanent_identifier
         ,p_stop_reachcode              := str_stop_reachcode
         ,p_stop_hydroseq               := int_stop_hydroseq
         ,p_stop_measure                := num_stop_measure
         ,p_max_distancekm              := num_max_distancekm
         ,p_max_flowtimeday             := num_max_flowtimeday
         ,p_return_flowline_details     := boo_return_flowline_details
         ,p_return_flowline_geometry    := boo_return_flowline_geometry
         ,p_known_region                := str_known_region
      );
      int_start_nhdplusid := rec.out_start_nhdplusid;
      str_start_permanent_identifier := rec.out_start_permanent_identifier;
      num_start_measure   := rec.out_start_measure;
      out_grid_srid       := rec.out_grid_srid;
      int_stop_nhdplusid  := rec.out_stop_nhdplusid;
      num_stop_measure    := rec.out_stop_measure;
      out_flowline_count  := rec.out_flowline_count;
      out_return_code     := rec.out_return_code;
      out_status_message  := rec.out_status_message;
      
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cipsrv_nhdplus_h.navigate(
          p_search_type                 := str_search_type
         ,p_start_nhdplusid             := int_start_nhdplusid
         ,p_start_permanent_identifier  := str_start_permanent_identifier
         ,p_start_reachcode             := str_start_reachcode
         ,p_start_hydroseq              := int_start_hydroseq
         ,p_start_measure               := num_start_measure
         ,p_stop_nhdplusid              := int_stop_nhdplusid
         ,p_stop_permanent_identifier   := str_stop_permanent_identifier
         ,p_stop_reachcode              := str_stop_reachcode
         ,p_stop_hydroseq               := int_stop_hydroseq
         ,p_stop_measure                := num_stop_measure
         ,p_max_distancekm              := num_max_distancekm
         ,p_max_flowtimeday             := num_max_flowtimeday
         ,p_return_flowline_details     := boo_return_flowline_details
         ,p_return_flowline_geometry    := boo_return_flowline_geometry
         ,p_known_region                := str_known_region
      );
      int_start_nhdplusid := rec.out_start_nhdplusid;
      str_start_permanent_identifier := rec.out_start_permanent_identifier;
      num_start_measure   := rec.out_start_measure;
      out_grid_srid       := rec.out_grid_srid;
      int_stop_nhdplusid  := rec.out_stop_nhdplusid;
      num_stop_measure    := rec.out_stop_measure;
      out_flowline_count  := rec.out_flowline_count;
      out_return_code     := rec.out_return_code;
      out_status_message  := rec.out_status_message;
   
   ELSE
      RAISE EXCEPTION 'err %',str_nhdplus_version;
      
   END IF;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Search programs for results
   ----------------------------------------------------------------------------
   IF p_linked_data_search_list IS NOT NULL
   AND array_length(p_linked_data_search_list,1) > 0
   THEN
   
      FOR i IN 1 .. array_length(p_linked_data_search_list,1)
      LOOP
         IF p_linked_data_search_list[i] = int_wqp_eventtype::VARCHAR
         THEN
            out_sfid_found_count := NULL;
            
            IF str_search_precision = 'CATCHMENT'
            THEN
               NULL;
               
            ELSIF str_search_precision IN ('REACH','REACHED MEASURES','ALL')
            THEN
               IF str_nhdplus_version = 'nhdplus_m'
               THEN            
                  INSERT INTO tmp_rad_points(
                      eventtype
                     ,permanent_identifier
                     ,eventdate
                     ,reachcode
                     ,reachsmdate
                     ,reachresolution
                     ,feature_permanent_identifier
                     ,featureclassref
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_datadesc
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,permid_joinkey
                     ,start_date
                     ,end_date
                     ,featuredetailurl
                     ,measure
                     ,eventoffset
                     ,geogstate
                     ,xwalk_huc12
                     ,navigable
                     ,network_distancekm
                     ,network_flowtimeday
                     ,shape
                  )
                  SELECT
                   int_wqp_eventtype
                  ,b.permanent_identifier
                  ,b.eventdate
                  ,b.reachcode
                  ,b.reachsmdate
                  ,'MR' AS reachresolution
                  ,b.feature_permanent_identifier
                  ,b.featureclassref
                  ,b.source_originator
                  ,b.source_featureid
                  ,b.source_featureid2
                  ,b.source_datadesc
                  ,b.source_series
                  ,b.source_subdivision
                  ,b.source_joinkey
                  ,b.permid_joinkey
                  ,b.start_date
                  ,b.end_date
                  ,b.featuredetailurl
                  ,b.measure
                  ,b.eventoffset
                  ,b.geogstate
                  ,b.xwalk_huc12
                  ,b.isnavigable AS navigable
                  ,cipsrv_owld.adjust_point_extent(
                      p_extent_value      => a.network_distancekm
                     ,p_direction         => p_search_type
                     ,p_flowline_amount   => a.lengthkm
                     ,p_flowline_fmeasure => a.fmeasure
                     ,p_flowline_tmeasure => a.tmeasure
                     ,p_event_measure     => b.measure::NUMERIC
                   ) AS network_distancekm
                  ,cipsrv_owld.adjust_point_extent(
                      p_extent_value      => a.network_flowtimeday
                     ,p_direction         => p_search_type
                     ,p_flowline_amount   => a.flowtimeday
                     ,p_flowline_fmeasure => a.fmeasure
                     ,p_flowline_tmeasure => a.tmeasure
                     ,p_event_measure     => b.measure::NUMERIC
                   ) AS network_flowtimeday
                  ,CASE
                   WHEN NOT p_return_linked_data_rad
                   THEN
                     NULL::GEOMETRY
                   ELSE
                     b.shape
                   END AS shape
                  FROM
                  tmp_navigation_results a
                  JOIN
                  cipsrv_owld.wqp_rad_p b
                  ON
                      b.reachcode  = a.reachcode
                  AND b.measure   >= a.fmeasure
                  AND b.measure   <= a.tmeasure
                  WHERE
                      b.reachresolution = 'MR'
                  AND (str_remove_start_permid IS NULL OR b.permid_joinkey != str_remove_start_permid)
                  AND (str_remove_stop_permid  IS NULL OR b.permid_joinkey != str_remove_stop_permid);
                  
                  GET DIAGNOSTICS out_rad_found_count = ROW_COUNT;
                  
                  IF p_push_source_geometry_as_rad
                  THEN
                     UPDATE tmp_rad_points a
                     SET shape = (
                        SELECT
                        b.shape
                        FROM
                        cipsrv_owld.wqp_src_p b
                        WHERE
                        b.permid_joinkey = a.permid_joinkey
                     )
                     WHERE
                     a.eventtype = int_wqp_eventtype;
                  
                  END IF;
                  
                  INSERT INTO tmp_sfid_found(
                      eventtype
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,sfiddetailurl
                     ,rad_event_count
                     ,nearest_rad_distancekm_permid
                     ,nearest_rad_network_distancekm
                     ,nearest_rad_flowtimeday_permid
                     ,nearest_rad_network_flowtimeday
                  )
                  SELECT
                   int_wqp_eventtype
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.sfiddetailurl
                  ,b.rad_event_count
                  ,b.nearest_rad_distancekm_permid
                  ,b.nearest_rad_network_distancekm
                  ,b.nearest_rad_flowtimeday_permid
                  ,b.nearest_rad_network_flowtimeda
                  FROM
                  cipsrv_owld.wqp_sfid a
                  JOIN (
                     /* needs better conversion from oracle */
                     SELECT
                      bb.source_joinkey
                     ,COUNT(*) AS rad_event_count
                     ,MIN(bb.permid_joinkey)      AS nearest_rad_distancekm_permid
                     ,MIN(bb.network_distancekm)  AS nearest_rad_network_distancekm
                     ,CASE
                      WHEN MIN(bb.network_flowtimeday) IS NULL
                      THEN
                        NULL
                      ELSE
                        MIN(bb.permid_joinkey)
                      END AS nearest_rad_flowtimeday_permid
                     ,MIN(bb.network_flowtimeday) AS nearest_rad_network_flowtimeda
                     FROM
                     tmp_rad_points bb
                     WHERE
                     bb.eventtype  = int_wqp_eventtype
                     GROUP BY
                     bb.source_joinkey
                  ) b
                  ON
                  a.source_joinkey = b.source_joinkey;
             
                  GET DIAGNOSTICS out_sfid_found_count = ROW_COUNT;
                  
               END IF;
               
            END IF;
            
         END IF;
   
      END LOOP;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.upstreamdownstream';
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
