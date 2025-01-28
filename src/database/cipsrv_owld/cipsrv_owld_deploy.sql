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
   str_remove_start_permid        VARCHAR;
   str_remove_stop_permid         VARCHAR;
      
BEGIN

   out_return_code := cipsrv_engine.create_navigation_temp_tables();

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
      
      IF p_remove_stop_start_sfids
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
      
      IF p_remove_stop_start_sfids
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
         NULL;
   
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
