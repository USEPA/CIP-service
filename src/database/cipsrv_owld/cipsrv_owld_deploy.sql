--******************************--
----- types/owld_locator_query.sql 

DROP TYPE IF EXISTS cipsrv_owld.owld_locator_query CASCADE;

CREATE TYPE cipsrv_owld.owld_locator_query 
AS(
    program_id          VARCHAR
   ,program_name        VARCHAR
   ,program_short_name  VARCHAR
   ,program_description VARCHAR
   ,program_url         VARCHAR
   ,program_eventtype   INTEGER
   ,program_vintage     DATE
   ,program_resolutions VARCHAR[]
   ,program_precisions  VARCHAR[]
);

ALTER TYPE cipsrv_owld.owld_locator_query OWNER TO cipsrv;

GRANT USAGE ON TYPE cipsrv_owld.owld_locator_query TO PUBLIC;

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
   -- Create tmp_sfid_found temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_sfid_found')
   THEN
      TRUNCATE TABLE tmp_sfid_found;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_sfid_found(
          eventtype                       INTEGER      NOT NULL
         ,source_originator               VARCHAR(130) NOT NULL
         ,source_featureid                VARCHAR(100) NOT NULL
         ,source_featureid2               VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,source_joinkey                  VARCHAR(40)  NOT NULL
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

      CREATE UNIQUE INDEX tmp_sfid_found_pk
      ON tmp_sfid_found(source_joinkey);
      
      CREATE INDEX tmp_sfid_found_01i
      ON tmp_sfid_found(eventtype);
      
      CREATE INDEX tmp_sfid_found_02i
      ON tmp_sfid_found(source_featureid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_cip temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_cip_found')
   THEN
      TRUNCATE TABLE tmp_cip_found;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_cip_found(            
          eventtype                       INTEGER      NOT NULL
         ,cip_joinkey                     VARCHAR(40)  NOT NULL
         ,permid_joinkey                  VARCHAR(40)
         ,source_originator               VARCHAR(130) NOT NULL
         ,source_featureid                VARCHAR(100) NOT NULL
         ,source_featureid2               VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,source_joinkey                  VARCHAR(40)  NOT NULL
         ,start_date                      DATE
         ,end_date                        DATE
         ,cat_joinkey                     VARCHAR(40)  NOT NULL
         ,catchmentstatecode              VARCHAR(2)
         ,nhdplusid                       BIGINT
         ,istribal                        VARCHAR(1)
         ,istribal_areasqkm               NUMERIC
         ,catchmentresolution             VARCHAR(2)
         ,catchmentareasqkm               NUMERIC
         ,xwalk_huc12                     VARCHAR(12)
         ,xwalk_method                    VARCHAR(18)
         ,xwalk_huc12_version             VARCHAR(16)
         ,isnavigable                     VARCHAR(1)
         ,hasvaa                          VARCHAR(1)
         ,issink                          VARCHAR(1)
         ,isheadwater                     VARCHAR(1)
         ,iscoastal                       VARCHAR(1)
         ,isocean                         VARCHAR(1)
         ,isalaskan                       VARCHAR(1)
         ,h3hexagonaddr                   VARCHAR(64)
         ,network_distancekm              NUMERIC
         ,network_flowtimeday             NUMERIC
      );
 
      CREATE UNIQUE INDEX tmp_cip_found_pk
      ON tmp_cip_found(cip_joinkey);
      
      CREATE INDEX tmp_cip_found_01i
      ON tmp_cip_found(eventtype);
      
      CREATE INDEX tmp_cip_found_02i
      ON tmp_cip_found(source_joinkey);
      
      CREATE INDEX tmp_cip_found_03i
      ON tmp_cip_found(source_featureid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Create tmp_huc12 temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_huc12_found')
   THEN
      TRUNCATE TABLE tmp_huc12_found;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_huc12_found(            
          eventtype                       INTEGER      NOT NULL
         ,huc12_joinkey                   VARCHAR(40)
         ,permid_joinkey                  VARCHAR(40)
         ,source_originator               VARCHAR(130) NOT NULL
         ,source_featureid                VARCHAR(100) NOT NULL
         ,source_featureid2               VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,source_joinkey                  VARCHAR(40)  NOT NULL
         ,start_date                      DATE
         ,end_date                        DATE
         ,xwalk_huc12                     VARCHAR(12)  NOT NULL
         ,xwalk_huc12_version             VARCHAR(16)  NOT NULL
         ,xwalk_catresolution             VARCHAR(2)   NOT NULL
         ,xwalk_huc12_areasqkm            NUMERIC
      );
 
      --CREATE UNIQUE INDEX tmp_huc12_found_pk
      --ON tmp_huc12_found(huc12_joinkey);
      
      CREATE INDEX tmp_huc12_found_01i
      ON tmp_huc12_found(eventtype);
      
      CREATE INDEX tmp_huc12_found_02i
      ON tmp_huc12_found(source_joinkey);
      
      CREATE INDEX tmp_huc12_found_03i
      ON tmp_huc12_found(xwalk_huc12);
      
      CREATE INDEX tmp_huc12_found_04i
      ON tmp_huc12_found(source_featureid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Create tmp_src_points temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_src_points')
   THEN
      TRUNCATE TABLE tmp_src_points;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_src_points(            
          eventtype                       INTEGER      NOT NULL
         ,permid_joinkey                  VARCHAR(40)  NOT NULL
         ,source_originator               VARCHAR(130) NOT NULL
         ,source_featureid                VARCHAR(100) NOT NULL
         ,source_featureid2               VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,source_joinkey                  VARCHAR(40)  NOT NULL
         ,start_date                      DATE
         ,end_date                        DATE
         ,featuredetailurl                VARCHAR(255)
         ,orderingkey                     NUMERIC
         ,shape                           GEOMETRY         
      );

      CREATE UNIQUE INDEX tmp_src_points_pk
      ON tmp_src_points(permid_joinkey);
      
      CREATE INDEX tmp_src_points_01i
      ON tmp_src_points(eventtype);
      
      CREATE INDEX tmp_src_points_02i
      ON tmp_src_points(source_joinkey);
      
      CREATE INDEX tmp_src_points_03i
      ON tmp_src_points(source_featureid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Create tmp_src_lines temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_src_lines')
   THEN
      TRUNCATE TABLE tmp_src_lines;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_src_lines(            
          eventtype                       INTEGER      NOT NULL
         ,permid_joinkey                  VARCHAR(40)  NOT NULL
         ,source_originator               VARCHAR(130) NOT NULL
         ,source_featureid                VARCHAR(100) NOT NULL
         ,source_featureid2               VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,source_joinkey                  VARCHAR(40)  NOT NULL
         ,start_date                      DATE
         ,end_date                        DATE
         ,featuredetailurl                VARCHAR(255)
         ,lengthkm                        NUMERIC
         ,orderingkey                     NUMERIC
         ,shape                           GEOMETRY         
      );

      CREATE UNIQUE INDEX tmp_src_lines_pk
      ON tmp_src_lines(permid_joinkey);
      
      CREATE INDEX tmp_src_lines_01i
      ON tmp_src_lines(eventtype);
      
      CREATE INDEX tmp_src_lines_02i
      ON tmp_src_lines(source_joinkey);
      
      CREATE INDEX tmp_src_lines_03i
      ON tmp_src_lines(source_featureid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Create tmp_src_areas temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_src_areas')
   THEN
      TRUNCATE TABLE tmp_src_areas;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_src_areas(            
          eventtype                       INTEGER      NOT NULL
         ,permid_joinkey                  VARCHAR(40)  NOT NULL
         ,source_originator               VARCHAR(130) NOT NULL
         ,source_featureid                VARCHAR(100) NOT NULL
         ,source_featureid2               VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,source_joinkey                  VARCHAR(40)  NOT NULL
         ,start_date                      DATE
         ,end_date                        DATE
         ,featuredetailurl                VARCHAR(255)
         ,areasqkm                        NUMERIC
         ,orderingkey                     NUMERIC
         ,shape                           GEOMETRY         
      );

      CREATE UNIQUE INDEX tmp_src_areas_pk
      ON tmp_src_areas(permid_joinkey);
      
      CREATE INDEX tmp_src_areas_01i
      ON tmp_src_areas(eventtype);
      
      CREATE INDEX tmp_src_areas_02i
      ON tmp_src_areas(source_joinkey);
      
      CREATE INDEX tmp_src_areas_03i
      ON tmp_src_areas(source_featureid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Create tmp_rad points temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_rad_points')
   THEN
      TRUNCATE TABLE tmp_rad_points;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_rad_points(            
          eventtype                       INTEGER      NOT NULL
         ,permanent_identifier            VARCHAR(40)  NOT NULL
         ,eventdate                       DATE
         ,reachcode                       VARCHAR(14)
         ,reachsmdate                     DATE
         ,reachresolution                 VARCHAR(2)
         ,feature_permanent_identifier    VARCHAR(40)
         ,featureclassref                 INTEGER
         ,source_originator               VARCHAR(130) NOT NULL
         ,source_featureid                VARCHAR(100) NOT NULL
         ,source_featureid2               VARCHAR(100)
         ,source_datadesc                 VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,source_joinkey                  VARCHAR(40)  NOT NULL
         ,permid_joinkey                  VARCHAR(40)  NOT NULL
         ,start_date                      DATE
         ,end_date                        DATE
         ,featuredetailurl                VARCHAR(255)
         ,measure                         NUMERIC
         ,eventoffset                     NUMERIC
         ,geogstate                       VARCHAR(2)
         ,xwalk_huc12                     VARCHAR(12)
         ,isnavigable                     VARCHAR(1)
         ,network_distancekm              NUMERIC
         ,network_flowtimeday             NUMERIC
         ,shape                           GEOMETRY         
      );

      CREATE UNIQUE INDEX tmp_rad_points_pk
      ON tmp_rad_points(permanent_identifier);
      
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
   -- Step 80
   -- Create tmp_rad lines temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_rad_lines')
   THEN
      TRUNCATE TABLE tmp_rad_lines;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_rad_lines(            
          eventtype                       INTEGER      NOT NULL
         ,permanent_identifier            VARCHAR(40)  NOT NULL
         ,eventdate                       DATE
         ,reachcode                       VARCHAR(14)
         ,reachsmdate                     DATE
         ,reachresolution                 VARCHAR(2)
         ,feature_permanent_identifier    VARCHAR(40)
         ,featureclassref                 INTEGER
         ,source_originator               VARCHAR(130) NOT NULL
         ,source_featureid                VARCHAR(100) NOT NULL
         ,source_featureid2               VARCHAR(100)
         ,source_datadesc                 VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,source_joinkey                  VARCHAR(40)  NOT NULL
         ,permid_joinkey                  VARCHAR(40)  NOT NULL
         ,start_date                      DATE
         ,end_date                        DATE
         ,featuredetailurl                VARCHAR(255)
         ,fmeasure                        NUMERIC
         ,tmeasure                        NUMERIC
         ,eventoffset                     NUMERIC
         ,event_lengthkm                  NUMERIC
         ,geogstate                       VARCHAR(2)
         ,xwalk_huc12                     VARCHAR(12)
         ,isnavigable                     VARCHAR(1)
         ,shape                           GEOMETRY         
      );

      CREATE UNIQUE INDEX tmp_rad_lines_pk
      ON tmp_rad_lines(permanent_identifier);
      
      CREATE INDEX tmp_rad_lines_01i
      ON tmp_rad_lines(eventtype);
      
      CREATE INDEX tmp_rad_lines_02i
      ON tmp_rad_lines(source_joinkey);
      
      CREATE INDEX tmp_rad_lines_03i
      ON tmp_rad_lines(permid_joinkey);
      
      CREATE INDEX tmp_rad_lines_04i
      ON tmp_rad_lines(source_featureid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 90
   -- Create tmp_rad areas temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_rad_areas')
   THEN
      TRUNCATE TABLE tmp_rad_areas;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_rad_areas(            
          eventtype                       INTEGER      NOT NULL
         ,permanent_identifier            VARCHAR(40)  NOT NULL
         ,eventdate                       DATE
         ,reachcode                       VARCHAR(14)
         ,reachsmdate                     DATE
         ,reachresolution                 VARCHAR(2)
         ,feature_permanent_identifier    VARCHAR(40)
         ,featureclassref                 INTEGER
         ,source_originator               VARCHAR(130) NOT NULL
         ,source_featureid                VARCHAR(100) NOT NULL
         ,source_featureid2               VARCHAR(100)
         ,source_datadesc                 VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,source_joinkey                  VARCHAR(40)  NOT NULL
         ,permid_joinkey                  VARCHAR(40)  NOT NULL
         ,start_date                      DATE
         ,end_date                        DATE
         ,featuredetailurl                VARCHAR(255)
         ,event_areasqkm                  NUMERIC
         ,geogstate                       VARCHAR(2)
         ,xwalk_huc12                     VARCHAR(12)
         ,shape                           GEOMETRY         
      );

      CREATE UNIQUE INDEX tmp_rad_areas_pk
      ON tmp_rad_areas(permanent_identifier);
      
      CREATE INDEX tmp_rad_areas_01i
      ON tmp_rad_areas(eventtype);
      
      CREATE INDEX tmp_rad_areas_02i
      ON tmp_rad_areas(source_joinkey);
      
      CREATE INDEX tmp_rad_areas_03i
      ON tmp_rad_areas(permid_joinkey);
      
      CREATE INDEX tmp_rad_areas_04i
      ON tmp_rad_areas(source_featureid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 100
   -- Create tmp_attr temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_attr')
   THEN
      TRUNCATE TABLE tmp_attr;

   ELSE
      CREATE TEMPORARY TABLE tmp_attr(            
          eventtype                       INTEGER      NOT NULL
         ,source_joinkey                  VARCHAR(40)  NOT NULL
         ,source_originator               VARCHAR(130) NOT NULL
         ,source_featureid                VARCHAR(100) NOT NULL
         ,source_featureid2               VARCHAR(100)
         ,source_series                   VARCHAR(100)
         ,source_subdivision              VARCHAR(100)
         ,start_date                      DATE
         ,end_date                        DATE
         ,sfiddetailurl                   VARCHAR(255)
         ,attributes                      JSONB
      );

      CREATE UNIQUE INDEX tmp_attr_pk
      ON tmp_attr(source_joinkey);

      CREATE INDEX tmp_attr_01i
      ON tmp_attr(eventtype);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 110
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

   ELSIF str_direction IN ('DM','DD','PP','PPALL','D')
   THEN
      IF p_flowline_fmeasure = p_event_measure
      THEN
         RETURN p_extent_value;

      END IF;

      num_amount := p_flowline_amount * ((p_event_measure - p_flowline_fmeasure) / (p_flowline_tmeasure - p_flowline_fmeasure));

   ELSE
      RAISE EXCEPTION 'err direction %',str_direction;

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
   ,program_short_name  VARCHAR
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
      program_short_name  := NULL;
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
         EXECUTE 'SELECT a.value_str FROM cipsrv_owld.' || rec.table_name || ' a WHERE UPPER(a.keyword) = ''SHORT_NAME'' LIMIT 1' 
         INTO program_short_name; 
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
      program_resolutions = NULL;
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
      program_precisions = NULL;
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
----- functions/owld_programs_ary.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.owld_programs_ary';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_owld.owld_programs_ary()
RETURNS cipsrv_owld.owld_locator_query[]
VOLATILE
AS $BODY$
DECLARE
   rec     RECORD;
   int_cnt INTEGER;
   rez     cipsrv_owld.owld_locator_query[];
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   int_cnt := 1;
   FOR rec IN SELECT a.* FROM cipsrv_owld.owld_programs() a
   LOOP
      rez[int_cnt] := (
          rec.program_id
         ,rec.program_name
         ,rec.program_short_name
         ,rec.program_description
         ,rec.program_url
         ,rec.program_eventtype
         ,rec.program_vintage
         ,rec.program_resolutions
         ,rec.program_precisions
      );
      int_cnt := int_cnt + 1;
         
   END LOOP;
   
   RETURN rez;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.owld_programs_ary';
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
   ,IN  p_cip_joinkey                   VARCHAR
   ,IN  p_search_direction              VARCHAR
   ,IN  p_reference_catchment_nhdplusid BIGINT
   ,IN  p_reference_reachcode           VARCHAR
   ,IN  p_reference_flowline_nhdplusid  BIGINT
   ,IN  p_reference_hydroseq            BIGINT
   ,IN  p_reference_measure             NUMERIC
   ,IN  p_reference_point               GEOMETRY
   ,IN  p_search_precision              VARCHAR
   ,IN  p_push_rad_for_permid           BOOLEAN
   ,IN  p_return_shape                  BOOLEAN DEFAULT FALSE
   ,IN  p_known_region                  VARCHAR DEFAULT NULL
   ,OUT out_catchment_nhdplusid         BIGINT
   ,OUT out_catchmentstatecode          VARCHAR
   ,OUT out_permanent_identifier        VARCHAR
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
   ,OUT out_cip_joinkey                 VARCHAR
   ,OUT out_cat_joinkey                 VARCHAR
   ,OUT out_linked_data_program         VARCHAR
   ,OUT out_return_code                 NUMERIC
   ,OUT out_status_message              VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                       RECORD;
   str_query_type            VARCHAR;
   ary_programs_abbrvs       VARCHAR[];
   ary_programs_eventtypes   INTEGER[];
   ary_programs_resolutions  VARCHAR[][];
   str_program_abbrv         VARCHAR;
   int_program_eventtype     INTEGER;
   int_count                 INTEGER;
   ary_searchable_programs   cipsrv_owld.owld_locator_query[];
   ary_targeted_programs     cipsrv_owld.owld_locator_query[];
   ary_targeted_programs2    cipsrv_owld.owld_locator_query[];
   str_nhdplus_version       VARCHAR;
   str_search_precision      VARCHAR;
   geom_shape                GEOMETRY;

BEGIN

   out_return_code := 0;

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   IF p_search_precision IS NULL
   THEN      
      str_search_precision := 'CATCHMENT';
   
   ELSE
      IF UPPER(p_search_precision) IN ('CATCHMENT')
      THEN
         str_search_precision := 'CATCHMENT';
      
      ELSIF UPPER(p_search_precision) IN ('REACH','REACHED')
      THEN
         str_search_precision := 'REACH';
      
      ELSE
         out_return_code := -10;
         out_status_message := 'Unknown precision parameter: ' || p_search_precision || '.';
         RETURN;
      
      END IF;
      
   END IF;
   
   IF p_nhdplus_version IS NULL
   OR p_nhdplus_version IN ('MR','nhdplus_m')
   THEN
      str_nhdplus_version := 'MR';
      
   ELSIF p_nhdplus_version IN ('HR','nhdplus_h')
   THEN
      str_nhdplus_version := 'HR';
      
   ELSE
      out_return_code := -10;
      out_status_message := 'Unknown resolution parameter: ' || p_nhdplus_version || '.';
      RETURN;
   
   END IF;

   --------------------------------------------------------------------------
   -- Step 20
   -- Determine basic query types to drive logic
   --------------------------------------------------------------------------
   IF p_cip_joinkey IS NOT NULL
   THEN
      str_query_type := 'cip_joinkey';
      
   ELSIF p_permid_joinkey IS NOT NULL
   THEN
      str_query_type := 'permid_joinkey';
      
   ELSIF p_source_joinkey IS NOT NULL
   THEN
      str_query_type := 'source_joinkey';
      
   ELSIF p_source_featureid IS NOT NULL
   THEN
      IF str_search_precision = 'CATCHMENT'
      THEN
         str_query_type := 'source_featureid CATCHMENT';
         
      ELSIF str_search_precision = 'REACH'
      THEN
         str_query_type := 'source_featureid REACH';
         
      END IF;
      
   ELSE
      out_return_code := -20;
      out_status_message := 'Unable to determine query parameters for OWLD locator search. '
                         || 'Provide either a SFID-combination or permid/source/cat joinkeys.';
      RETURN;

   END IF;
   
   IF str_search_precision = 'REACH'
   AND str_query_type = 'cip_joinkey'
   THEN
      out_return_code := -10;
      out_status_message := 'CIP JoinKey request invalid combined with REACH search precision.';
      RETURN;
   
   END IF;

   --------------------------------------------------------------------------
   -- Step 30
   -- Check for lack of programs to query on system
   --------------------------------------------------------------------------
   ary_searchable_programs := cipsrv_owld.owld_programs_ary();
   
   IF ARRAY_LENGTH(ary_searchable_programs,1) = 0
   THEN
      out_return_code := -30;
      out_status_message := 'No OWLD programs are currently loaded in this CIPSRV system.';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Try to filter down by provided program id if provided
   --------------------------------------------------------------------------
   ary_targeted_programs := NULL;
   IF p_linked_data_program IS NOT NULL
   THEN
      FOR i IN 1 .. ARRAY_LENGTH(ary_searchable_programs,1)
      LOOP
         IF p_linked_data_program = ary_searchable_programs[i].program_id
         OR p_linked_data_program = ary_searchable_programs[i].program_eventtype::VARCHAR
         THEN
            ary_targeted_programs[1] := ary_searchable_programs[i];
   
         END IF;
         
      END LOOP;
      
      IF ary_targeted_programs IS NULL
      OR ARRAY_LENGTH(ary_targeted_programs,1) = 0
      THEN
         out_return_code := -40;
         out_status_message := 'Requested program ' || p_linked_data_program || ' not found on this system.';
         RETURN;
         
      END IF;
      
   ELSE
      ary_targeted_programs := ary_searchable_programs;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Try to filter down by resolution if provided
   --------------------------------------------------------------------------
   ary_targeted_programs2 := NULL;
   IF str_nhdplus_version IS NOT NULL
   THEN
      int_count := 1;
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         IF str_nhdplus_version = ANY(ary_targeted_programs[i].program_resolutions)
         THEN
            ary_targeted_programs2[int_count] := ary_targeted_programs[i];
            ary_targeted_programs2[int_count].program_resolutions := ARRAY[str_nhdplus_version::VARCHAR];
            int_count := int_count + 1;
            
         END IF;
      
      END LOOP;
      
      IF ary_targeted_programs2 IS NULL
      OR ARRAY_LENGTH(ary_targeted_programs2,1) = 0
      THEN
         out_return_code := -50;
         out_status_message := 'Requested resolution ' || str_nhdplus_version || ' excludes all requested programs.';
         RETURN;
         
      END IF;
   
   ELSE
      ary_targeted_programs2 := ary_targeted_programs;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 60
   -- Try to filter down by precision if provided
   --------------------------------------------------------------------------
   ary_targeted_programs := NULL;
   IF str_search_precision IS NOT NULL
   THEN
      int_count := 1;
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs2,1)
      LOOP
         IF LOWER(str_search_precision) = ANY(ary_targeted_programs2[i].program_precisions)
         OR UPPER(str_search_precision) = ANY(ary_targeted_programs2[i].program_precisions)
         THEN
            ary_targeted_programs[int_count] := ary_targeted_programs2[i];
            int_count := int_count + 1;
            
         END IF;
      
      END LOOP;
      
      IF ary_targeted_programs IS NULL
      OR ARRAY_LENGTH(ary_targeted_programs,1) = 0
      THEN
         out_return_code := -60;
         
         IF str_nhdplus_version IS NOT NULL
         THEN
            out_status_message := 'Requested precision ' || str_search_precision || ' for resolution ' || str_nhdplus_version || ' excludes all requested programs.';
            
         ELSE
            out_status_message := 'Requested precision ' || str_search_precision || ' excludes all requested programs.';
         
         END IF;
         
         RETURN;
         
      END IF;
   
   ELSE
      ary_targeted_programs := ary_targeted_programs2;

   END IF;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Start with simple CIP JoinKey request
   --------------------------------------------------------------------------
   IF str_query_type = 'cip_joinkey'
   THEN
      <<outer_loop>>
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         FOR j IN 1 .. ARRAY_LENGTH(ary_targeted_programs[i].program_resolutions,1)
         LOOP
            EXECUTE '
               SELECT
                a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.nhdplusid
               ,a.catchmentstatecode
               ,a.cat_joinkey
               FROM
               cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
               WHERE
                   a.catchmentresolution = $1
               AND a.cip_joinkey = $2         
            ' USING
             ary_targeted_programs[i].program_resolutions[j]
            ,p_cip_joinkey
            INTO
             out_permid_joinkey
            ,out_source_originator
            ,out_source_featureid
            ,out_source_featureid2
            ,out_source_series
            ,out_source_subdivision
            ,out_source_joinkey
            ,out_start_date
            ,out_end_date
            ,out_catchment_nhdplusid
            ,out_catchmentstatecode
            ,out_cat_joinkey;
            
            IF out_permid_joinkey IS NOT NULL
            THEN
               out_nhdplus_version     := ary_targeted_programs[i].program_resolutions[j];
               out_linked_data_program := ary_targeted_programs[i].program_id;
               
               IF p_return_shape
               THEN
                  EXECUTE '
                     SELECT
                     a.shape
                     FROM
                     cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip_geo a
                     WHERE
                     a.cat_joinkey = $1
                  ' USING
                  out_cat_joinkey
                  INTO
                  out_shape;
               
               END IF;
               
            END IF;
            
            IF out_permid_joinkey IS NOT NULL
            THEN
               EXIT outer_loop;
            
            END IF;
            
         END LOOP;
   
      END LOOP;
      
      IF out_permid_joinkey IS NULL
      THEN
         out_return_code := -70;
         out_status_message := 'CIP Joinkey ' || p_cip_joinkey || ' not found in requested program set.';
         RETURN;
      
      END IF;
   
   END IF;

   --------------------------------------------------------------------------
   -- Step 80
   -- Next do PermID JoinKey request
   --------------------------------------------------------------------------
   IF str_query_type = 'permid_joinkey'
   THEN
      <<outer_loop>>
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         EXECUTE '
            SELECT
             a.permid_joinkey
            ,a.source_originator
            ,a.source_featureid
            ,a.source_featureid2
            ,a.source_series
            ,a.source_subdivision
            ,a.source_joinkey
            ,a.start_date
            ,a.end_date
            ,CASE WHEN $1 
             THEN
               a.shape
             ELSE
               CAST(NULL AS GEOMETRY)
             END AS shape 
            FROM
            cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_p a
            WHERE
            a.permid_joinkey = $2
         ' USING
          p_return_shape
         ,p_permid_joinkey
         INTO
          out_permid_joinkey
         ,out_source_originator
         ,out_source_featureid
         ,out_source_featureid2
         ,out_source_series
         ,out_source_subdivision
         ,out_source_joinkey
         ,out_start_date
         ,out_end_date
         ,out_shape;

         IF out_permid_joinkey IS NULL
         THEN
            EXECUTE '
               SELECT
                a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,CASE WHEN $1 
                THEN
                  a.shape
                ELSE
                  CAST(NULL AS GEOMETRY)
                END AS shape 
               FROM
               cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_l a
               WHERE
               a.permid_joinkey = $2
            ' USING
             p_return_shape
            ,p_permid_joinkey
            INTO
             out_permid_joinkey
            ,out_source_originator
            ,out_source_featureid
            ,out_source_featureid2
            ,out_source_series
            ,out_source_subdivision
            ,out_source_joinkey
            ,out_start_date
            ,out_end_date
            ,out_shape;
            
            IF out_permid_joinkey IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,CASE WHEN $1 
                   THEN
                     a.shape
                   ELSE
                     CAST(NULL AS GEOMETRY)
                   END AS shape 
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_a a
                  WHERE
                  a.permid_joinkey = $2
               ' USING
                p_return_shape
               ,p_permid_joinkey
               INTO
                out_permid_joinkey
               ,out_source_originator
               ,out_source_featureid
               ,out_source_featureid2
               ,out_source_series
               ,out_source_subdivision
               ,out_source_joinkey
               ,out_start_date
               ,out_end_date
               ,out_shape;
            
            END IF;

         END IF;
         
         IF out_permid_joinkey IS NOT NULL
         THEN
            out_linked_data_program := ary_targeted_programs[i].program_id;
            
            IF p_push_rad_for_permid
            THEN
               EXECUTE '
                  SELECT
                   a.reachresolution
                  ,a.reachcode
                  ,a.measure
                  ,a.feature_permanent_identifier::BIGINT
                  ,CASE WHEN $1 
                   THEN
                     a.shape
                   ELSE
                     CAST(NULL AS GEOMETRY)
                   END AS shape                
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_rad_p a
                  WHERE
                      a.reachresolution = $2
                  AND a.permid_joinkey  = $3
                  LIMIT 1
               ' USING
                p_return_shape
               ,str_nhdplus_version
               ,out_permid_joinkey
               INTO
                out_nhdplus_version
               ,out_reachcode
               ,out_measure
               ,out_flowline_nhdplusid
               ,out_shape;
            
            END IF;
            
            IF out_nhdplus_version IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.catchmentresolution
                  ,a.nhdplusid
                  ,a.catchmentstatecode
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
                  WHERE
                      a.catchmentresolution = $1
                  AND a.permid_joinkey      = $2
                  LIMIT 1
               ' USING
                str_nhdplus_version
               ,out_permid_joinkey
               INTO
                out_nhdplus_version
               ,out_catchment_nhdplusid
               ,out_catchmentstatecode;
               
               IF out_catchment_nhdplusid IS NULL
               THEN
                  EXECUTE '
                     SELECT
                      a.catchmentresolution
                     ,a.nhdplusid
                     ,a.catchmentstatecode
                     FROM
                     cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
                     WHERE
                         a.catchmentresolution = $1
                     AND a.source_joinkey      = $2
                     LIMIT 1
                  ' USING
                   str_nhdplus_version
                  ,out_source_joinkey
                  INTO
                   out_nhdplus_version
                  ,out_catchment_nhdplusid
                  ,out_catchmentstatecode;
               
               END IF;
               
            END IF;

         END IF;
         
         IF out_nhdplus_version IS NOT NULL
         THEN
            EXIT outer_loop;
         
         END IF;
  
      END LOOP;
      
      IF out_permid_joinkey IS NULL
      THEN
         out_return_code := -80;
         out_status_message := 'PermID Joinkey ' || p_permid_joinkey || ' not found in requested program set.';
         RETURN;
      
      END IF;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 90
   -- Next try naked Source JoinKey request
   --------------------------------------------------------------------------
   IF str_query_type = 'source_joinkey'
   THEN
      <<outer_loop>>
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         EXECUTE '
            SELECT
             a.permid_joinkey
            ,a.source_originator
            ,a.source_featureid
            ,a.source_featureid2
            ,a.source_series
            ,a.source_subdivision
            ,a.source_joinkey
            ,a.start_date
            ,a.end_date
            ,CASE WHEN $1 
             THEN
               a.shape
             ELSE
               CAST(NULL AS GEOMETRY)
             END AS shape 
            FROM
            cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_p a
            WHERE
            a.source_joinkey = $2
            LIMIT 1
         ' USING
          p_return_shape
         ,p_source_joinkey
         INTO
          out_permid_joinkey
         ,out_source_originator
         ,out_source_featureid
         ,out_source_featureid2
         ,out_source_series
         ,out_source_subdivision
         ,out_source_joinkey
         ,out_start_date
         ,out_end_date
         ,out_shape;

         IF out_permid_joinkey IS NULL
         THEN
            EXECUTE '
               SELECT
                a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,CASE WHEN $1 
                THEN
                  a.shape
                ELSE
                  CAST(NULL AS GEOMETRY)
                END AS shape 
               FROM
               cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_l a
               WHERE
               a.source_joinkey = $2
               LIMIT 1
            ' USING
             p_return_shape
            ,p_source_joinkey
            INTO
             out_permid_joinkey
            ,out_source_originator
            ,out_source_featureid
            ,out_source_featureid2
            ,out_source_series
            ,out_source_subdivision
            ,out_source_joinkey
            ,out_start_date
            ,out_end_date
            ,out_shape;
            
            IF out_permid_joinkey IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,CASE WHEN $1 
                   THEN
                     a.shape
                   ELSE
                     CAST(NULL AS GEOMETRY)
                   END AS shape 
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_a a
                  WHERE
                  a.source_joinkey = $2
                  LIMIT 1
               ' USING
                p_return_shape
               ,p_source_joinkey
               INTO
                out_permid_joinkey
               ,out_source_originator
               ,out_source_featureid
               ,out_source_featureid2
               ,out_source_series
               ,out_source_subdivision
               ,out_source_joinkey
               ,out_start_date
               ,out_end_date
               ,out_shape;
            
            END IF;

         END IF;

         IF out_source_joinkey IS NOT NULL
         THEN
            out_linked_data_program := ary_targeted_programs[i].program_id;

            IF p_push_rad_for_permid
            THEN
               EXECUTE '
                  SELECT
                   a.reachresolution
                  ,a.reachcode
                  ,a.measure
                  ,a.feature_permanent_identifier::BIGINT
                  ,CASE WHEN $1 
                   THEN
                     a.shape
                   ELSE
                     CAST(NULL AS GEOMETRY)
                   END AS shape                
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_rad_p a
                  WHERE
                      a.reachresolution = $2
                  AND a.permid_joinkey  = $3
                  LIMIT 1
               ' USING
                p_return_shape
               ,str_nhdplus_version
               ,out_permid_joinkey
               INTO
                out_nhdplus_version
               ,out_reachcode
               ,out_measure
               ,out_flowline_nhdplusid
               ,geom_shape;
               
               IF geom_shape IS NOT NULL
               THEN
                  out_shape := geom_shape;

               END IF;
               
            END IF;

            IF out_nhdplus_version IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.reachresolution
                  ,a.reachcode
                  ,a.measure
                  ,a.feature_permanent_identifier::BIGINT
                  ,CASE WHEN $1 
                   THEN
                     a.shape
                   ELSE
                     CAST(NULL AS GEOMETRY)
                   END AS shape                
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_rad_p a
                  WHERE
                      a.reachresolution = $2
                  AND a.source_joinkey  = $3
                  LIMIT 1
               ' USING
                p_return_shape
               ,str_nhdplus_version
               ,out_source_joinkey
               INTO
                out_nhdplus_version
               ,out_reachcode
               ,out_measure
               ,out_flowline_nhdplusid
               ,geom_shape;
               
               IF geom_shape IS NOT NULL
               THEN
                  out_shape := geom_shape;

               END IF;

            END IF;
         
            IF out_nhdplus_version IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.catchmentresolution
                  ,a.nhdplusid
                  ,a.catchmentstatecode
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
                  WHERE
                      a.catchmentresolution = $1
                  AND a.permid_joinkey      = $2
                  LIMIT 1
               ' USING
                str_nhdplus_version
               ,out_permid_joinkey
               INTO
                out_nhdplus_version
               ,out_catchment_nhdplusid
               ,out_catchmentstatecode;
               
            END IF;
               
            IF out_catchment_nhdplusid IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.catchmentresolution
                  ,a.nhdplusid
                  ,a.catchmentstatecode
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
                  WHERE
                      a.catchmentresolution = $1
                  AND a.source_joinkey      = $2
                  LIMIT 1
               ' USING
                str_nhdplus_version
               ,out_source_joinkey
               INTO
                out_nhdplus_version
               ,out_catchment_nhdplusid
               ,out_catchmentstatecode;
            
            END IF;
            
         END IF;
         
         IF out_nhdplus_version IS NOT NULL
         THEN
            EXIT outer_loop;
         
         END IF;
         
      END LOOP;
      
      IF out_permid_joinkey IS NULL
      THEN
         out_return_code := -90;
         out_status_message := 'Source Joinkey ' || p_source_joinkey || ' not found in requested program set.';
         RETURN;
      
      END IF;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 100
   -- next query for catchment by sfid parts
   --------------------------------------------------------------------------
   IF str_query_type = 'source_featureid CATCHMENT'
   THEN
      <<outer_loop>>
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         FOR j IN 1 .. ARRAY_LENGTH(ary_targeted_programs[i].program_resolutions,1)
         LOOP
            EXECUTE '
               SELECT
                a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.nhdplusid
               ,a.catchmentstatecode
               ,a.cat_joinkey
               FROM
               cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
               WHERE
                   a.catchmentresolution = $1
               AND ($2  IS NULL OR a.source_originator = $3)
               AND ($4  IS NULL OR a.source_featureid  = $5)
               AND ($6  IS NULL OR a.source_featureid2 = $7)
               AND ($8  IS NULL OR a.source_series     = $9)
               AND ($10 IS NULL OR a.start_date        = $11)
               AND ($12 IS NULL OR a.end_date          = $13)               
            ' USING
             ary_targeted_programs[i].program_resolutions[j]
            ,p_source_originator
            ,p_source_originator
            ,p_source_featureid
            ,p_source_featureid
            ,p_source_featureid2
            ,p_source_featureid2
            ,p_source_series
            ,p_source_series
            ,p_start_date
            ,p_start_date
            ,p_end_date
            ,p_end_date
            INTO
             out_permid_joinkey
            ,out_source_originator
            ,out_source_featureid
            ,out_source_featureid2
            ,out_source_series
            ,out_source_subdivision
            ,out_source_joinkey
            ,out_start_date
            ,out_end_date
            ,out_catchment_nhdplusid
            ,out_catchmentstatecode
            ,out_cat_joinkey;
            
            IF out_permid_joinkey IS NOT NULL
            THEN
               out_nhdplus_version     := ary_targeted_programs[i].program_resolutions[j];
               out_linked_data_program := ary_targeted_programs[i].program_id;
               
               IF p_return_shape
               THEN
                  EXECUTE '
                     SELECT
                     a.shape
                     FROM
                     cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip_geo a
                     WHERE
                     a.cat_joinkey = $1
                  ' USING
                  out_cat_joinkey
                  INTO
                  out_shape;
               
               END IF;
               
               RETURN;
               
            END IF;
         
            IF out_permid_joinkey IS NOT NULL
            THEN
               EXIT outer_loop;
            
            END IF;
            
         END LOOP;
   
      END LOOP;
      
      IF out_permid_joinkey IS NULL
      THEN
         out_return_code := -100;
         out_status_message := 'SFID query returned no results in requested CIP indexed program set.';
         RETURN;
      
      END IF;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 110
   -- Finally query for reached by sfid parts
   --------------------------------------------------------------------------
   IF str_query_type = 'source_featureid REACH'
   THEN
      <<outer_loop>>
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         FOR j IN 1 .. ARRAY_LENGTH(ary_targeted_programs[i].program_resolutions,1)
         LOOP
            EXECUTE '
               SELECT
                a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.feature_permanent_identifier::BIGINT
               ,a.geogstate
               ,a.geogstate || a.feature_permanent_identifier
               ,CASE WHEN $1 
                THEN
                  a.shape
                ELSE
                  CAST(NULL AS GEOMETRY)
                END AS shape
               FROM
               cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_rad_p a
               WHERE
                   a.reachresolution = $2
               AND ($3  IS NULL OR a.source_originator = $4)
               AND ($5  IS NULL OR a.source_featureid  = $6)
               AND ($7  IS NULL OR a.source_featureid2 = $8)
               AND ($9  IS NULL OR a.source_series     = $10)
               AND ($11 IS NULL OR a.start_date        = $12)
               AND ($13 IS NULL OR a.end_date          = $14)               
            ' USING
             p_return_shape
            ,ary_targeted_programs[i].program_resolutions[j]
            ,p_source_originator
            ,p_source_originator
            ,p_source_featureid
            ,p_source_featureid
            ,p_source_featureid2
            ,p_source_featureid2
            ,p_source_series
            ,p_source_series
            ,p_start_date
            ,p_start_date
            ,p_end_date
            ,p_end_date
            INTO
             out_permid_joinkey
            ,out_source_originator
            ,out_source_featureid
            ,out_source_featureid2
            ,out_source_series
            ,out_source_subdivision
            ,out_source_joinkey
            ,out_start_date
            ,out_end_date
            ,out_catchment_nhdplusid
            ,out_catchmentstatecode
            ,out_cat_joinkey;
            
            IF out_permid_joinkey IS NOT NULL
            THEN
               out_nhdplus_version     := ary_targeted_programs[i].program_resolutions[j];
               out_linked_data_program := ary_targeted_programs[i].program_id;
               
               RETURN;
             
            END IF;
         
            IF out_permid_joinkey IS NOT NULL
            THEN
               EXIT outer_loop;
            
            END IF;
            
         END LOOP;
   
      END LOOP;
      
      IF out_permid_joinkey IS NULL
      THEN
         out_return_code := -110;
         out_status_message := 'SFID query returned no results in requested reached indexed program set.';
         RETURN;
      
      END IF;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 120
   -- Error out with no results
   --------------------------------------------------------------------------
   IF out_permid_joinkey IS NULL
   THEN
      out_return_code := -120;
      out_status_message := 'Inputs failed to query.';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 130
   -- Try to value add
   --------------------------------------------------------------------------
   IF  out_nhdplus_version IS NOT NULL
   AND out_catchment_nhdplusid IS NOT NULL
   AND out_measure IS NULL
   THEN
      rec := cipsrv_engine.get_measure(
          p_nhdplus_version  := out_nhdplus_version
         ,p_direction        := p_search_direction
         ,p_nhdplusid        := out_catchment_nhdplusid
      );
      
      out_measure              := rec.out_measure;
      out_permanent_identifier := rec.out_permanent_identifier;
      out_reachcode            := rec.out_reachcode;
      out_hydroseq             := rec.out_hydroseq;
      
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
   ,IN  p_start_cip_joinkey             VARCHAR
   ,IN  p_start_linked_data_program     VARCHAR
   ,IN  p_start_search_precision        VARCHAR
   ,IN  p_start_push_rad_for_permid     BOOLEAN
      
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
   ,IN  p_stop_cip_joinkey              VARCHAR
   ,IN  p_stop_linked_data_program      VARCHAR
   ,IN  p_stop_search_precision         VARCHAR
   ,IN  p_stop_push_rad_for_permid      BOOLEAN
   
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
   ,OUT out_start_linked_data_program   VARCHAR
   ,OUT out_grid_srid                   INTEGER
   ,OUT out_stop_nhdplusid              BIGINT
   ,OUT out_stop_measure                NUMERIC
   ,OUT out_stop_linked_data_program    VARCHAR
   ,OUT out_flowline_count              INTEGER
   ,OUT out_catchment_count             INTEGER
   ,OUT out_cip_found_count             INTEGER
   ,OUT out_huc12_found_count           INTEGER
   ,OUT out_rad_found_count             INTEGER
   ,OUT out_sfid_found_count            INTEGER
   ,OUT out_src_found_count             INTEGER
   ,OUT out_attr_found_count            INTEGER
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
   str_start_nhdplus_version      VARCHAR;
   str_start_source_joinkey       VARCHAR;
   str_start_permid_joinkey       VARCHAR;
   str_start_cip_joinkey          VARCHAR;
   str_start_search_precision     VARCHAR := p_start_search_precision;
   str_start_linked_data_program  VARCHAR;
   
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
   str_stop_nhdplus_version       VARCHAR;
   str_stop_source_joinkey        VARCHAR;
   str_stop_permid_joinkey        VARCHAR;
   str_stop_cip_joinkey           VARCHAR;
   str_stop_search_precision      VARCHAR := p_stop_search_precision;
   str_stop_linked_data_program   VARCHAR;
   
   str_search_precision           VARCHAR := UPPER(p_search_precision);   
   int_attains_eventtype          INTEGER := 10033;
   int_frspub_eventtype           INTEGER := 10028;
   int_npdes_eventtype            INTEGER := 10015;
   int_wqp_eventtype              INTEGER := 10032;
   boo_remove_stop_start_sfids    BOOLEAN;
   str_remove_start_permid        VARCHAR(40);
   str_remove_stop_permid         VARCHAR(40);
   str_resolution_abbrev          VARCHAR(2);
   str_owld                       VARCHAR(64);
   int_count                      INTEGER;
   int_owld                       INTEGER;
   boo_catchment_okay             BOOLEAN;
   boo_reach_okay                 BOOLEAN;
   str_joinkey_fix                VARCHAR;
      
BEGIN

   out_return_code := cipsrv_engine.create_navigation_temp_tables();
   out_return_code := cipsrv_engine.create_delineation_temp_tables();
   out_return_code := cipsrv_owld.create_updn_temp_tables();

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   out_return_code    := 0;
   
   IF str_nhdplus_version IS NULL
   OR str_nhdplus_version IN ('MR','nhdplus_m')
   THEN
      str_nhdplus_version := 'MR';
      
   ELSIF str_nhdplus_version IN ('HR','nhdplus_h')
   THEN
      str_nhdplus_version := 'HR';
      
   ELSE
      out_return_code    := -10;
      out_status_message := 'Invalid resolution value ' || str_nhdplus_version || '.';
   
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
   
   out_flowline_count  := 0;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Convert any program locations to network locations
   ----------------------------------------------------------------------------
   IF p_start_linked_data_program IS NOT NULL
   OR p_start_permid_joinkey      IS NOT NULL
   OR p_start_source_joinkey      IS NOT NULL
   OR p_start_cip_joinkey         IS NOT NULL
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
         ,p_cip_joinkey                   := p_start_cip_joinkey
         ,p_search_direction              := str_search_type
         ,p_reference_catchment_nhdplusid := NULL
         ,p_reference_reachcode           := NULL
         ,p_reference_flowline_nhdplusid  := NULL
         ,p_reference_hydroseq            := NULL
         ,p_reference_measure             := NULL
         ,p_reference_point               := NULL
         ,p_search_precision              := str_start_search_precision
         ,p_known_region                  := str_known_region
         ,p_push_rad_for_permid           := p_start_push_rad_for_permid
      );
      int_start_catnhdplusid         := rec.out_catchment_nhdplusid;
      str_start_permanent_identifier := rec.out_permanent_identifier;
      str_start_reachcode            := rec.out_reachcode;
      str_start_nhdplus_version      := rec.out_nhdplus_version;
      
      IF rec.out_flowline_nhdplusid IS NOT NULL
      THEN
         int_start_nhdplusid         := rec.out_flowline_nhdplusid;
         
      ELSIF rec.out_catchment_nhdplusid IS NOT NULL
      THEN
         int_start_nhdplusid         := rec.out_catchment_nhdplusid;
      
      ELSE
         IF rec.out_return_code != 0
         THEN
            out_return_code          := rec.out_return_code;
            out_status_message       := rec.out_status_message;
     
         ELSE
            out_return_code          := -20;
            out_status_message       := 'Unable to determine network location for start event';

         END IF;
         
         RETURN;
         
      END IF;
      
      str_start_linked_data_program  := rec.out_linked_data_program;
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
      str_start_cip_joinkey          := rec.out_cip_joinkey;
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
   OR p_stop_permid_joinkey      IS NOT NULL
   OR p_stop_source_joinkey      IS NOT NULL
   OR p_stop_cip_joinkey         IS NOT NULL
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
         ,p_cip_joinkey                   := p_stop_cip_joinkey
         ,p_search_direction              := str_search_type
         ,p_reference_catchment_nhdplusid := NULL
         ,p_reference_reachcode           := NULL
         ,p_reference_flowline_nhdplusid  := NULL
         ,p_reference_hydroseq            := NULL
         ,p_reference_measure             := NULL
         ,p_reference_point               := NULL
         ,p_search_precision              := str_stop_search_precision
         ,p_known_region                  := str_known_region
         ,p_push_rad_for_permid           := TRUE
      );
      int_stop_catnhdplusid          := rec.out_catchment_nhdplusid;
      str_stop_permanent_identifier  := rec.out_permanent_identifier;
      str_stop_reachcode             := rec.out_reachcode;
      str_stop_nhdplus_version       := rec.out_nhdplus_version;
      
      IF rec.out_flowline_nhdplusid IS NOT NULL
      THEN
         int_stop_nhdplusid          := rec.out_flowline_nhdplusid;
         
      ELSIF rec.out_catchment_nhdplusid IS NOT NULL
      THEN
         int_stop_nhdplusid          := rec.out_catchment_nhdplusid;
      
      ELSE
         IF rec.out_return_code != 0
         THEN
            out_return_code          := rec.out_return_code;
            out_status_message       := rec.out_status_message;
     
         ELSE
            out_return_code          := -20;
            out_status_message       := 'Unable to determine network location for stop event';

         END IF;
         
         RETURN;
         
      END IF;
      
      str_stop_linked_data_program   := rec.out_linked_data_program;
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
      str_stop_cip_joinkey           := rec.out_cip_joinkey;
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
   
   out_start_nhdplusid            := int_start_nhdplusid;
   out_start_measure              := num_start_measure;
   out_start_permanent_identifier := str_start_permanent_identifier;
   out_start_linked_data_program  := str_start_linked_data_program;
   out_stop_nhdplusid             := int_stop_nhdplusid;
   out_stop_measure               := num_stop_measure;
   out_stop_linked_data_program   := str_stop_linked_data_program;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Call the navigation engine
   ----------------------------------------------------------------------------
   IF str_nhdplus_version IN ('MR','nhdplus_m')
   THEN
      str_resolution_abbrev := 'MR';
      
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
         ,p_return_flowline_details     := TRUE
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
      
      IF p_return_catchments
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,areasqkm
            ,orderingkey
            ,shape
         )
         SELECT
          a.nhdplusid
         ,a.sourcefc
         ,a.areasqkm
         ,b.network_distancekm
         ,a.shape
         FROM
         cipsrv_nhdplus_m.nhdpluscatchment a
         JOIN
         tmp_navigation_results b
         ON
         a.nhdplusid = b.nhdplusid;
         
         GET DIAGNOSTICS int_count = ROW_COUNT;
         
         IF out_catchment_count IS NULL
         THEN
            out_catchment_count := int_count;
            
         ELSE
            out_catchment_count := out_catchment_count + int_count;
         
         END IF;
      
      END IF;
      
   ELSIF str_nhdplus_version IN ('HR','nhdplus_h')
   THEN
      str_resolution_abbrev := 'HR';
      
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
         ,p_return_flowline_details     := TRUE
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
      
      IF p_return_catchments
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,areasqkm
            ,orderingkey
            ,shape
         )
         SELECT
          a.nhdplusid
         ,a.sourcefc
         ,a.areasqkm
         ,b.network_distancekm
         ,a.shape
         FROM
         cipsrv_nhdplus_h.nhdpluscatchment a
         JOIN
         tmp_navigation_results b
         ON
         a.nhdplusid = b.nhdplusid;
         
         GET DIAGNOSTICS int_count = ROW_COUNT;
         
         IF out_catchment_count IS NULL
         THEN
            out_catchment_count := int_count;
            
         ELSE
            out_catchment_count := out_catchment_count + int_count;
         
         END IF;
         
      END IF;
   
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
   IF  p_linked_data_search_list IS NOT NULL
   AND array_length(p_linked_data_search_list,1) > 0
   THEN

      FOR i IN 1 .. array_length(p_linked_data_search_list,1)
      LOOP
      
         boo_catchment_okay := FALSE;
         boo_reach_okay     := FALSE;
         
         IF p_linked_data_search_list[i] IN (int_frspub_eventtype::VARCHAR,'FRSPUB')
         THEN
            str_owld := 'cipsrv_owld.frspub';
            int_owld := int_frspub_eventtype;
            
            boo_catchment_okay := TRUE;
            boo_reach_okay     := TRUE;
            str_joinkey_fix    := 'permid_joinkey';
            
         ELSIF p_linked_data_search_list[i] IN (int_npdes_eventtype::VARCHAR,'NPDES')
         THEN
            str_owld := 'cipsrv_owld.npdes';
            int_owld := int_npdes_eventtype;
            
            boo_catchment_okay := TRUE;
            boo_reach_okay     := TRUE;
            str_joinkey_fix    := 'permid_joinkey';
            
         ELSIF p_linked_data_search_list[i] IN (int_wqp_eventtype::VARCHAR,'WQP')
         THEN
            str_owld := 'cipsrv_owld.wqp';
            int_owld := int_wqp_eventtype;
            
            boo_catchment_okay := TRUE;
            boo_reach_okay     := TRUE;
            str_joinkey_fix    := 'permid_joinkey';
            
         ELSIF p_linked_data_search_list[i] IN (int_attains_eventtype::VARCHAR,'ATTAINS')
         THEN
            str_owld := 'cipsrv_owld.attains';
            int_owld := int_attains_eventtype;
            
            boo_catchment_okay := TRUE;
            boo_reach_okay     := FALSE;
            str_joinkey_fix    := 'source_joinkey';
            
         ELSE
            CONTINUE;
         
         END IF;

         --##################################################################--
         IF  str_search_precision IN ('REACH','REACHED') 
         AND boo_reach_okay
         THEN
            -------------------------------------------------------------------
            EXECUTE '
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
                  ,isnavigable
                  ,network_distancekm
                  ,network_flowtimeday
                  ,shape
               )
               SELECT
                $1
               ,b.permanent_identifier
               ,b.eventdate
               ,b.reachcode
               ,b.reachsmdate
               ,$2 AS reachresolution
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
               ,b.isnavigable
               ,cipsrv_owld.adjust_point_extent(
                   p_extent_value      => a.network_distancekm
                  ,p_direction         => $3
                  ,p_flowline_amount   => a.lengthkm
                  ,p_flowline_fmeasure => a.fmeasure
                  ,p_flowline_tmeasure => a.tmeasure
                  ,p_event_measure     => b.measure::NUMERIC
                ) AS network_distancekm
               ,cipsrv_owld.adjust_point_extent(
                   p_extent_value      => a.network_flowtimeday
                  ,p_direction         => $4
                  ,p_flowline_amount   => a.flowtimeday
                  ,p_flowline_fmeasure => a.fmeasure
                  ,p_flowline_tmeasure => a.tmeasure
                  ,p_event_measure     => b.measure::NUMERIC
                ) AS network_flowtimeday
               ,CASE
                WHEN NOT $5
                THEN
                  NULL::GEOMETRY
                ELSE
                  b.shape
                END AS shape
               FROM
               tmp_navigation_results a
               JOIN
               ' || str_owld || '_rad_p b
               ON
                   b.reachcode  = a.reachcode
               AND b.measure   >= a.fmeasure
               AND b.measure   <= a.tmeasure
               WHERE
                   b.reachresolution = $6
               AND ($7 IS NULL OR b.permid_joinkey != $8)
               AND ($9 IS NULL OR b.permid_joinkey != $10);
            ' 
            USING
             int_owld
            ,str_resolution_abbrev
            ,p_search_type
            ,p_search_type
            ,p_return_linked_data_rad
            ,str_resolution_abbrev
            ,str_remove_start_permid
            ,str_remove_start_permid
            ,str_remove_stop_permid
            ,str_remove_stop_permid;

            GET DIAGNOSTICS int_count = ROW_COUNT;           

            IF out_rad_found_count IS NULL
            OR out_rad_found_count = 0
            THEN
               out_rad_found_count := int_count;
               
            ELSE
               out_rad_found_count := out_rad_found_count + int_count;
            
            END IF;

            -------------------------------------------------------------------
            IF p_push_source_geometry_as_rad
            AND int_count > 0
            THEN
               EXECUTE '
                  UPDATE tmp_rad_points a
                  SET shape = (
                     SELECT
                     b.shape
                     FROM
                     ' || str_owld || '_src_p b
                     WHERE
                     b.permid_joinkey = a.permid_joinkey
                  )
                  WHERE
                  a.eventtype = $1
               ' USING
               int_owld;
            
            END IF;

            -------------------------------------------------------------------
            IF p_return_linked_data_source
            THEN
               EXECUTE '
                  INSERT INTO tmp_src_points(
                      eventtype
                     ,permid_joinkey
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,featuredetailurl
                     ,orderingkey
                     ,shape
                  )
                  SELECT
                   $1
                  ,a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.featuredetailurl
                  ,b.network_distancekm
                  ,a.shape
                  FROM
                  ' || str_owld || '_src_p a
                  JOIN
                  tmp_rad_points b
                  ON
                  a.' || str_joinkey_fix || ' = b.' || str_joinkey_fix || '
                  ON CONFLICT DO NOTHING
               ' USING
               int_owld;

               GET DIAGNOSTICS int_count = ROW_COUNT;
               
               IF out_src_found_count IS NULL
               OR out_src_found_count = 0
               THEN
                  out_src_found_count := int_count;
                  
               ELSE
                  out_src_found_count := out_src_found_count + int_count;
                  
               END IF;

            END IF;
            
            EXECUTE '
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
                $1
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
               ' || str_owld || '_sfid a
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
                  bb.eventtype = $2
                  GROUP BY
                  bb.source_joinkey
               ) b
               ON
               a.source_joinkey = b.source_joinkey
            ' USING
             int_owld
            ,int_owld;
                
            GET DIAGNOSTICS int_count = ROW_COUNT;
       
            IF out_sfid_found_count IS NULL
            OR out_sfid_found_count = 0
            THEN
               out_sfid_found_count := int_count;
               
            ELSE
               out_sfid_found_count := out_sfid_found_count + int_count;
            
            END IF;
            
         --##################################################################--
         ELSIF str_search_precision IN ('CATCHMENT') 
         AND boo_catchment_okay
         THEN
            -------------------------------------------------------------------
            -------------------------------------------------------------------
            EXECUTE '
               INSERT INTO tmp_cip_found(
                   eventtype
                  ,cip_joinkey
                  ,permid_joinkey
                  ,source_originator
                  ,source_featureid
                  ,source_featureid2
                  ,source_series
                  ,source_subdivision
                  ,source_joinkey
                  ,start_date
                  ,end_date
                  ,cat_joinkey
                  ,catchmentstatecode
                  ,nhdplusid
                  ,istribal
                  ,istribal_areasqkm
                  ,catchmentresolution
                  ,catchmentareasqkm
                  ,xwalk_huc12
                  ,xwalk_method
                  ,xwalk_huc12_version
                  ,isnavigable
                  ,hasvaa
                  ,issink
                  ,isheadwater
                  ,iscoastal
                  ,isocean
                  ,isalaskan
                  ,h3hexagonaddr
                  ,network_distancekm
                  ,network_flowtimeday
               )
               SELECT
                $1
               ,a.cip_joinkey
               ,a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.cat_joinkey
               ,a.catchmentstatecode
               ,a.nhdplusid
               ,a.istribal
               ,a.istribal_areasqkm
               ,a.catchmentresolution
               ,a.catchmentareasqkm
               ,a.xwalk_huc12
               ,a.xwalk_method
               ,a.xwalk_huc12_version
               ,a.isnavigable
               ,a.hasvaa
               ,a.issink
               ,a.isheadwater
               ,a.iscoastal
               ,a.isocean
               ,a.isalaskan
               ,a.h3hexagonaddr
               ,b.network_distancekm
               ,b.network_flowtimeday
               FROM
               ' || str_owld || '_cip a
               JOIN (
                  SELECT
                   bb.nhdplusid
                  ,MIN(bb.network_distancekm)  AS network_distancekm
                  ,MIN(bb.network_flowtimeday) AS network_flowtimeday
                  FROM 
                  tmp_navigation_results bb
                  GROUP BY
                  bb.nhdplusid
               ) b
               ON
               b.nhdplusid = a.nhdplusid
               WHERE
                   a.catchmentresolution = $2
               AND ($3 IS NULL OR a.permid_joinkey != $4)
               AND ($5 IS NULL OR a.permid_joinkey != $6);
            ' USING
             int_owld
            ,str_resolution_abbrev
            ,str_remove_start_permid
            ,str_remove_start_permid
            ,str_remove_stop_permid
            ,str_remove_stop_permid;
            
            GET DIAGNOSTICS int_count = ROW_COUNT;

            IF out_cip_found_count IS NULL
            OR out_cip_found_count = 0
            THEN
               out_cip_found_count := int_count;
               
            ELSE
               out_cip_found_count := out_cip_found_count + int_count;
            
            END IF;
            
            -------------------------------------------------------------------
            -------------------------------------------------------------------
            IF p_return_linked_data_huc12
            THEN
               EXECUTE '
                  INSERT INTO tmp_huc12_found(            
                      eventtype
                     ,huc12_joinkey
                     ,permid_joinkey
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,xwalk_huc12
                     ,xwalk_huc12_version
                     ,xwalk_catresolution
                     ,xwalk_huc12_areasqkm
                  )
                  SELECT
                   $1
                  ,NULL
                  ,a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.xwalk_huc12
                  ,a.xwalk_huc12_version
                  ,a.xwalk_catresolution
                  ,a.xwalk_huc12_areasqkm
                  FROM
                  ' || str_owld || '_huc12 a
                  WHERE
                      a.xwalk_catresolution = $2
                  AND EXISTS (
                     SELECT
                     1
                     FROM
                     tmp_cip_found bb
                     WHERE
                     bb.' || str_joinkey_fix || ' = a.' || str_joinkey_fix || '
                  ) 
               ' USING
                int_owld
               ,str_resolution_abbrev;
               
               GET DIAGNOSTICS int_count = ROW_COUNT;
               
               IF out_huc12_found_count IS NULL
               OR out_huc12_found_count = 0
               THEN
                  out_huc12_found_count := int_count;
                  
               ELSE
                  out_huc12_found_count := out_huc12_found_count + int_count;
                  
               END IF;
            
            END IF;

            -------------------------------------------------------------------
            -------------------------------------------------------------------
            IF p_return_linked_data_source
            THEN
               ----------------------------------------------------------------
               EXECUTE '
                  INSERT INTO tmp_src_points(
                      eventtype
                     ,permid_joinkey
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,featuredetailurl
                     ,orderingkey
                     ,shape
                  )
                  SELECT
                   $1
                  ,a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.featuredetailurl
                  ,b.network_distancekm
                  ,a.shape
                  FROM
                  ' || str_owld || '_src_p a
                  JOIN (
                     SELECT
                      bb.' || str_joinkey_fix || '
                     ,MIN(bb.network_distancekm) AS network_distancekm
                     FROM
                     tmp_cip_found bb
                     GROUP BY
                     bb.' || str_joinkey_fix || '
                  ) b
                  ON
                  a.' || str_joinkey_fix || ' = b.' || str_joinkey_fix || '
                  ON CONFLICT DO NOTHING
               ' USING
               int_owld;
               
               GET DIAGNOSTICS int_count = ROW_COUNT;
               
               IF out_src_found_count IS NULL
               OR out_src_found_count = 0
               THEN
                  out_src_found_count := int_count;
                  
               ELSE
                  out_src_found_count := out_src_found_count + int_count;
                  
               END IF;

               ----------------------------------------------------------------
               EXECUTE '
                  INSERT INTO tmp_src_lines(
                      eventtype
                     ,permid_joinkey
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,featuredetailurl
                     ,lengthkm
                     ,orderingkey
                     ,shape
                  )
                  SELECT
                   $1
                  ,a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.featuredetailurl
                  ,a.lengthkm
                  ,b.network_distancekm
                  ,a.shape
                  FROM
                  ' || str_owld || '_src_l a
                  JOIN (
                     SELECT
                      bb.' || str_joinkey_fix || '
                     ,MIN(bb.network_distancekm) AS network_distancekm
                     FROM
                     tmp_cip_found bb
                     GROUP BY
                     bb.' || str_joinkey_fix || '
                  ) b
                  ON
                  a.' || str_joinkey_fix || ' = b.' || str_joinkey_fix || '
                  ON CONFLICT DO NOTHING
               ' USING
               int_owld;
               
               GET DIAGNOSTICS int_count = ROW_COUNT;
               
               IF out_src_found_count IS NULL
               OR out_src_found_count = 0
               THEN
                  out_src_found_count := int_count;
                  
               ELSE
                  out_src_found_count := out_src_found_count + int_count;
                  
               END IF;

               ----------------------------------------------------------------
               EXECUTE '
                  INSERT INTO tmp_src_areas(
                      eventtype
                     ,permid_joinkey
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,featuredetailurl
                     ,areasqkm
                     ,orderingkey
                     ,shape
                  )
                  SELECT
                   $1
                  ,a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.featuredetailurl
                  ,a.areasqkm
                  ,b.network_distancekm
                  ,a.shape
                  FROM
                  ' || str_owld || '_src_a a
                  JOIN (
                     SELECT
                      bb.' || str_joinkey_fix || '
                     ,MIN(bb.network_distancekm) AS network_distancekm
                     FROM
                     tmp_cip_found bb
                     GROUP BY
                     bb.' || str_joinkey_fix || '
                  ) b
                  ON
                  a.' || str_joinkey_fix || ' = b.' || str_joinkey_fix || '
                  ON CONFLICT DO NOTHING
               ' USING
               int_owld;

               GET DIAGNOSTICS int_count = ROW_COUNT;
               
               IF out_src_found_count IS NULL
               OR out_src_found_count = 0
               THEN
                  out_src_found_count := int_count;
                  
               ELSE
                  out_src_found_count := out_src_found_count + int_count;
                  
               END IF;               

            END IF;
            
            -------------------------------------------------------------------
            -------------------------------------------------------------------
            EXECUTE '
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
                  ,src_cat_joinkey_count
                  ,nearest_cip_distancekm_permid
                  ,nearest_cip_network_distancekm
                  ,nearest_cip_flowtimeday_permid
                  ,nearest_cip_network_flowtimeday
               )
               SELECT
                $1
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.sfiddetailurl
               ,b.src_cat_joinkey_count
               ,b.nearest_cip_distancekm_permid
               ,b.nearest_cip_network_distancekm
               ,b.nearest_cip_flowtimeday_permid
               ,b.nearest_cip_network_flowtimeday
               FROM
               ' || str_owld || '_sfid a
               JOIN (
                  SELECT
                   bb.source_joinkey
                  ,COUNT(*) AS src_cat_joinkey_count
                  ,MIN(bb.permid_joinkey)      AS nearest_cip_distancekm_permid
                  ,MIN(bb.network_distancekm)  AS nearest_cip_network_distancekm
                  ,CASE
                   WHEN MIN(bb.network_flowtimeday) IS NULL
                   THEN
                     NULL
                   ELSE
                     MIN(bb.permid_joinkey)
                   END AS nearest_cip_flowtimeday_permid
                  ,MIN(bb.network_flowtimeday) AS nearest_cip_network_flowtimeday
                  FROM
                  tmp_cip_found bb
                  WHERE
                  bb.eventtype = $2
                  GROUP BY
                  bb.source_joinkey
               ) b
               ON
               a.source_joinkey = b.source_joinkey
            ' USING
             int_owld
            ,int_owld;
                
            GET DIAGNOSTICS int_count = ROW_COUNT;
       
            IF out_sfid_found_count IS NULL
            OR out_sfid_found_count = 0
            THEN
               out_sfid_found_count := int_count;
               
            ELSE
               out_sfid_found_count := out_sfid_found_count + int_count;
            
            END IF;
      
         END IF;
         
         ----------------------------------------------------------------------
         ----------------------------------------------------------------------
         IF p_return_linked_data_attributes
         THEN
            EXECUTE '
               INSERT INTO tmp_attr(
                   eventtype
                  ,source_joinkey
                  ,source_originator
                  ,source_featureid
                  ,source_featureid2
                  ,source_series
                  ,source_subdivision
                  ,start_date
                  ,end_date
                  ,sfiddetailurl
                  ,attributes
               )
               SELECT
                $1
               ,a.source_joinkey
               ,b.source_originator
               ,b.source_featureid
               ,b.source_featureid2
               ,b.source_series
               ,b.source_subdivision
               ,b.start_date
               ,b.end_date
               ,b.sfiddetailurl
               ,(TO_JSONB(a) -''objectid'' -''source_joinkey'' -''globalid'') AS attribute
               FROM
               ' || str_owld || '_attr a
               JOIN
               tmp_sfid_found b
               ON
               b.source_joinkey = a.source_joinkey
            ' USING
            int_owld;
            
            GET DIAGNOSTICS int_count = ROW_COUNT;
       
            IF out_attr_found_count IS NULL
            OR out_attr_found_count = 0
            THEN
               out_attr_found_count := int_count;
               
            ELSE
               out_attr_found_count := out_attr_found_count + int_count;
            
            END IF;
      
         END IF;
   
      END LOOP;
      
   END IF;
   
   ----------------------------------------------------------------------------
   IF NOT p_return_linked_data_rad
   THEN
      out_rad_found_count := NULL;
      
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
