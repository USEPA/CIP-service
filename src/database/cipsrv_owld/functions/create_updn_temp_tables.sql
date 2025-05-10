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
   -- Step 40
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
   -- Step 50
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
   -- Step 60
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
   -- Step 70
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
   -- Step 80
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
