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
