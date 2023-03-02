CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.create_navigation_temp_tables()
RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE

BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_navigation_connections temp table
   ----------------------------------------------------------------------------
   
   IF cipsrv_nhdplus_m.temp_table_exists('tmp_navigation_working30')
   THEN
      TRUNCATE TABLE tmp_navigation_working30;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_navigation_working30(
          nhdplusid                   BIGINT
         ,hydroseq                    BIGINT
         ,fmeasure                    NUMERIC
         ,tmeasure                    NUMERIC
         ,lengthkm                    NUMERIC
         ,flowtimeday                 NUMERIC
         ,network_distancekm          NUMERIC
         ,network_flowtimeday         NUMERIC
         ,downhydroseq                BIGINT
         ,navtermination_flag         INTEGER
         ,nav_order                   INTEGER
         ,selected                    BOOLEAN
      );

      CREATE UNIQUE INDEX tmp_navigation_working30_pk
      ON tmp_navigation_working30(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_navigation_working30_1u
      ON tmp_navigation_working30(hydroseq);
      
      CREATE INDEX tmp_navigation_working30_01i
      ON tmp_navigation_working30(network_distancekm);
            
      CREATE INDEX tmp_navigation_working30_02i
      ON tmp_navigation_working30(network_flowtimeday);
      
      CREATE INDEX tmp_navigation_working30_03i
      ON tmp_navigation_working30(downhydroseq);
      
      CREATE INDEX tmp_navigation_working30_04i
      ON tmp_navigation_working30(nav_order);
      
      CREATE INDEX tmp_navigation_working30_05i
      ON tmp_navigation_working30(selected);
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.create_navigation_temp_tables() OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.create_navigation_temp_tables() TO PUBLIC;

