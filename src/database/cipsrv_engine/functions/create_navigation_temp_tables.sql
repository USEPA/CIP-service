CREATE OR REPLACE FUNCTION cipsrv_engine.create_navigation_temp_tables()
RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_navigation_working30 temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_navigation_working30')
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
         ,levelpathi                  BIGINT
         ,terminalpa                  BIGINT
         ,uphydroseq                  BIGINT
         ,dnhydroseq                  BIGINT
         ,dnminorhyd                  BIGINT
         ,arbolatesu                  NUMERIC
         ,navtermination_flag         INTEGER
         ,nav_order                   INTEGER
         ,selected                    BOOLEAN
         ,is_open_branch              BOOLEAN
         ,branch_id                   INTEGER
      );

      CREATE UNIQUE INDEX tmp_navigation_working30_pk
      ON tmp_navigation_working30(hydroseq);
      
      CREATE INDEX tmp_navigation_working30_01i
      ON tmp_navigation_working30(dnhydroseq);
      
      CREATE INDEX tmp_navigation_working30_02i
      ON tmp_navigation_working30(navtermination_flag);
      
      CREATE INDEX tmp_navigation_working30_03i
      ON tmp_navigation_working30(selected)
      WHERE selected IS FALSE;
      
      CREATE INDEX tmp_navigation_working30_04i
      ON tmp_navigation_working30(is_open_branch)
      WHERE is_open_branch IS TRUE;
      
      CREATE INDEX tmp_navigation_working30_05i
      ON tmp_navigation_working30(branch_id);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_navigation_results temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_navigation_results')
   THEN
      TRUNCATE TABLE tmp_navigation_results;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_navigation_results(
          nhdplusid                   BIGINT
         ,hydroseq                    BIGINT
         ,fmeasure                    NUMERIC
         ,tmeasure                    NUMERIC
         ,levelpathi                  BIGINT
         ,terminalpa                  BIGINT
         ,uphydroseq                  BIGINT
         ,dnhydroseq                  BIGINT
         ,lengthkm                    NUMERIC
         ,flowtimeday                 NUMERIC
         /* ++++++++++ */
         ,network_distancekm          NUMERIC
         ,network_flowtimeday         NUMERIC
         /* ++++++++++ */
         ,permanent_identifier        VARCHAR(40)
         ,reachcode                   VARCHAR(14)
         ,fcode                       INTEGER
         ,gnis_id                     VARCHAR(10)
         ,gnis_name                   VARCHAR(65)
         ,wbarea_permanent_identifier VARCHAR(40)
         /* ++++++++++ */
         ,quality_marker              INTEGER
         ,navtermination_flag         INTEGER
         ,shape                       GEOMETRY
         ,nav_order                   INTEGER
      );

      CREATE UNIQUE INDEX tmp_navigation_results_pk
      ON tmp_navigation_results(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_navigation_results_1u
      ON tmp_navigation_results(hydroseq);
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.create_navigation_temp_tables() 
OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.create_navigation_temp_tables() 
TO PUBLIC;

