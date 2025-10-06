DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.nav_ppall';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.nav_ppall(
    IN  obj_start_flowline        cipsrv_nhdplus_m.flowline
   ,IN  obj_stop_flowline         cipsrv_nhdplus_m.flowline
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   rec         RECORD;
   int_count   INTEGER;
   int_check   INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_network_working30 temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_network_working30d')
   THEN
      TRUNCATE TABLE tmp_network_working30d;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_network_working30d(
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
         ,dnhydroseq                  BIGINT
         ,uphydroseq                  BIGINT
         ,fromnode                    BIGINT
         ,tonode                      BIGINT
         ,cost                        FLOAT8
      );

      CREATE INDEX tmp_network_working30d_01i
      ON tmp_network_working30d(nhdplusid);
      
      CREATE INDEX tmp_network_working30d_02i
      ON tmp_network_working30d(hydroseq);
      
      CREATE INDEX tmp_network_working30d_03i
      ON tmp_network_working30d(fromnode);
      
      CREATE INDEX tmp_network_working30d_04i
      ON tmp_network_working30d(tonode);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Run downstream mainline
   ----------------------------------------------------------------------------
   WITH RECURSIVE dm(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,base_pathlength
      ,base_pathtime
      ,fromnode
      ,tonode
      ,cost
   )
   AS (
      SELECT
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,obj_start_flowline.fmeasure
      ,obj_start_flowline.out_measure
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,obj_start_flowline.pathlengthkm    + obj_start_flowline.out_lengthkm
      ,obj_start_flowline.pathflowtimeday + obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.fromnode
      ,obj_start_flowline.tonode
      ,1::FLOAT8
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm  -- segment lengthkm
      ,mq.totma
      ,dm.base_pathlength - mq.pathlength
      ,dm.base_pathtime   - mq.pathtimema 
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,dm.base_pathlength -- base pathlength
      ,dm.base_pathtime
      ,mq.fromnode
      ,mq.tonode
      ,1::FLOAT8
      FROM
      cipsrv_nhdplus_m.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      dm
      WHERE
          mq.hydroseq   =  dm.dnhydroseq
      AND mq.terminalpa =  dm.terminalpa
      AND mq.hydroseq   >= obj_stop_flowline.hydroseq
   )
   INSERT INTO tmp_network_working30d(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi 
      ,terminalpa
      ,uphydroseq 
      ,dnhydroseq
      ,fromnode
      ,tonode
      ,cost
   )
   SELECT
    a.nhdplusid
   ,a.hydroseq
   ,a.fmeasure
   ,a.tmeasure
   ,a.lengthkm
   ,a.flowtimeday
   ,a.network_distancekm
   ,a.network_flowtimeday
   ,a.levelpathi
   ,a.terminalpa
   ,a.uphydroseq
   ,a.dnhydroseq
   ,a.fromnode
   ,a.tonode
   ,a.cost
   FROM
   dm a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Traverse any divergences
   ----------------------------------------------------------------------------
   LOOP
      FOR rec IN 
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,a.totma               AS flowtimeday
         ,b.network_distancekm  AS base_pathlength
         ,b.network_flowtimeday AS base_pathtime
         ,a.levelpathi
         ,a.terminalpa 
         ,a.uphydroseq
         ,a.dnhydroseq 
         ,a.fromnode
         ,a.tonode
         FROM 
         cipsrv_nhdplus_m.nhdplusflowlinevaa_nav a
         JOIN 
         tmp_network_working30d b
         ON
             a.ary_upstream_hydroseq @> ARRAY[b.hydroseq]
         AND a.hydroseq <> b.dnhydroseq
         WHERE
         NOT EXISTS (
            SELECT
            1
            FROM
            tmp_network_working30d cc
            WHERE
            cc.hydroseq = a.hydroseq
         )
         ORDER BY
         a.hydroseq DESC
      LOOP
      
         WITH RECURSIVE dm(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,base_pathlength
            ,base_pathtime
            ,fromnode
            ,tonode
            ,cost
         )
         AS (
            SELECT
             rec.nhdplusid
            ,rec.hydroseq
            ,rec.fmeasure
            ,rec.tmeasure
            ,rec.lengthkm
            ,rec.flowtimeday
            ,rec.base_pathlength  + rec.lengthkm
            ,rec.base_pathtime    + rec.flowtimeday
            ,rec.levelpathi
            ,rec.terminalpa
            ,rec.uphydroseq
            ,rec.dnhydroseq
            ,rec.base_pathlength 
            ,rec.base_pathtime
            ,rec.fromnode
            ,rec.tonode
            ,100::FLOAT8 AS cost
            UNION
            SELECT
             mq.nhdplusid
            ,mq.hydroseq
            ,mq.fmeasure
            ,mq.tmeasure
            ,mq.lengthkm
            ,mq.totma
            ,dm.network_distancekm  + mq.lengthkm
            ,dm.network_flowtimeday + mq.totma
            ,mq.levelpathi
            ,mq.terminalpa
            ,mq.uphydroseq
            ,mq.dnhydroseq
            ,dm.base_pathlength
            ,dm.base_pathtime
            ,mq.fromnode
            ,mq.tonode
            ,100::FLOAT8 AS cost
            FROM
            cipsrv_nhdplus_m.nhdplusflowlinevaa_nav mq
            CROSS JOIN
            dm
            WHERE
                mq.hydroseq   = dm.dnhydroseq
            AND mq.terminalpa = dm.terminalpa
            AND mq.hydroseq   >= obj_stop_flowline.hydroseq
            AND NOT EXISTS (
               SELECT
               1
               FROM
               tmp_network_working30d cc
               WHERE
               cc.hydroseq = mq.hydroseq
            )
         )
         INSERT INTO tmp_network_working30d(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi 
            ,terminalpa
            ,uphydroseq 
            ,dnhydroseq
            ,fromnode
            ,tonode
            ,cost
         )
         SELECT
          a.nhdplusid
         ,a.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,a.flowtimeday
         ,a.network_distancekm
         ,a.network_flowtimeday
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.fromnode
         ,a.tonode
         ,a.cost
         FROM
         dm a
         ON CONFLICT DO NOTHING;         

      END LOOP;
   
      EXIT WHEN NOT FOUND;
   
   END LOOP;
      
   ----------------------------------------------------------------------------
   -- Step 40
   -- If downstream location not found, then exit
   ----------------------------------------------------------------------------
   SELECT
   COUNT(*)
   INTO int_count
   FROM 
   tmp_network_working30d a
   WHERE
   a.nhdplusid = obj_stop_flowline.nhdplusid;
   
   IF int_count = 0
   THEN
      RETURN 0;
      
   END IF;
      
   ----------------------------------------------------------------------------
   -- Step 50
   -- Turn around and go upstream
   ----------------------------------------------------------------------------
   WITH RECURSIVE ut(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,base_pathlength
      ,base_pathtime
      ,nav_order
   )
   AS (
      SELECT
       obj_stop_flowline.nhdplusid
      ,obj_stop_flowline.hydroseq
      ,obj_stop_flowline.out_measure
      ,obj_stop_flowline.tmeasure
      ,obj_stop_flowline.out_lengthkm
      ,obj_stop_flowline.out_flowtimeday
      ,obj_stop_flowline.out_lengthkm
      ,obj_stop_flowline.out_flowtimeday
      ,obj_stop_flowline.levelpathi
      ,obj_stop_flowline.terminalpa
      ,obj_stop_flowline.uphydroseq
      ,obj_stop_flowline.dnhydroseq
      ,obj_stop_flowline.pathlengthkm    + (obj_stop_flowline.lengthkm    - obj_stop_flowline.out_lengthkm)
      ,obj_stop_flowline.pathflowtimeday + (obj_stop_flowline.flowtimeday - obj_stop_flowline.out_flowtimeday)
      ,0 AS nav_order
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm
      ,mq.totma
      ,mq.pathlength - ut.base_pathlength + mq.lengthkm
      ,mq.pathtimema - ut.base_pathtime   + mq.totma
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,ut.base_pathlength
      ,ut.base_pathtime
      ,ut.nav_order + 1 
      FROM 
      cipsrv_nhdplus_m.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      ut
      WHERE
      mq.ary_downstream_hydroseq @> ARRAY[ut.hydroseq]
      AND mq.hydroseq IN (
         SELECT
         b.hydroseq
         FROM
         tmp_network_working30d b
      )
   )
   INSERT INTO tmp_navigation_working30(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,nav_order
   )
   SELECT
    a.nhdplusid
   ,a.hydroseq
   ,a.fmeasure
   ,a.tmeasure
   ,a.lengthkm
   ,a.flowtimeday
   ,a.network_distancekm
   ,a.network_flowtimeday
   ,a.levelpathi
   ,a.terminalpa
   ,a.uphydroseq
   ,a.dnhydroseq
   ,a.nav_order
   FROM
   ut a
   ON CONFLICT DO NOTHING;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Trim the top of the run
   ----------------------------------------------------------------------------
   UPDATE tmp_navigation_working30 a
   SET 
    fmeasure            = obj_start_flowline.fmeasure
   ,tmeasure            = obj_start_flowline.out_measure
   ,lengthkm            = obj_start_flowline.out_lengthkm
   ,flowtimeday         = obj_start_flowline.out_flowtimeday
   ,network_distancekm  = a.network_distancekm  - (a.lengthkm    - obj_start_flowline.out_lengthkm)
   ,network_flowtimeday = a.network_flowtimeday - (a.flowtimeday - obj_start_flowline.out_flowtimeday)
   ,navtermination_flag = 2
   WHERE
   a.hydroseq = obj_start_flowline.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Return total count of results
   ----------------------------------------------------------------------------
   SELECT 
   COUNT(*) 
   INTO int_count 
   FROM 
   tmp_navigation_working30 a;
   
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.nav_ppall';
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

