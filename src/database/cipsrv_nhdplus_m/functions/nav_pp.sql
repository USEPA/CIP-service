DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.nav_pp';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.nav_pp(
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
   IF cipsrv_engine.temp_table_exists('tmp_network_working30')
   THEN
      TRUNCATE TABLE tmp_network_working30;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_network_working30(
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
         ,fromnode                    BIGINT
         ,tonode                      BIGINT
         ,cost                        FLOAT8
      );

      CREATE INDEX tmp_network_working30_01i
      ON tmp_network_working30(nhdplusid);
      
      CREATE INDEX tmp_network_working30_02i
      ON tmp_network_working30(hydroseq);
      
      CREATE INDEX tmp_network_working30_03i
      ON tmp_network_working30(fromnode);
      
      CREATE INDEX tmp_network_working30_04i
      ON tmp_network_working30(tonode);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Run downstream mainline as most probable solution
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
   INSERT INTO tmp_network_working30(
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

   SELECT
   COUNT(*)
   INTO int_count
   FROM 
   tmp_network_working30 a
   WHERE
   a.nhdplusid = obj_stop_flowline.nhdplusid;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- If found then dump into working30 and exit
   ----------------------------------------------------------------------------
   IF int_count > 0
   THEN   
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
         ,selected
      )
      SELECT
       b.nhdplusid
      ,b.hydroseq
      ,b.fmeasure
      ,b.tmeasure
      ,b.lengthkm
      ,b.flowtimeday
      ,b.network_distancekm
      ,b.network_flowtimeday
      ,b.levelpathi
      ,b.terminalpa
      ,b.uphydroseq
      ,b.dnhydroseq
      ,TRUE
      FROM
      tmp_network_working30 b;
      
      UPDATE tmp_navigation_working30 a
      SET
       fmeasure            = obj_stop_flowline.out_measure
      ,tmeasure            = obj_stop_flowline.tmeasure
      ,lengthkm            = obj_stop_flowline.out_lengthkm
      ,flowtimeday         = obj_stop_flowline.out_flowtimeday
      ,network_distancekm  = a.network_distancekm  + obj_stop_flowline.out_lengthkm    - a.lengthkm
      ,network_flowtimeday = a.network_flowtimeday + obj_stop_flowline.out_flowtimeday - a.flowtimeday
      WHERE
      a.nhdplusid = obj_stop_flowline.nhdplusid;
      
   -------------------------------------------------------------------------
   -- Step 40
   -- Otherwise run divergences downstream
   -------------------------------------------------------------------------
   ELSE
   
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
            tmp_network_working30 b
            ON
                a.ary_upstream_hydroseq @> ARRAY[b.hydroseq]
            AND a.hydroseq <> b.dnhydroseq
            WHERE
            NOT EXISTS (
               SELECT
               1
               FROM
               tmp_network_working30 cc
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
                  tmp_network_working30 cc
                  WHERE
                  cc.hydroseq = mq.hydroseq
               )
            )
            INSERT INTO tmp_network_working30(
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
      
      SELECT
      COUNT(*)
      INTO int_count
      FROM 
      tmp_network_working30 a
      WHERE
      a.nhdplusid = obj_stop_flowline.nhdplusid;

      IF int_count > 0
      THEN
         -- Remove duplicate flowlines keeping the cheapest cost
         DELETE FROM tmp_network_working30 a
         WHERE a.ctid IN (
            SELECT b.ctid FROM (
               SELECT
                bb.ctid
               ,ROW_NUMBER() OVER (PARTITION BY bb.nhdplusid ORDER BY bb.cost ASC) AS rnum
               FROM
               tmp_network_working30 bb
            ) b
            WHERE
            b.rnum > 1
         );
         
         -- Determine dikstra shortest route from start to stop
         WITH dijk AS(
            SELECT
             a.seq
            ,a.node
            ,a.edge
            ,a.cost
            FROM
            pgr_dijkstra(
                'SELECT nhdplusid AS id,fromnode AS source,tonode AS target,cost,-1::FLOAT8 AS reverse_cost FROM tmp_network_working30'
               ,obj_start_flowline.out_node
               ,obj_stop_flowline.out_node
               ,TRUE
            ) a
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
            ,selected
         )
         SELECT
          b.nhdplusid
         ,b.hydroseq
         ,b.fmeasure
         ,b.tmeasure
         ,b.lengthkm
         ,b.flowtimeday
         ,b.network_distancekm
         ,b.network_flowtimeday
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,TRUE
         FROM
         dijk a
         JOIN
         tmp_network_working30 b
         ON
         a.edge = b.nhdplusid;
         
         -- Replace the start and stop segments
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
            ,selected
         ) VALUES (
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
            ,0
            ,TRUE
         );
         
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
            ,selected
         ) VALUES (
             obj_stop_flowline.nhdplusid
            ,obj_stop_flowline.hydroseq
            ,obj_stop_flowline.out_measure
            ,obj_stop_flowline.tmeasure
            ,obj_stop_flowline.out_lengthkm
            ,obj_stop_flowline.out_flowtimeday
            ,obj_start_flowline.out_pathlengthkm    - obj_stop_flowline.out_pathlengthkm
            ,obj_start_flowline.out_pathflowtimeday - obj_stop_flowline.out_pathflowtimeday
            ,obj_stop_flowline.levelpathi
            ,obj_stop_flowline.terminalpa
            ,obj_stop_flowline.uphydroseq
            ,obj_stop_flowline.dnhydroseq
            ,99999999
            ,TRUE            
         );
         
      END IF;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Tag the downstream nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,a.fmeasure
      ,a.tmeasure
      ,b.ary_downstream_hydroseq
      ,b.coastal_connection
      ,b.network_end
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_m.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
          a.selected = TRUE   
      AND a.navtermination_flag IS NULL
   )
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN EXISTS ( SELECT 1 FROM tmp_navigation_working30 d WHERE d.hydroseq = ANY(cte.ary_downstream_hydroseq) )
   THEN
      0
   ELSE
      CASE
      WHEN cte.hydroseq = obj_stop_flowline.hydroseq
      THEN
         CASE
         WHEN obj_stop_flowline.fmeasure = cte.fmeasure 
         AND  obj_stop_flowline.tmeasure = cte.tmeasure 
         THEN
            CASE
            WHEN cte.coastal_connection
            THEN
               3
            WHEN cte.network_end
            THEN
               5
            ELSE
               1
            END
         ELSE
            2
         END
      END
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Return total count of results
   ----------------------------------------------------------------------------
   SELECT 
   COUNT(*) 
   INTO int_count 
   FROM 
   tmp_navigation_working30 a
   WHERE 
   a.selected IS TRUE;
   
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.nav_pp(
    cipsrv_nhdplus_m.flowline
   ,cipsrv_nhdplus_m.flowline  
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.nav_pp(
    cipsrv_nhdplus_m.flowline
   ,cipsrv_nhdplus_m.flowline
)  TO PUBLIC;

