DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.nav_dd';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_dd(
    IN  obj_start_flowline       cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distancekm   NUMERIC
   ,IN  num_maximum_flowtimeday  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   int_collect            INTEGER;
   int_count              INTEGER;
   int_min_hydrosequence  BIGINT;
   int_min_levelpathid    BIGINT;
   num_pathlength_buffer  NUMERIC := 100;
   num_pathtime_buffer    NUMERIC := 10;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Run DM first to establish mainline
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
      ,nav_order
      ,selected
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
      ,0 AS nav_order
      ,TRUE
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
      ,dm.nav_order + 10000
      ,CASE 
       WHEN (
         num_maximum_distancekm IS NULL
         AND
         num_maximum_flowtimeday IS NULL
       ) 
       OR dm.network_distancekm <= num_maximum_distancekm
       OR (
         mq.totma IS NOT NULL
         AND
         dm.network_flowtimeday <= num_maximum_flowtimeday
       )
       THEN
         TRUE
       ELSE
         FALSE
       END AS selected
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      dm
      WHERE
          mq.hydroseq   = dm.dnhydroseq
      AND mq.terminalpa = dm.terminalpa
      AND (
            num_maximum_distancekm IS NULL
         OR dm.network_distancekm <= num_maximum_distancekm + num_pathlength_buffer
      )
      AND (
            num_maximum_flowtimeday IS NULL
         OR ( 
            mq.totma IS NOT NULL
            AND
            dm.network_flowtimeday <= num_maximum_flowtimeday + num_pathtime_buffer
         )
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
      ,selected
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
   ,a.selected
   FROM
   dm a; 
   
   GET DIAGNOSTICS int_count = ROW_COUNT;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Tag the nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,b.coastal_connection
      ,b.network_end
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
          a.selected = TRUE   
      AND a.navtermination_flag IS NULL
   )
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN a.nav_order = (SELECT MAX(b.nav_order) FROM tmp_navigation_working30 b LIMIT 1)
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
      0
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Extract the divergences off the mainline
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
         ,b.nav_order + 1       AS nav_order
         FROM 
         cipsrv_nhdplus_h.nhdplusflowlinevaa_nav a
         JOIN 
         tmp_navigation_working30 b
         ON
             a.ary_upstream_hydroseq @> ARRAY[b.hydroseq]
         AND a.hydroseq <> b.dnhydroseq
         WHERE
             b.selected IS TRUE
         AND NOT EXISTS (
            SELECT
            1
            FROM
            tmp_navigation_working30 cc
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
            ,nav_order
            ,selected
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
            ,rec.nav_order
            ,TRUE
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
            ,dm.nav_order + 1
            ,TRUE AS selected
            FROM
            cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
            CROSS JOIN
            dm
            WHERE
                mq.hydroseq   = dm.dnhydroseq
            AND mq.terminalpa = dm.terminalpa
            AND (
                  num_maximum_distancekm IS NULL
               OR dm.network_distancekm <= num_maximum_distancekm
            )
            AND (
                  num_maximum_flowtimeday IS NULL
               OR ( 
                  mq.totma IS NOT NULL
                  AND
                  dm.network_flowtimeday <= num_maximum_flowtimeday
               )
            )
            AND NOT EXISTS (
               SELECT
               1
               FROM
               tmp_navigation_working30 cc
               WHERE
               cc.hydroseq = mq.hydroseq
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
            ,selected
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
         ,a.selected
         FROM
         dm a
         ON CONFLICT DO NOTHING;         
         
         GET DIAGNOSTICS int_collect = ROW_COUNT;
         int_count := int_count + int_collect;

      END LOOP;
   
      EXIT WHEN NOT FOUND;
   
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Remove extraneous records
   ----------------------------------------------------------------------------
   DELETE FROM tmp_navigation_working30 a
   WHERE
   NOT a.selected;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Tag the downstream nav termination flags
   ----------------------------------------------------------------------------
   FOR rec IN
      SELECT
       a.hydroseq
      ,b.ary_downstream_hydroseq
      ,b.coastal_connection
      ,b.network_end
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
      a.navtermination_flag IS NULL
   LOOP
      UPDATE tmp_navigation_working30 a
      SET navtermination_flag = CASE
      WHEN EXISTS ( SELECT 1 FROM tmp_navigation_working30 d WHERE d.hydroseq = ANY(rec.ary_downstream_hydroseq) )
      THEN
         0
      ELSE
         CASE
         WHEN rec.coastal_connection
         THEN
            3
         WHEN rec.network_end
         THEN
            5
         ELSE
            1
         END
      END
      WHERE
      a.hydroseq = rec.hydroseq;
   
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Return total count of results
   ----------------------------------------------------------------------------
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.nav_dd';
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
