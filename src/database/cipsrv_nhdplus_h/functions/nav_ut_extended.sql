DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.nav_ut_extended';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_ut_extended(
    IN  obj_start_flowline       cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distancekm   NUMERIC
   ,IN  num_maximum_flowtimeday  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   rec                      RECORD;
   int_count                INTEGER;
   int_check                INTEGER;
   num_init_baselengthkm    NUMERIC;
   num_init_baseflowtimeday NUMERIC;
   
BEGIN

   num_init_baselengthkm    := obj_start_flowline.pathlengthkm    + (obj_start_flowline.lengthkm    - obj_start_flowline.out_lengthkm);
   num_init_baseflowtimeday := obj_start_flowline.pathflowtimeday + (obj_start_flowline.flowtimeday - obj_start_flowline.out_flowtimeday);
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- First run upstream mainline
   ----------------------------------------------------------------------------
   WITH RECURSIVE um(
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
      ,divergence
      ,base_pathlength
      ,base_pathtime
      ,nav_order
   )
   AS (
      SELECT
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,obj_start_flowline.out_measure
      ,obj_start_flowline.tmeasure
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,obj_start_flowline.divergence
      ,num_init_baselengthkm
      ,num_init_baseflowtimeday
      ,0 AS nav_order
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm  -- segment lengthkm
      ,mq.totma
      ,mq.pathlength - um.base_pathlength + mq.lengthkm
      ,mq.pathtimema - um.base_pathtime   + mq.totma
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,mq.divergence
      ,um.base_pathlength -- base pathlength
      ,um.base_pathtime
      ,um.nav_order + 1000              
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      um
      WHERE 
      (
         (
                mq.hydroseq   = um.uphydroseq
            AND mq.levelpathi = um.levelpathi
         )
         OR (
                mq.hydroseq    = um.uphydroseq
            AND um.divergence  = 2
         )
         OR (
                mq.force_main_line IS TRUE
            AND mq.dnhydroseq  = um.hydroseq
         )
      )
      AND (
            num_maximum_distancekm IS NULL
         OR mq.pathlength - um.base_pathlength <= num_maximum_distancekm
      )
      AND (
            num_maximum_flowtimeday IS NULL
         OR mq.pathtimema - um.base_pathtime   <= num_maximum_flowtimeday
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
   ,TRUE
   FROM
   um a
   ON CONFLICT DO NOTHING;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Tag the upstream mainline nav termination flags
   ----------------------------------------------------------------------------
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN a.nav_order = (SELECT MAX(b.nav_order) FROM tmp_navigation_working30 b LIMIT 1)
   THEN
      1
   ELSE
      0
   END
   WHERE 
   selected = TRUE;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Extract the divs off the mainline
   ----------------------------------------------------------------------------
   FOR rec IN 
      SELECT 
       a.nhdplusid
      ,a.hydroseq
      ,a.fmeasure
      ,a.tmeasure
      ,a.lengthkm
      ,a.totma
      ,b.network_distancekm  + a.lengthkm AS network_distancekm
      ,b.network_flowtimeday + a.totma    AS network_flowtimeday 
      ,a.levelpathi
      ,a.terminalpa
      ,a.uphydroseq
      ,a.dnhydroseq
      ,b.nav_order
      FROM 
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav a
      JOIN
      tmp_navigation_working30 b
      ON
      a.ary_downstream_hydroseq @> ARRAY[b.hydroseq]
      WHERE NOT EXISTS (
         SELECT
         1
         FROM
         tmp_navigation_working30 cc
         WHERE
         cc.hydroseq = a.hydroseq
      )
   
   LOOP
      
      BEGIN
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
             rec.nhdplusid
            ,rec.hydroseq
            ,rec.fmeasure
            ,rec.tmeasure
            ,rec.lengthkm
            ,rec.totma
            ,rec.network_distancekm
            ,rec.network_flowtimeday
            ,rec.levelpathi
            ,rec.terminalpa
            ,rec.uphydroseq
            ,rec.dnhydroseq
            ,rec.nav_order
            ,TRUE
         );
   
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
             rec.nhdplusid
            ,rec.hydroseq
            ,rec.fmeasure
            ,rec.tmeasure
            ,rec.lengthkm
            ,rec.totma
            ,rec.lengthkm
            ,rec.totma
            ,rec.levelpathi
            ,rec.terminalpa
            ,rec.uphydroseq
            ,rec.dnhydroseq
            ,num_init_baselengthkm 
            ,num_init_baseflowtimeday
            ,rec.nav_order
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
            cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
            CROSS JOIN
            ut
            WHERE
            mq.ary_downstream_hydroseq @> ARRAY[ut.hydroseq]
            AND (
                  num_maximum_distancekm IS NULL
               OR mq.pathlength - ut.base_pathlength <= num_maximum_distancekm
            )
            AND (
                  num_maximum_flowtimeday IS NULL
               OR mq.pathtimema - ut.base_pathtime   <= num_maximum_flowtimeday
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
         ,TRUE
         FROM
         ut a
         WHERE
         a.nhdplusid <> rec.nhdplusid
         ON CONFLICT DO NOTHING;
         
         -- At some point this should be removed
         GET DIAGNOSTICS int_check = row_count;
         IF int_check > 10000
         THEN
            RAISE WARNING '% %',rec.nhdplusid,int_check;
            
         END IF;              
         
         int_count := int_count + int_check;
         
      EXCEPTION
         WHEN UNIQUE_VIOLATION 
         THEN
            NULL;

         WHEN OTHERS
         THEN               
            RAISE;
            
      END;

   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Tag the upstream mainline nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,b.ary_upstream_hydroseq
      ,b.headwater
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
   WHEN EXISTS ( SELECT 1 FROM tmp_navigation_working30 d WHERE d.hydroseq = ANY(cte.ary_upstream_hydroseq) )
   THEN
      0
   ELSE
      CASE
      WHEN cte.headwater
      THEN
         4
      ELSE
         1
      END
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Return total count of results
   ----------------------------------------------------------------------------
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_ut_extended(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC   
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_ut_extended(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
)  TO PUBLIC;

