DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.nav_dm';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_dm(
    IN  obj_start_flowline       cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distancekm   NUMERIC
   ,IN  num_maximum_flowtimeday  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   int_count INTEGER;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Return total count of results
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
      ,dm.nav_order + 1 
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
   -- Return total count of results
   ----------------------------------------------------------------------------
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_dm(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC   
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_dm(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
)  TO PUBLIC;

