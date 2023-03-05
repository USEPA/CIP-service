CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.nav_um(
    IN  obj_start_flowline       cipsrv_nhdplus_m.flowline
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
      ,obj_start_flowline.pathlengthkm    + (obj_start_flowline.lengthkm    - obj_start_flowline.out_lengthkm)
      ,obj_start_flowline.pathflowtimeday + (obj_start_flowline.flowtimeday - obj_start_flowline.out_flowtimeday)
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
      ,um.nav_order + 1 
      FROM
      cipsrv_nhdplus_m.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      um
      WHERE
      (
         (
                mq.hydroseq   = um.uphydroseq
            AND mq.levelpathi = um.levelpathi
         )
         OR (
                mq.hydroseq   = um.uphydroseq
            AND um.divergence = 2
         )
      )
      AND(
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
   um a;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Tag the nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,b.headwater
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
   WHEN a.nav_order = (SELECT MAX(c.nav_order) FROM tmp_navigation_working30 c LIMIT 1)
   THEN
      CASE
      WHEN cte.headwater
      THEN
         4
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

ALTER FUNCTION cipsrv_nhdplus_m.nav_um(
    cipsrv_nhdplus_m.flowline
   ,NUMERIC
   ,NUMERIC   
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.nav_um(
    cipsrv_nhdplus_m.flowline
   ,NUMERIC
   ,NUMERIC
)  TO PUBLIC;

