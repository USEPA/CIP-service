DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.nav_ut_search';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.nav_ut_search(
    IN  p_start_flowline          cipsrv_nhdplus_m.flowline
   ,IN  p_maximum_distancekm      NUMERIC
   ,IN  p_maximum_flowtimeday     NUMERIC
   ,IN  p_init_baselengthkm       NUMERIC
   ,IN  p_init_baseflowtimeday    NUMERIC 
   ,IN  p_base_arbolatesu         NUMERIC
   ,IN  p_branch_id               INTEGER
   ,OUT out_flowline_count        INTEGER
   ,OUT out_branches              cipsrv_nhdplus_m.flowline[]
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   int_count                INTEGER;
   
BEGIN

   out_return_code          := 0;
   out_flowline_count       := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   ----------------------------------------------------------------------------
   IF p_start_flowline IS NULL
   THEN
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   ----------------------------------------------------------------------------
   WITH RECURSIVE ut(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ---
      ,lengthkm
      ,flowtimeday
      ---
      ,network_distancekm
      ,network_flowtimeday
      ---
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,divergence
      ---
      ,base_pathlength
      ,base_pathtime
      ,nav_order
      ,is_open_branch
   )
   AS (
      SELECT
       p_start_flowline.nhdplusid
      ,p_start_flowline.hydroseq
      ,p_start_flowline.out_measure
      ,p_start_flowline.tmeasure
      ---
      ,p_start_flowline.out_lengthkm
      ,p_start_flowline.out_flowtimeday
      ---
      ,p_start_flowline.network_distancekm
      ,p_start_flowline.network_flowtimeday
      ---
      ,p_start_flowline.levelpathi
      ,p_start_flowline.terminalpa
      ,p_start_flowline.uphydroseq
      ,p_start_flowline.dnhydroseq
      ,p_start_flowline.divergence
      --
      ,p_init_baselengthkm
      ,p_init_baseflowtimeday
      --
      ,p_start_flowline.nav_order
      ,FALSE AS is_open_branch
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm  -- segment lengthkm
      ,mq.totma
      ,(mq.pathlength + p_start_flowline.pathlength_adj)   - ut.base_pathlength + mq.lengthkm
      ,(mq.pathtimema + p_start_flowline.pathflowtime_adj) - ut.base_pathtime   + mq.totma
      ,mq.levelpathi
      ,mq.terminalpa
      ,CASE 
       WHEN mq.force_main_line
       THEN  -- Neuter navigation at big tribs
         CAST(NULL AS BIGINT)
       ELSE
         mq.uphydroseq
       END AS uphydroseq
      ,mq.dnhydroseq
      ,mq.divergence
      ---
      ,ut.base_pathlength -- base pathlength
      ,ut.base_pathtime
      ---
      ,ut.nav_order + 1
      ,CASE 
       WHEN mq.force_main_line
       THEN
         TRUE
       ELSE
         FALSE
       END AS is_open_branch
      FROM
      cipsrv_nhdplus_m.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      ut
      WHERE
          ut.uphydroseq IS NOT NULL
      AND mq.dnhydroseq = ut.hydroseq
      AND(
            p_maximum_distancekm IS NULL
         OR (mq.pathlength + p_start_flowline.pathlength_adj)   - ut.base_pathlength <= p_maximum_distancekm
      )
      AND (
            p_maximum_flowtimeday IS NULL
         OR (mq.pathtimema + p_start_flowline.pathflowtime_adj) - ut.base_pathtime   <= p_maximum_flowtimeday
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
      ,is_open_branch
      ,branch_id
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
   ,a.is_open_branch
   ,p_branch_id 
   FROM
   ut a
   ON CONFLICT DO NOTHING;

   GET DIAGNOSTICS out_flowline_count = ROW_COUNT;

   ---------------------------------------------------------------------------
   -- Step 30
   ----------------------------------------------------------------------------
   SELECT 
   ARRAY_AGG((   
       a.nhdplusid
      ,a.hydroseq
      ,a.fmeasure
      ,a.tmeasure
      ,a.levelpathi
      ,a.terminalpa
      ,a.uphydroseq
      ,a.dnhydroseq
      ,a.dnminorhyd
      ,a.divergence
      ,NULL::INTEGER
      ,a.arbolatesu
      ,NULL::BIGINT
      ,NULL::BIGINT
      ,a.vpuid
      ---
      ,NULL::VARCHAR
      ,NULL::VARCHAR
      ,NULL::INTEGER
      ---
      ,a.lengthkm
      ,a.lengthkm / (a.tmeasure - a.fmeasure)
      ,a.totma
      ,CASE 
       WHEN a.totma IN (-9998,-9999)
       THEN
          CAST(NULL AS NUMERIC)
       ELSE
          a.totma / (a.tmeasure - a.fmeasure)
       END
      ---
      ,a.pathlength
      ,a.pathtimema
      ---
      ,NULL::INTEGER
      ,a.fmeasure
      ,a.lengthkm
      ,a.totma
      ,a.pathlength
      ,a.pathtimema
      ,NULL::BIGINT
      ---
      ,a.network_distancekm  + a.lengthkm 
      ,a.network_flowtimeday + a.totma
      ,a.pathlength_adj
      ,a.pathtimema_adj
      ---
      ,a.nav_order + 1
      ,a.arbolatesu::INTEGER + 100000000
   )::cipsrv_nhdplus_m.flowline)
   INTO out_branches
   FROM (
      SELECT
       aa.nhdplusid
      ,aa.hydroseq
      ,aa.fmeasure
      ,aa.tmeasure
      ,aa.levelpathi
      ,aa.terminalpa
      ,aa.uphydroseq
      ,aa.dnhydroseq
      ,aa.dnminorhyd
      ,aa.divergence
      ,NULL::INTEGER
      ,aa.arbolatesu
      ,NULL::BIGINT
      ,NULL::BIGINT
      ,aa.vpuid
      ---
      ,NULL::VARCHAR
      ,NULL::VARCHAR
      ,NULL::INTEGER
      ---
      ,aa.lengthkm
      ,aa.totma
      ,aa.pathlength
      ,aa.pathtimema
      ---
      ,bb.network_distancekm
      ,bb.network_flowtimeday
      ,0  AS pathlength_adj
      ,0  AS pathtimema_adj
      ---
      ,bb.nav_order
      FROM
      cipsrv_nhdplus_m.nhdplusflowlinevaa_nav aa
      JOIN
      tmp_navigation_working30 bb
      ON
      bb.hydroseq = aa.hydroseq
      WHERE
          bb.branch_id = p_branch_id
      AND bb.is_open_branch
      ORDER BY
      bb.network_distancekm
   ) a;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.nav_ut_search';
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

