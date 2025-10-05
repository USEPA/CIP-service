DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.nav_ut_extended_minordiv';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_ut_extended_minordiv(
    IN  p_maximum_distancekm      NUMERIC
   ,IN  p_maximum_flowtimeday     NUMERIC
   ,IN  p_source_pathlength_adj   NUMERIC
   ,IN  p_source_pathtimema_adj   NUMERIC
   ,OUT out_minor_divs            cipsrv_nhdplus_h.flowline[]
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                      RECORD;
   obj_flowline             cipsrv_nhdplus_h.flowline;
   int_count bigint;
   z bigint[];
   zn numeric[];
   vs varchar[];
   
BEGIN
/*
   SELECT
     array_agg(aa.hydroseq::VARCHAR || ' ,' || aa.lengthkm::VARCHAR || ' ,' || bb.pathlength_adj::VARCHAR ) INTO vs

   FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav aa
      JOIN (
         SELECT
          bbb.hydroseq
         ,bbb.uphydroseq
         ,bbb.network_distancekm
         ,bbb.network_flowtimeday
         ,ccc.pathlength_adj
         ,ccc.pathtimema_adj
         FROM
         tmp_navigation_working30 bbb
         JOIN
         cipsrv_nhdplus_h.nhdplusflow_upminordivs ccc
         ON
         ccc.tohydroseq = bbb.hydroseq
         WHERE 
         NOT EXISTS (
            SELECT
            1
            FROM
            tmp_navigation_working30 ddd
            WHERE
            ddd.hydroseq = bbb.uphydroseq
         )
      ) bb
      ON
      bb.uphydroseq = aa.hydroseq;
      raise exception '%',to_json(vs);

*/

   out_return_code := 0;
   out_minor_divs  := ARRAY[]::cipsrv_nhdplus_h.flowline[];

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
      ,a.pathlength_adj + p_source_pathlength_adj
      ,a.pathtimema_adj + p_source_pathtimema_adj
      ---
      ,a.nav_order + 1
   )::cipsrv_nhdplus_h.flowline)
   INTO out_minor_divs
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
      ,bb.pathlength_adj
      ,bb.pathtimema_adj
      ---
      ,bb.nav_order
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav aa
      JOIN (
         SELECT
          bbb.hydroseq
         ,bbb.uphydroseq
         ,bbb.network_distancekm
         ,bbb.network_flowtimeday
         ,ccc.pathlength_adj
         ,ccc.pathtimema_adj
         ,bbb.nav_order
         FROM
         tmp_navigation_working30 bbb
         JOIN
         cipsrv_nhdplus_h.nhdplusflow_upminordivs ccc
         ON
         ccc.tohydroseq = bbb.hydroseq
         WHERE 
         NOT EXISTS (
            SELECT
            1
            FROM
            tmp_navigation_working30 ddd
            WHERE
            ddd.hydroseq = bbb.uphydroseq
         )
      ) bb
      ON
      bb.uphydroseq = aa.hydroseq
      ORDER BY
      bb.network_distancekm  + bb.pathlength_adj
   ) a;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.nav_ut_extended_minordiv';
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

