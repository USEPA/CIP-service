DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.nav_ut_minordiv';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.nav_ut_minordiv(
    IN  p_maximum_distancekm      NUMERIC
   ,IN  p_maximum_flowtimeday     NUMERIC
   ,IN  p_source_pathlength_adj   NUMERIC
   ,IN  p_source_pathtimema_adj   NUMERIC
   ,OUT out_minor_divs            cipsrv_nhdplus_m.flowline[]
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                      RECORD;
   obj_flowline             cipsrv_nhdplus_m.flowline;
   int_count bigint;
   z bigint[];
   zn numeric[];
   vs varchar[];
   
BEGIN

   out_return_code := 0;
   out_minor_divs  := ARRAY[]::cipsrv_nhdplus_m.flowline[];

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
      ,a.pathlength_adj + p_source_pathlength_adj
      ,a.pathtimema_adj + p_source_pathtimema_adj
      ---
      ,a.nav_order + 1
      ,a.arbolatesu::INTEGER
   )::cipsrv_nhdplus_m.flowline)
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
      cipsrv_nhdplus_m.nhdplusflowlinevaa_nav aa
      JOIN (
         SELECT
          ccc.tohydroseq
         ,ccc.fromhydroseq
         ,bbb.network_distancekm
         ,bbb.network_flowtimeday
         ,ccc.pathlength_adj
         ,ccc.pathtimema_adj
         ,bbb.nav_order
         FROM
         tmp_navigation_working30 bbb
         JOIN
         cipsrv_nhdplus_m.nhdplusflow_upminordivs ccc
         ON
         ccc.tohydroseq = bbb.hydroseq
         WHERE 
         NOT EXISTS (
            SELECT
            1
            FROM
            tmp_navigation_working30 ddd
            WHERE
            ddd.hydroseq = ccc.fromhydroseq
         )
      ) bb
      ON
      bb.fromhydroseq = aa.hydroseq
      WHERE (
            p_maximum_distancekm IS NULL
         OR bb.network_distancekm  <= p_maximum_distancekm
      )
      AND (
            p_maximum_flowtimeday IS NULL
         OR bb.network_flowtimeday <= p_maximum_flowtimeday
      )
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
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.nav_ut_minordiv';
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

