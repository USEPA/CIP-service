DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.randomppnav';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.randomppnav(
    IN  p_region               VARCHAR DEFAULT NULL
   ,IN  p_return_geometry      BOOLEAN DEFAULT FALSE
   ,OUT out_nhdplusid1         BIGINT
   ,OUT out_reachcode1         VARCHAR
   ,OUT out_measure1           NUMERIC
   ,OUT out_shape1             GEOMETRY
   ,OUT out_nhdplusid2         BIGINT
   ,OUT out_reachcode2         VARCHAR
   ,OUT out_measure2           NUMERIC
   ,OUT out_shape2             GEOMETRY
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                RECORD;
   num_fmeasure       NUMERIC;
   num_tmeasure       NUMERIC;
   int_hydroseq       BIGINT;
   int_dnhydroseq     BIGINT;
   int_terminalpathid BIGINT;
   boo_check          BOOLEAN;
   int_sanity         INTEGER;
   sdo_flowline       GEOMETRY;
   int_sancheck       INTEGER;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF p_region IN ('32702','AS','AMERICAN SAMOA')
   THEN
      int_sancheck := 250;
      
   ELSE
      int_sancheck := 100;
   
   END IF;
   
   boo_check := FALSE;
   int_sanity := 1;
   
   WHILE NOT boo_check
   LOOP
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Select a random navigable flowline
   --------------------------------------------------------------------------
      rec := cipsrv_nhdplus_h.randomnav(
          p_region          := p_region
         ,p_return_geometry := p_return_geometry
      );
      
      IF rec.out_return_code != 0
      THEN
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         RETURN;
      
      END IF;
      
      out_nhdplusid1  := rec.out_nhdplusid;
      out_reachcode1  := rec.out_reachcode;
      out_measure1    := rec.out_measure;
      out_shape1      := rec.out_shape;
   
      SELECT
       a.hydroseq
      ,a.dnhydroseq
      ,a.terminalpa
      INTO
       int_hydroseq
      ,int_dnhydroseq
      ,int_terminalpathid
      FROM
      cipsrv_nhdplus_h.networknhdflowline a 
      WHERE
      a.nhdplusid = out_nhdplusid1;
      
   --------------------------------------------------------------------------
   -- Step 30
   -- Select a random downstream location from the start point
   --------------------------------------------------------------------------
      WITH RECURSIVE pp(
         nhdplusid
        ,hydroseq
        ,dnhydroseq
        ,terminalpa
        ,fmeasure
        ,tmeasure
      )
      AS (
         SELECT
          out_nhdplusid1
         ,int_hydroseq
         ,int_dnhydroseq
         ,int_terminalpathid
         ,0::NUMERIC
         ,0::NUMERIC
         UNION
         SELECT
          mq.nhdplusid
         ,mq.hydroseq
         ,mq.dnhydroseq
         ,mq.terminalpa
         ,mq.fmeasure
         ,mq.tmeasure
         FROM
         cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
         CROSS JOIN
         pp
         WHERE
             mq.ary_upstream_hydroseq @> ARRAY[pp.hydroseq]
         AND mq.terminalpa =  pp.terminalpa
      )
      SELECT
       a.nhdplusid
      ,b.reachcode
      ,a.fmeasure
      ,a.tmeasure
      ,CASE WHEN p_return_geometry THEN b.shape ELSE NULL::GEOMETRY END AS shape
      INTO
       out_nhdplusid2
      ,out_reachcode2
      ,num_fmeasure
      ,num_tmeasure
      ,sdo_flowline
      FROM
      pp a
      JOIN
      cipsrv_nhdplus_h.networknhdflowline b
      ON
      b.nhdplusid = a.nhdplusid
      WHERE
          a.nhdplusid <> out_nhdplusid1
      AND RANDOM() < 0.01 
      LIMIT 1;
      
      out_measure2 := RANDOM() * (num_tmeasure - num_fmeasure) + num_fmeasure;
      out_measure2 := ROUND(out_measure2,5);
      
      IF out_nhdplusid2 IS NOT NULL
      THEN
         boo_check := TRUE;
         
      END IF;
      
      int_sanity := int_sanity + 1;
      
      IF int_sanity > int_sancheck
      THEN
         out_return_code := -9;
         out_status_message := 'Sanity check failed';
         RETURN;
      
      END IF;
      
   END LOOP;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Build shape2 point if requested
   --------------------------------------------------------------------------
   IF p_return_geometry
   THEN
      sdo_flowline := ST_TRANSFORM(sdo_flowline,4326);   
      out_shape2   := ST_Force2D(ST_GeometryN(ST_LocateAlong(sdo_flowline,out_measure2),1));
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.randomppnav(
    VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.randomppnav(
    VARCHAR
   ,BOOLEAN
) TO PUBLIC;

