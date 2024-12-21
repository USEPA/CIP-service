DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.randomnav';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.randomnav(
    IN  p_region               VARCHAR DEFAULT NULL
   ,IN  p_return_geometry      BOOLEAN DEFAULT FALSE
   ,OUT out_nhdplusid          BIGINT
   ,OUT out_reachcode          VARCHAR
   ,OUT out_measure            NUMERIC
   ,OUT out_shape              GEOMETRY
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   num_fmeasure    NUMERIC;
   num_tmeasure    NUMERIC;
   num_random      NUMERIC;
   boo_search      BOOLEAN;
   int_sanity      INTEGER;
   num_big_samp    NUMERIC := 0.001;
   sdo_flowline    GEOMETRY;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Select a random navigable flowline
   --------------------------------------------------------------------------
   boo_search := TRUE;
   int_sanity := 0;
   
   WHILE boo_search
   LOOP
      IF p_region IN ('CONUS','5070')
      THEN
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.isnavigable
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline_5070 aa
            TABLESAMPLE SYSTEM(num_big_samp)
         ) a
         WHERE 
         a.isnavigable
         ORDER BY RANDOM()
         LIMIT 1;
            
      ELSIF p_region IN ('HAWAII','HI','26904')
      THEN
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.isnavigable
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline_26904 aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         WHERE 
         a.isnavigable
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('PRVI','32161')
      THEN
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.isnavigable
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline_32161 aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         WHERE 
         a.isnavigable
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('GUAMMAR','GUMP','32655')
      THEN
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.isnavigable
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline_32655 aa
            TABLESAMPLE SYSTEM(1)
         ) a
         WHERE 
         a.isnavigable
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('AMSAMOA','AS','32702')
      THEN
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.isnavigable
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline_32702 aa
            TABLESAMPLE SYSTEM(1)
         ) a
         WHERE 
         a.isnavigable = 'Y'
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSE
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.fcode
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.networknhdflowline aa
            TABLESAMPLE SYSTEM(num_big_samp)
         ) a
         WHERE 
         a.fcode NOT IN (56600)
         ORDER BY RANDOM()
         LIMIT 1;
      
      END IF;
      
      IF out_nhdplusid IS NOT NULL
      THEN
         boo_search := FALSE;
         
      END IF;
      
      int_sanity := int_sanity + 1;
      IF int_sanity > 10
      THEN
         num_big_samp := num_big_samp * 2;
         
      END IF;
      
      IF int_sanity > 25
      THEN
         out_return_code := -9;
         out_status_message := 'Unable to sample ' || p_region || ' via ' || num_big_samp::VARCHAR;
         RETURN;
         
      END IF;
      
   END LOOP;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Determine random measure on flowline
   --------------------------------------------------------------------------
   out_measure := RANDOM() * (num_tmeasure - num_fmeasure) + num_fmeasure;
   out_measure := ROUND(out_measure,5);
   
   IF p_return_geometry
   THEN
      sdo_flowline := ST_TRANSFORM(sdo_flowline,4326);   
      out_shape    := ST_Force2D(ST_GeometryN(ST_LocateAlong(sdo_flowline,out_measure),1));
      
   END IF;
   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.randomnav(
    VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.randomnav(
    VARCHAR
   ,BOOLEAN
) TO PUBLIC;

