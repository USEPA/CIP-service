DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.randomcatchment';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.randomcatchment(
    IN  p_region                VARCHAR DEFAULT NULL
   ,IN  p_include_extended      BOOLEAN DEFAULT FALSE
   ,IN  p_return_geometry       BOOLEAN DEFAULT FALSE
   ,OUT out_nhdplusid           BIGINT
   ,OUT out_areasqkm            NUMERIC
   ,OUT out_catchmentstatecodes VARCHAR[]
   ,OUT out_shape               GEOMETRY
   ,OUT out_centroid            GEOMETRY
   ,OUT out_return_code         INTEGER
   ,OUT out_status_message      VARCHAR
)
STABLE
AS $BODY$
DECLARE
   boo_search           BOOLEAN;
   int_sanity           INTEGER;
   num_big_samp         NUMERIC := 0.001;
   str_statecode        VARCHAR;
   boo_include_extended BOOLEAN := p_include_extended;
   boo_return_geometry  BOOLEAN := p_return_geometry;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF boo_include_extended IS NULL
   THEN
      boo_include_extended := FALSE;
      
   END IF;
   
   IF boo_return_geometry IS NULL
   THEN
      boo_return_geometry := FALSE;
      
   END IF;
   
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
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isocean
            FROM
            cipsrv_nhdplus_h.catchment_5070 aa
            TABLESAMPLE SYSTEM(num_big_samp)
         ) a
         WHERE 
         boo_include_extended OR NOT a.isocean
         ORDER BY RANDOM()
         LIMIT 1;
               
      ELSIF p_region IN ('ALASKA','AK','3338')
      THEN     
         IF NOT boo_include_extended
         THEN
            out_status_message := 'Alaska is entirely extended H3 catchments.';
            RETURN;
            
         END IF;
         
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isalaskan
            FROM
            cipsrv_nhdplus_h.catchment_3338 aa
            TABLESAMPLE SYSTEM(num_big_samp)
         ) a
         WHERE 
         boo_include_extended OR NOT a.isalaskan
         ORDER BY RANDOM()
         LIMIT 1;
               
      ELSIF p_region IN ('HAWAII','HI','26904')
      THEN
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isocean
            FROM
            cipsrv_nhdplus_h.catchment_26904 aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         WHERE 
         boo_include_extended OR NOT a.isocean
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('PRVI','32161')
      THEN
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isocean
            FROM
            cipsrv_nhdplus_h.catchment_32161 aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         WHERE 
         boo_include_extended OR NOT a.isocean
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('GUAMMAR','GUMP','32655')
      THEN         
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isocean
            FROM
            cipsrv_nhdplus_h.catchment_32655 aa
            TABLESAMPLE SYSTEM(1)
         ) a
         WHERE 
         boo_include_extended OR NOT a.isocean
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('AMSAMOA','AS','32702')
      THEN
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isocean
            FROM
            cipsrv_nhdplus_h.catchment_32702 aa
            TABLESAMPLE SYSTEM(1)
         ) a
         WHERE 
         boo_include_extended OR NOT a.isocean
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSE
         SELECT
          a.nhdplusid
         ,a.catchmentstatecode
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecode
            ,aa.isocean
            ,aa.isalaskan
            FROM
            cipsrv_epageofab_h.catchment_fabric aa
            TABLESAMPLE SYSTEM(num_big_samp)
         ) a
         WHERE 
         boo_include_extended OR (a.isocean = 'N' AND a.isalaskan = 'N')
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
   -- Get the catchment details
   --------------------------------------------------------------------------
   IF str_statecode NOT IN ('AK','HI','PR','VI','GU','MP','AS')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN boo_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN boo_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_5070_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSIF str_statecode IN ('AK')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN boo_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN boo_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_3338_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSIF str_statecode IN ('HI')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN boo_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN boo_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_26904_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSIF str_statecode IN ('PR','VI')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN boo_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN boo_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_32161_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSIF str_statecode IN ('GU','MP')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN boo_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN boo_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_32655_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSIF str_statecode IN ('AS')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN boo_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN boo_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_32702_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.randomcatchment(
    VARCHAR
   ,BOOLEAN
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.randomcatchment(
    VARCHAR
   ,BOOLEAN
   ,BOOLEAN
) TO PUBLIC;

