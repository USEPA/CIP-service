DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.randomhuc12';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_support.randomhuc12(
    IN  p_region               VARCHAR DEFAULT NULL
   ,IN  p_source_dataset       VARCHAR DEFAULT 'NP21'
   ,IN  p_return_geometry      BOOLEAN DEFAULT FALSE
   ,IN  p_known_huc12          VARCHAR DEFAULT NULL
   ,OUT out_huc12              VARCHAR
   ,OUT out_huc12_name         VARCHAR
   ,OUT out_source_dataset     VARCHAR
   ,OUT out_shape              GEOMETRY
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   int_count       INTEGER;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Select a random huc12
   --------------------------------------------------------------------------
   IF p_source_dataset = 'NP21'
   THEN
      IF p_known_huc12 IS NOT NULL
      THEN
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         a.huc12 = p_known_huc12;
      
      ELSIF p_region IN ('CONUS','5070')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE 
         SUBSTR(a.huc12,1,2) NOT IN ('19','20','21','22');
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         SUBSTR(a.huc12,1,2) NOT IN (''19'',''20'',''21'',''22'')
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('ALASKA','AK','3338')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE 
         SUBSTR(a.huc12,1,2) = '19';
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         SUBSTR(a.huc12,1,2) = ''19''
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('HAWAII','HI','26904')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE 
         SUBSTR(a.huc12,1,2) = '20';
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         SUBSTR(a.huc12,1,2) = ''20''
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('PRVI','32161')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE 
         SUBSTR(a.huc12,1,2) = '21';
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         SUBSTR(a.huc12,1,2) = ''21''
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('GUAMMAR','GUMP','32655')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE 
         SUBSTR(a.huc12,1,4) IN ('2201','2202');
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         SUBSTR(a.huc12,1,4) IN (''2201'',''2202'')
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('AMSAMOA','AS','32702')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE 
         SUBSTR(a.huc12,1,4) IN ('2203');
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         SUBSTR(a.huc12,1,4) IN (''2203'')
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_np21 a
         WHERE
         a.huc12 = out_huc12;
         
      ELSE
         SELECT
          a.huc12
         ,a.name
         ,a.shape
         INTO
          out_huc12
         ,out_huc12_name
         ,out_shape
         FROM (
            SELECT
             aa.huc12
            ,aa.name
            ,CASE
             WHEN p_return_geometry
             THEN
               aa.shape
             ELSE
               NULL
             END AS shape
            FROM
            cipsrv_wbd.wbd_hu12_np21 aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         ORDER BY RANDOM()
         LIMIT 1;
      
      END IF;
   
   ELSIF p_source_dataset = 'NPHR'
   THEN
      IF p_known_huc12 IS NOT NULL
      THEN
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         a.huc12 = p_known_huc12;
      
      ELSIF p_region IN ('CONUS','5070')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE 
         SUBSTR(a.huc12,1,2) NOT IN ('19','20','21','22');
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         SUBSTR(a.huc12,1,2) NOT IN (''19'',''20'',''21'',''22'')
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('ALASKA','AK','3338')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE 
         SUBSTR(a.huc12,1,2) = '19';
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         SUBSTR(a.huc12,1,2) = ''19''
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('HAWAII','HI','26904')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE 
         SUBSTR(a.huc12,1,2) = '20';
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         SUBSTR(a.huc12,1,2) = ''20''
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('PRVI','32161')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE 
         SUBSTR(a.huc12,1,2) = '21';
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         SUBSTR(a.huc12,1,2) = ''21''
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('GUAMMAR','GUMP','32655')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE 
         SUBSTR(a.huc12,1,4) IN ('2201','2202');
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         SUBSTR(a.huc12,1,4) IN (''2201'',''2202'')
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('AMSAMOA','AS','32702')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE 
         SUBSTR(a.huc12,1,4) IN ('2203');
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         SUBSTR(a.huc12,1,4) IN (''2203'')
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr a
         WHERE
         a.huc12 = out_huc12;
         
      ELSE
         SELECT
          a.huc12
         ,a.name
         ,a.shape
         INTO
          out_huc12
         ,out_huc12_name
         ,out_shape
         FROM (
            SELECT
             aa.huc12
            ,aa.name
            ,CASE
             WHEN p_return_geometry
             THEN
               aa.shape
             ELSE
               NULL
             END AS shape
            FROM
            cipsrv_wbd.wbd_hu12_nphr aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         ORDER BY RANDOM()
         LIMIT 1;
         
      END IF;
      
   ELSIF p_source_dataset = 'F3'
   THEN
      IF p_known_huc12 IS NOT NULL
      THEN
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         a.huc12 = p_known_huc12;
      
      ELSIF p_region IN ('CONUS','5070')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE 
         SUBSTR(a.huc12,1,2) NOT IN ('19','20','21','22');
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         SUBSTR(a.huc12,1,2) NOT IN (''19'',''20'',''21'',''22'')
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('ALASKA','AK','3338')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE 
         SUBSTR(a.huc12,1,2) = '19';
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         SUBSTR(a.huc12,1,2) = ''19''
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('HAWAII','HI','26904')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE 
         SUBSTR(a.huc12,1,2) = '20';
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         SUBSTR(a.huc12,1,2) = ''20''
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('PRVI','32161')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE 
         SUBSTR(a.huc12,1,2) = '21';
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         SUBSTR(a.huc12,1,2) = ''21''
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('GUAMMAR','GUMP','32655')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE 
         SUBSTR(a.huc12,1,4) IN ('2201','2202');
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         SUBSTR(a.huc12,1,4) IN (''2201'',''2202'')
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         a.huc12 = out_huc12;
      
      ELSIF p_region IN ('AMSAMOA','AS','32702')
      THEN
         SELECT 
         COUNT(*) 
         INTO
         int_count
         FROM 
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE 
         SUBSTR(a.huc12,1,4) IN ('2203');
         
         EXECUTE '
         SELECT
         a.huc12
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         SUBSTR(a.huc12,1,4) IN (''2203'')
         OFFSET FLOOR(' || (RANDOM() * int_count)::VARCHAR || ')
         LIMIT 1'
         INTO out_huc12;
         
         SELECT
          a.name
         ,CASE
          WHEN p_return_geometry
          THEN
            a.shape
          ELSE
            NULL
          END AS shape
         INTO
          out_huc12_name
         ,out_shape
         FROM
         cipsrv_wbd.wbd_hu12_f3 a
         WHERE
         a.huc12 = out_huc12;
         
      ELSE
         SELECT
          a.huc12
         ,a.name
         ,a.shape
         INTO
          out_huc12
         ,out_huc12_name
         ,out_shape
         FROM (
            SELECT
             aa.huc12
            ,aa.name
            ,CASE
             WHEN p_return_geometry
             THEN
               aa.shape
             ELSE
               NULL
             END AS shape
            FROM
            cipsrv_wbd.wbd_hu12_f3 aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         ORDER BY RANDOM()
         LIMIT 1;
         
      END IF;
      
   ELSE
      out_return_code := -9;
      out_status_message := 'No existing WBD HUC12 datasource ' || p_source_dataset || ' found.';
      RETURN;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 30
   --
   --------------------------------------------------------------------------
   out_source_dataset := p_source_dataset;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.randomhuc12';
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


