DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.index_line_levelpath';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.index_line_levelpath(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_lengthkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_line_threshold_perc     NUMERIC
   ,IN  p_permid_joinkey          UUID
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   str_known_region       VARCHAR;
   int_srid               INTEGER;
   geom_input             GEOMETRY;
   geom_part              GEOMETRY;
   num_line_threshold     NUMERIC;
   int_count              INTEGER;
   int_count2             INTEGER;
   num_main_levelpathi    BIGINT;
   ary_main_lp_int_nodes  BIGINT[];
   ary_done_levelpathis   BIGINT[];
   num_fromnode           BIGINT;
   num_tonode             BIGINT;
   num_connector_fromnode BIGINT;
   num_connector_tonode   BIGINT;
   num_min_hydroseq       BIGINT;
   num_max_hydroseq       BIGINT;
   boo_check              BOOLEAN;
   int_debug              INTEGER;
   str_debug              VARCHAR;
   int_geom_count         INTEGER;   
   num_geometry_lengthkm  NUMERIC;

BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_line_threshold_perc IS NULL
   THEN
      num_line_threshold := 0;
   
   ELSE
      num_line_threshold := p_line_threshold_perc / 100;
      
   END IF;
   
   str_known_region := p_known_region;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Validate the known region
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
   
   IF num_geometry_lengthkm IS NULL
   THEN
      num_geometry_lengthkm := ROUND(ST_Length(ST_Transform(
          p_geometry
         ,int_srid
      ))::NUMERIC * 0.001,8);
      
   ELSE
      num_geometry_lengthkm := p_geometry_lengthkm;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Loop through any multi-part linestrings
   ----------------------------------------------------------------------------
   int_geom_count := ST_NumGeometries(p_geometry);
   FOR i IN 1 .. int_geom_count
   LOOP
      out_return_code := cipsrv_nhdplus_m.create_line_temp_tables();
      geom_part := ST_GeometryN(p_geometry,i);
      
   ----------------------------------------------------------------------------
   -- Step 40
   -- Load the temp table
   ----------------------------------------------------------------------------      
      IF str_known_region = '5070'
      THEN
         geom_input := ST_Transform(geom_part,5070);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_m.catchment_5070 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '3338'
      THEN
         geom_input := ST_Transform(geom_part,3338);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_m.catchment_3338 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
      
      ELSIF str_known_region = '26904'
      THEN
         geom_input := ST_Transform(geom_part,26904);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_m.catchment_26904 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '32161'
      THEN
         geom_input := ST_Transform(geom_part,32161);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_m.catchment_32161 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '32655'
      THEN
         geom_input := ST_Transform(geom_part,32655);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_m.catchment_32655 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '32702'
      THEN
         geom_input := ST_Transform(geom_part,32702);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_m.catchment_32702 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
      
      ELSE
         RAISE EXCEPTION 'err %',str_known_region;
         
      END IF;
      
      GET DIAGNOSTICS int_count = ROW_COUNT;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Determine the best fit mainpath right upfront
   -- Collect other levelpathis excluding any that touch interior of main
   ----------------------------------------------------------------------------
      IF int_count > 0
      THEN
      
         SELECT
          a.levelpathi
         ,MAX(a.hydroseq)
         ,MIN(a.hydroseq)
         INTO
          num_main_levelpathi
         ,num_max_hydroseq
         ,num_min_hydroseq
         FROM
         tmp_line a
         WHERE
            a.isnavigable
         OR (num_line_threshold IS NULL OR a.nhdpercentage > num_line_threshold)
         OR a.eventpercentage = 1
         GROUP BY
         a.levelpathi
         ORDER BY
         SUM(a.eventpercentage) DESC
         LIMIT 1;
         
         SELECT ARRAY(
            SELECT 
            a.tonode::BIGINT
            FROM
            cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes a
            WHERE
                a.levelpathi = num_main_levelpathi
            AND a.hydroseq >= num_min_hydroseq
            AND a.hydroseq <  num_max_hydroseq
         ) 
         INTO ary_main_lp_int_nodes;
         
         INSERT INTO tmp_line_levelpathi(
             levelpathi
            ,max_hydroseq
            ,min_hydroseq
            ,totaleventpercentage
            ,totaloverlapmeasure
            ,levelpathilengthkm     
            ,fromnode
            ,tonode
            ,connector_fromnode
            ,connector_tonode
         )
         SELECT
          a.levelpathi
         ,a.max_hydroseq
         ,a.min_hydroseq
         ,a.totaleventpercentage
         ,a.totaloverlapmeasure
         ,b.levelpathilengthkm
         ,c.fromnode
         ,d.tonode
         ,c.connector_fromnode
         ,d.connector_tonode
         FROM (
            SELECT
             aa.levelpathi
            ,MAX(aa.hydroseq)        AS max_hydroseq
            ,MIN(aa.hydroseq)        AS min_hydroseq
            ,SUM(aa.eventpercentage) AS totaleventpercentage
            ,SUM(aa.overlapmeasure)  AS totaloverlapmeasure
            FROM
            tmp_line aa
            WHERE (
                  aa.levelpathi = num_main_levelpathi
               OR NOT (aa.tonode = ANY(ary_main_lp_int_nodes))
            )
            AND ( 
               (num_line_threshold IS NULL OR aa.nhdpercentage > num_line_threshold)
               OR aa.eventpercentage = 1
            )
            GROUP BY
            aa.levelpathi
         ) a
         JOIN
         cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi b
         ON
         a.levelpathi = b.levelpathi
         JOIN
         tmp_line c
         ON
         a.max_hydroseq = c.hydroseq
         JOIN
         tmp_line d
         ON
         a.min_hydroseq = d.hydroseq;
         
         GET DIAGNOSTICS int_count = ROW_COUNT;

         INSERT INTO tmp_cip(
             permid_joinkey
            ,nhdplusid
         ) 
         SELECT
          p_permid_joinkey         
         ,a.nhdplusid
         FROM
         cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes a
         WHERE
             a.levelpathi = num_main_levelpathi
         AND a.hydroseq >= num_min_hydroseq
         AND a.hydroseq <= num_max_hydroseq
         ON CONFLICT DO NOTHING;
         
         GET DIAGNOSTICS int_count2 = ROW_COUNT;
         
         -- This is a bit dodgy but if the input is entirely outside the scope of navigable catchments
         -- e.g. out in the ocean or emulating a coastal flowline, then provide back the spatial intersect
         IF int_count2 = 0
         THEN
            INSERT INTO tmp_cip(
                permid_joinkey
               ,nhdplusid
            ) 
            SELECT 
             p_permid_joinkey
            ,a.nhdplusid
            FROM
            tmp_line a
            WHERE
            NOT a.isnavigable
            ON CONFLICT DO NOTHING;
            
         END IF;
 
   ----------------------------------------------------------------------------
   -- Step 70
   -- Loop through the additional levelpathis extending main levelpathi
   ----------------------------------------------------------------------------
         IF int_count > 1
         THEN
            SELECT
             a.levelpathi
            ,a.max_hydroseq
            ,a.min_hydroseq 
            ,a.fromnode
            ,a.tonode
            ,a.connector_fromnode
            ,a.connector_tonode
            INTO
             num_main_levelpathi
            ,num_max_hydroseq
            ,num_min_hydroseq
            ,num_fromnode
            ,num_tonode
            ,num_connector_fromnode
            ,num_connector_tonode
            FROM
            tmp_line_levelpathi a
            WHERE
            a.levelpathi = num_main_levelpathi;
            
            ary_done_levelpathis := '{}';
            
            <<sanity_loop>>
            FOR i IN 1 .. 10
            LOOP
               FOR rec IN (
                  SELECT
                   a.levelpathi
                  ,a.max_hydroseq
                  ,a.min_hydroseq 
                  ,a.fromnode
                  ,a.tonode
                  ,a.connector_fromnode
                  ,a.connector_tonode
                  FROM
                  tmp_line_levelpathi a
                  WHERE
                  a.levelpathi != num_main_levelpathi
                  AND NOT (a.levelpathi = ANY(ary_done_levelpathis))
               ) LOOP
                  boo_check := FALSE;
                  
                  IF num_fromnode IN (rec.fromnode,rec.connector_fromnode)
                  OR num_connector_fromnode IN (rec.fromnode,rec.connector_fromnode)
                  THEN
                     boo_check := TRUE;
                     num_fromnode := rec.tonode;
                     num_connector_fromnode := rec.connector_tonode;
                  
                  ELSIF num_fromnode IN (rec.tonode,rec.connector_tonode)
                  OR num_connector_fromnode IN (rec.tonode,rec.connector_tonode)
                  THEN
                     boo_check := TRUE;
                     num_fromnode := rec.fromnode;
                     num_connector_fromnode := rec.connector_fromnode;
                     
                  ELSIF num_tonode IN (rec.fromnode,rec.connector_fromnode)
                  OR num_connector_tonode IN (rec.fromnode,rec.connector_fromnode)
                  THEN
                     boo_check := TRUE;
                     num_tonode := rec.tonode;
                     num_connector_tonode := rec.connector_tonode;
                  
                  ELSIF num_tonode IN (rec.tonode,rec.connector_tonode)
                  OR num_connector_tonode IN (rec.tonode,rec.connector_tonode)
                  THEN
                     boo_check := TRUE;
                     num_tonode := rec.fromnode;
                     num_connector_tonode := rec.connector_fromnode;   
                     
                  END IF;
                  
                  IF boo_check
                  THEN
                     ary_done_levelpathis := array_append(ary_done_levelpathis,rec.levelpathi);
                     
                     INSERT INTO tmp_cip(
                         permid_joinkey
                        ,nhdplusid
                     ) 
                     SELECT
                      p_permid_joinkey
                     ,a.nhdplusid
                     FROM
                     cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes a
                     WHERE
                         a.levelpathi = rec.levelpathi
                     AND a.hydroseq >= rec.min_hydroseq
                     AND a.hydroseq <= rec.max_hydroseq
                     ON CONFLICT DO NOTHING;
                  
                  ELSE
                     EXIT sanity_loop;
                     
                  END IF;
                  
               END LOOP;
               
            END LOOP;
            
         END IF;
         
      END IF;

   END LOOP;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.index_line_levelpath(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,UUID
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.index_line_levelpath(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,UUID
) TO PUBLIC;

