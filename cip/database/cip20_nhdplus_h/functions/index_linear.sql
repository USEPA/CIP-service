CREATE OR REPLACE FUNCTION cip20_nhdplus_h.index_linear(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_lengthkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_linear_threashold_perc  NUMERIC
   ,OUT p_return_code             INTEGER
   ,OUT p_status_message          VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   str_known_region       VARCHAR;
   int_srid               INTEGER;
   geom_input             GEOMETRY;
   num_lin_threshold      NUMERIC;
   int_count              INTEGER;
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

BEGIN

   p_return_code := 0;
   
   -----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   -----------------------------------------------------------------------------
   IF p_linear_threashold_perc IS NULL
   THEN
      num_lin_threshold := 0;
   
   ELSE
      num_lin_threshold := p_linear_threashold_perc / 100;
      
   END IF;
   
   str_known_region := p_known_region;
   
   -----------------------------------------------------------------------------
   -- Step 20
   -- Flush or create the temp tables
   -----------------------------------------------------------------------------
   p_return_code := cip20_nhdplus_h.create_linear_temp_tables();
   
   -----------------------------------------------------------------------------
   -- Step 30
   -- Validate the known region
   -----------------------------------------------------------------------------
   rec := cip20_nhdplus_h.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid         := rec.p_srid;
   p_return_code    := rec.p_return_code;
   p_status_message := rec.p_status_message;
   
   IF p_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
    
   -----------------------------------------------------------------------------
   -- Step 40
   -- Load the temp table
   -----------------------------------------------------------------------------      
   IF str_known_region = '5070'
   THEN
      geom_input := ST_Transform(p_geometry,5070);
      
      INSERT INTO tmp_linear(
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
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.areasqkm
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
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
               FROM
               cip20_nhdplus_h.catchment_5070 aaaa
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
      geom_input := ST_Transform(p_geometry,3338);
      
      INSERT INTO tmp_linear(
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
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.areasqkm
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
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
               FROM
               cip20_nhdplus_h.catchment_3338 aaaa
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
      geom_input := ST_Transform(p_geometry,26904);
      
      INSERT INTO tmp_linear(
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
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.areasqkm
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
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
               FROM
               cip20_nhdplus_h.catchment_26904 aaaa
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
      geom_input := ST_Transform(p_geometry,32161);
      
      INSERT INTO tmp_linear(
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
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.areasqkm
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
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
               FROM
               cip20_nhdplus_h.catchment_32161 aaaa
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
      geom_input := ST_Transform(p_geometry,32655);
      
      INSERT INTO tmp_linear(
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
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.areasqkm
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
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
               FROM
               cip20_nhdplus_h.catchment_32655 aaaa
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
      geom_input := ST_Transform(p_geometry,32702);
      
      INSERT INTO tmp_linear(
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
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.areasqkm
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
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
               FROM
               cip20_nhdplus_h.catchment_32702 aaaa
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

   IF int_count = 0
   THEN
      p_return_code    := -10;
      p_status_message := 'No catchments found using input geometry';
      RETURN;
      
   END IF;

   -----------------------------------------------------------------------------
   -- Step 50
   -- Determine the best fit mainpath right upfront
   -- Collect other levelpathis excluding any that touch interior of main
   -----------------------------------------------------------------------------
   SELECT
    a.levelpathi
   ,MAX(a.hydroseq)
   ,MIN(a.hydroseq)
   INTO
    num_main_levelpathi
   ,num_max_hydroseq
   ,num_min_hydroseq
   FROM
   tmp_linear a
   WHERE
      (num_lin_threshold IS NULL OR a.nhdpercentage > num_lin_threshold)
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
      cip20_nhdplus_h.nhdplusflowlinevaa_catnodes a
      WHERE
          a.levelpathi = num_main_levelpathi
      AND a.hydroseq >= num_min_hydroseq
      AND a.hydroseq <  num_max_hydroseq
   ) 
   INTO ary_main_lp_int_nodes;
   
   INSERT INTO tmp_linear_levelpathi(
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
      tmp_linear aa
      WHERE (
            aa.levelpathi = num_main_levelpathi
         OR NOT (aa.tonode = ANY(ary_main_lp_int_nodes))
      )
      AND ( 
         (num_lin_threshold IS NULL OR aa.nhdpercentage > num_lin_threshold)
         OR aa.eventpercentage = 1
      )
      GROUP BY
      aa.levelpathi
   ) a
   JOIN
   cip20_nhdplus_h.nhdplusflowlinevaa_levelpathi b
   ON
   a.levelpathi = b.levelpathi
   JOIN
   tmp_linear c
   ON
   a.max_hydroseq = c.hydroseq
   JOIN
   tmp_linear d
   ON
   a.min_hydroseq = d.hydroseq;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;

   INSERT INTO tmp_cip(
      nhdplusid
   ) 
   SELECT 
   a.nhdplusid
   FROM
   cip20_nhdplus_h.nhdplusflowlinevaa_catnodes a
   WHERE
       a.levelpathi = num_main_levelpathi
   AND a.hydroseq >= num_min_hydroseq
   AND a.hydroseq <= num_max_hydroseq
   ON CONFLICT DO NOTHING;
   
   IF int_count < 2
   THEN
      -- If only one levelpathi, then exit   
      RETURN;
   
   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 70
   -- Loop through the additional levelpathis extending main levelpathi
   -----------------------------------------------------------------------------
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
   tmp_linear_levelpathi a
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
         tmp_linear_levelpathi a
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
               nhdplusid
            ) 
            SELECT 
            a.nhdplusid
            FROM
            cip20_nhdplus_h.nhdplusflowlinevaa_catnodes a
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

   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_nhdplus_h.index_linear(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_nhdplus_h.index_linear(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
) TO PUBLIC;

