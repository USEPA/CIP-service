CREATE OR REPLACE FUNCTION cip20_nhdplus_m.index_line_simple(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_lengthkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_line_threashold_perc    NUMERIC
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                 RECORD;
   str_known_region    VARCHAR;
   int_srid            INTEGER;
   geom_input          GEOMETRY;
   num_lin_threshold   NUMERIC;
   int_count           INTEGER;

BEGIN

   IF p_line_threashold_perc IS NULL
   THEN
      num_lin_threshold := 0;
   
   ELSE
      num_lin_threshold := p_line_threashold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

   rec := cip20_nhdplus_m.determine_grid_srid(
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

   IF str_known_region = '5070'
   THEN
      geom_input := ST_Transform(p_geometry,5070);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
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
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cip20_nhdplus_m.catchment_5070 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;

   ELSIF str_known_region = '3338'
   THEN
      geom_input := ST_Transform(p_geometry,3338);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
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
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cip20_nhdplus_m.catchment_3338 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input := ST_Transform(p_geometry,26904);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
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
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cip20_nhdplus_m.catchment_26904 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input := ST_Transform(p_geometry,32161);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
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
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cip20_nhdplus_m.catchment_32161 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input := ST_Transform(p_geometry,32655);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
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
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cip20_nhdplus_m.catchment_32655 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input := ST_Transform(p_geometry,32702);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
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
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cip20_nhdplus_m.catchment_32702 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;

   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_nhdplus_m.index_line_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_nhdplus_m.index_line_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
) TO PUBLIC;

