DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.index_area_simple';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.index_area_simple(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_areasqkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_cat_threshold_perc      NUMERIC
   ,IN  p_evt_threshold_perc      NUMERIC
   ,IN  p_permid_joinkey          UUID
   ,IN  p_permid_geometry         GEOMETRY
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
   num_cat_threshold      NUMERIC;
   num_evt_threshold      NUMERIC;
   num_geometry_areasqkm  NUMERIC;
   permid_geometry        GEOMETRY;

BEGIN

   IF p_cat_threshold_perc IS NULL
   THEN
      num_cat_threshold := 0;
   
   ELSE
      num_cat_threshold := p_cat_threshold_perc / 100;
      
   END IF;
   
   IF p_evt_threshold_perc IS NULL
   THEN
      num_evt_threshold := 0;
   
   ELSE
      num_evt_threshold := p_evt_threshold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

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
   
   IF p_geometry_areasqkm IS NULL
   THEN
      num_geometry_areasqkm := ROUND(ST_Area(ST_Transform(
          p_geometry
         ,int_srid
      ))::NUMERIC / 1000000,8);
      
   ELSE
      num_geometry_areasqkm := p_geometry_areasqkm;
      
   END IF;
      
   IF str_known_region = '5070'
   THEN
      geom_input      := ST_Transform(p_geometry,5070);
      permid_geometry := ST_Transform(p_permid_geometry,5070);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_5070_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '3338'
   THEN
      geom_input      := ST_Transform(p_geometry,3338);
      permid_geometry := ST_Transform(p_permid_geometry,3338);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )  
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_3338_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input      := ST_Transform(p_geometry,26904);
      permid_geometry := ST_Transform(p_permid_geometry,26904);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_26904_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input      := ST_Transform(p_geometry,32161);
      permid_geometry := ST_Transform(p_permid_geometry,32161);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32161_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input      := ST_Transform(p_geometry,32655);
      permid_geometry := ST_Transform(p_permid_geometry,32655);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )  
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32655_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input      := ST_Transform(p_geometry,32702);
      permid_geometry := ST_Transform(p_permid_geometry,32702);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )  
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32702_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.index_area_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.index_area_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) TO PUBLIC;

