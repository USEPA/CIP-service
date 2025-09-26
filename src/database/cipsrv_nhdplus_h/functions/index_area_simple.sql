DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.index_area_simple';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.index_area_simple(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_areasqkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_cat_threshold_perc      NUMERIC
   ,IN  p_evt_threshold_perc      NUMERIC
   ,IN  p_permid_joinkey          UUID
   ,IN  p_permid_geometry         GEOMETRY
   ,IN  p_statesplit              INTEGER DEFAULT NULL
   ,OUT out_known_region          VARCHAR
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   int_srid               INTEGER;
   geom_input             GEOMETRY;
   num_cat_threshold      NUMERIC;
   num_evt_threshold      NUMERIC;
   num_geometry_areasqkm  NUMERIC;
   permid_geometry        GEOMETRY;
   int_splitselector      INTEGER;

BEGIN

   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
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
   
   IF p_statesplit IS NULL
   OR p_statesplit NOT IN (1,2)
   THEN
      int_splitselector := 1;
      
   ELSE
      int_splitselector := p_statesplit;
   
   END IF;

   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   out_known_region   := int_srid::VARCHAR;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   IF p_geometry_areasqkm IS NULL
   THEN
      num_geometry_areasqkm := ROUND(ST_Area(ST_Transform(
          p_geometry
         ,int_srid
      ))::NUMERIC / 1000000,8);
      
   ELSE
      num_geometry_areasqkm := p_geometry_areasqkm;
      
   END IF;
      
   ----------------------------------------------------------------------------
   IF out_known_region = '5070'
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
               cipsrv_nhdplus_h.catchment_5070_full aaaa
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
   
   ELSIF out_known_region = '3338'
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
               cipsrv_nhdplus_h.catchment_3338_full aaaa
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
   
   ELSIF out_known_region = '26904'
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
               cipsrv_nhdplus_h.catchment_26904_full aaaa
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
      
   ELSIF out_known_region = '32161'
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
               cipsrv_nhdplus_h.catchment_32161_full aaaa
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
      
   ELSIF out_known_region = '32655'
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
               cipsrv_nhdplus_h.catchment_32655_full aaaa
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
      
   ELSIF out_known_region = '32702'
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
               cipsrv_nhdplus_h.catchment_32702_full aaaa
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
      out_status_message := 'err ' || out_known_region;
      
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.index_area_simple';
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

