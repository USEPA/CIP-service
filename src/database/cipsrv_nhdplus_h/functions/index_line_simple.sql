DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.index_line_simple';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.index_line_simple(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_lengthkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_line_threshold_perc     NUMERIC
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
   num_line_threshold     NUMERIC;
   int_count              INTEGER;
   num_geometry_lengthkm  NUMERIC;
   permid_geometry        GEOMETRY;

BEGIN

   IF p_line_threshold_perc IS NULL
   THEN
      num_line_threshold := 0;
   
   ELSE
      num_line_threshold := p_line_threshold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_h.determine_grid_srid(
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

   IF p_geometry_lengthkm IS NULL
   THEN
      num_geometry_lengthkm := ROUND(ST_Length(ST_Transform(
          p_geometry
         ,int_srid
      ))::NUMERIC * 0.001,8);
      
   ELSE
      num_geometry_lengthkm := p_geometry_lengthkm;
      
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
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
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
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
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
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
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
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
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
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
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
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
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
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
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
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
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
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
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
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
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
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
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
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
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

ALTER FUNCTION cipsrv_nhdplus_h.index_line_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.index_line_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) TO PUBLIC;

