DROP MATERIALIZED VIEW IF EXISTS cipdev_support.epageofab_m_catchment_istribal_32655 CASCADE;

DROP SEQUENCE IF EXISTS cipdev_support.epageofab_m_catchment_istribal_32655_seq;
CREATE SEQUENCE cipdev_support.epageofab_m_catchment_istribal_32655_seq START WITH 1;

CREATE MATERIALIZED VIEW cipdev_support.epageofab_m_catchment_istribal_32655(
    objectid
   ,nhdplusid
   ,catchmentstatecode
   ,areasqkm
   ,overlap_areasqkm
   ,overlap_percentage
   ,istribal
   ,shape
)
AS
SELECT
 NEXTVAL('cipdev_support.epageofab_m_catchment_istribal_32655_seq')::INTEGER AS objectid
,a.nhdplusid
,a.catchmentstatecode
,a.areasqkm
,a.overlap_areasqkm
,a.overlap_percentage
,CASE 
 WHEN a.overlap_percentage > 0.999
 THEN
   CAST('F' AS VARCHAR(1)) 
 ELSE
   CAST('P' AS VARCHAR(1))
 END AS istribal
,a.overlap_shape AS shape
FROM (
   SELECT
    aa.nhdplusid
   ,aa.catchmentstatecode
   ,aa.areasqkm
   ,aa.overlap_areasqkm
   ,(aa.overlap_areasqkm / aa.areasqkm)::NUMERIC AS overlap_percentage
   ,aa.overlap_shape
   FROM (   
      SELECT
       aaa.nhdplusid
      ,aaa.catchmentstatecode
      ,MAX(aaa.areasqkm)        AS areasqkm
      ,MAX(aaa.tribal_stusps)   AS tribal_stusps
      ,MAX(aaa.tribal_areasqkm) AS tribal_areasqkm
      ,MAX(aaa.tribal_cid)      AS tribal_cid
      ,SUM(public.ST_AREA(
         public.ST_TRANSFORM(aaa.overlap_shape,4326)::GEOGRAPHY
       )::NUMERIC / 1000000) AS overlap_areasqkm
      ,public.ST_COLLECTIONEXTRACT(
          public.ST_UNION(
             cipsrv_nhdplus_m.snap_to_common_grid(
                p_geometry      := aaa.overlap_shape
               ,p_known_region  := '32655'
               ,p_grid_size     := 0.001
             )
          )
         ,3
       ) AS overlap_shape
      FROM (
         SELECT
          aaaa.nhdplusid
         ,aaaa.catchmentstatecodes[1] AS catchmentstatecode
         ,aaaa.areasqkm
         ,bbbb.stusps   AS tribal_stusps
         ,bbbb.areasqkm AS tribal_areasqkm
         ,bbbb.cid      AS tribal_cid
         ,public.ST_COLLECTIONEXTRACT(
             public.ST_INTERSECTION(
                 cipsrv_nhdplus_m.snap_to_common_grid(
                   p_geometry      := bbbb.shape
                  ,p_known_region  := '32655'
                  ,p_grid_size     := 0.001
                 )
                ,aaaa.shape
             )
            ,3
          ) AS overlap_shape
         FROM 
         cipsrv_nhdplus_m.catchment_32655 aaaa
         INNER JOIN LATERAL (
            SELECT
             bbbbb.stusps
            ,bbbbb.cid
            ,bbbbb.areasqkm
            ,bbbbb.shape
            FROM
            cipdev_support.epa_segs_air_flat_32655_m bbbbb
         ) AS bbbb
         ON
         public.ST_INTERSECTS(bbbb.shape,aaaa.shape)
         WHERE
             aaaa.statesplit IN (0,1)
         AND aaaa.areasqkm > 0
      ) aaa
      WHERE
          aaa.overlap_shape IS NOT NULL
      AND NOT public.ST_ISEMPTY(aaa.overlap_shape)
      GROUP BY
       aaa.catchmentstatecode
      ,aaa.nhdplusid
   ) aa
   WHERE
       aa.overlap_areasqkm > 0.00001
   AND aa.overlap_areasqkm / aa.areasqkm > 0.0001
) a;

ALTER TABLE cipdev_support.epageofab_m_catchment_istribal_32655 OWNER TO cipsrv;
GRANT SELECT ON cipdev_support.epageofab_m_catchment_istribal_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS epageofab_m_catchment_istribal_32655_pk
ON cipdev_support.epageofab_m_catchment_istribal_32655(catchmentstatecode,nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS epageofab_m_catchment_istribal_32655_u01
ON cipdev_support.epageofab_m_catchment_istribal_32655(objectid);

CREATE INDEX IF NOT EXISTS epageofab_m_catchment_istribal_32655_spx
ON cipdev_support.epageofab_m_catchment_istribal_32655 USING gist(shape);

ANALYZE cipdev_support.epageofab_m_catchment_istribal_32655;
