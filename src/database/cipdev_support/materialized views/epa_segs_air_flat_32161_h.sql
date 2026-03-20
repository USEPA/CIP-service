DROP MATERIALIZED VIEW IF EXISTS cipdev_support.epa_segs_air_flat_32161_h CASCADE;

DROP SEQUENCE IF EXISTS cipdev_support.epa_segs_air_flat_32161_h_seq;
CREATE SEQUENCE cipdev_support.epa_segs_air_flat_32161_h_seq START WITH 1;

CREATE MATERIALIZED VIEW cipdev_support.epa_segs_air_flat_32161_h(
 objectid
,stusps
,cid
,areasqkm
,ptcount
,shape
)
AS
SELECT
 NEXTVAL('cipdev_support.epa_segs_air_flat_32161_h_seq')::INTEGER AS objectid
,a.stusps
,a.cid
,public.ST_AREA(
   public.ST_TRANSFORM(a.shape,4326)::GEOGRAPHY
 ) / 1000000 AS areasqkm
,public.ST_NPOINTS(a.shape) AS ptcount
,a.shape
FROM (
   /* break overly large polygons into smaller parts */
   SELECT
    aa.stusps
   ,aa.cid
   ,public.ST_SUBDIVIDE(aa.shape,10000) AS shape
   FROM (
      /* unload any multipolygons into single polygons */
      SELECT
       aaa.stusps
      ,aaa.cid
      ,(public.ST_DUMP(aaa.shape)).geom AS shape
      FROM (
         /* break contiguous polygons by state */
         SELECT
          bbbb.stusps
         ,aaaa.cid
         ,public.ST_COLLECTIONEXTRACT(
             public.ST_INTERSECTION(
                 cipsrv_nhdplus_h.snap_to_common_grid(
                    p_geometry      := bbbb.shape
                   ,p_known_region  := '32161'
                   ,p_grid_size     := 0.001
                 )
                ,aaaa.shape
             )
            ,3
          ) AS shape
         FROM (
            /* merge and flatten into contiguous polygons */
            SELECT
             aaaaa.cid
            ,public.ST_COLLECTIONEXTRACT(
                public.ST_UNION(
                  cipsrv_nhdplus_h.snap_to_common_grid(
                       p_geometry      := aaaaa.shape
                      ,p_known_region  := '32161'
                      ,p_grid_size     := 0.001
                  )
                )
               ,3
            ) AS shape
            FROM (
               /* tag reservation lands that touch eachother */
               SELECT
                public.ST_ClusterDBSCAN(
                   aaaaaa.shape
                  ,eps       => 0
                  ,minpoints => 1
                ) OVER () AS cid
               ,aaaaaa.shape   
               FROM
               cipdev_support.epa_segs_air_32161 aaaaaa
            ) aaaaa
            GROUP BY
            aaaaa.cid
         ) aaaa
         INNER JOIN LATERAL (
            SELECT
             bbbbb.stusps
            ,bbbbb.shape
            FROM
            cipsrv_support.tiger_fedstatewaters_32161 bbbbb
         ) AS bbbb
         ON
         public.ST_INTERSECTS(bbbb.shape,aaaa.shape)
      ) aaa
   ) aa
) a;

ALTER TABLE cipdev_support.epa_segs_air_flat_32161_h OWNER TO cipsrv;
GRANT SELECT ON cipdev_support.epa_segs_air_flat_32161_h TO public;

CREATE UNIQUE INDEX IF NOT EXISTS epa_segs_air_flat_32161_h_pk
ON cipdev_support.epa_segs_air_flat_32161_h(objectid);

CREATE INDEX IF NOT EXISTS epa_segs_air_flat_32161_h_spx
ON cipdev_support.epa_segs_air_flat_32161_h USING gist(shape);

ANALYZE cipdev_support.epa_segs_air_flat_32161_h;
