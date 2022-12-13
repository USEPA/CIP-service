DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.catchment_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.catchment_3338(
    nhdplusid
   ,areasqkm
   ---
   ,hydroseq
   ,levelpathi
   ,lengthkm
   ,tonode
   ,fromnode
   ,connector_tonode
   ,connector_fromnode
   ---
   ,fcode
   ---
   ,shape
   ,shape_centroid
)
AS
SELECT
 a.nhdplusid::BIGINT        AS nhdplusid
,a.areasqkm
---
,b.hydroseq
,b.levelpathi
,b.lengthkm
,b.tonode
,b.fromnode
,b.connector_tonode
,b.connector_fromnode
---
,c.fcode::INTEGER           AS fcode
---
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    aa.nhdplusid::BIGINT
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,3338)) AS shape
   FROM
   cipsrv_nhdplus_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('AK')
   GROUP BY
   aa.nhdplusid::BIGINT
) a
LEFT JOIN
cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_m.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_m.catchment_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_3338 TO public;

CREATE UNIQUE INDEX catchment_3338_01u
ON cipsrv_nhdplus_m.catchment_3338(nhdplusid);

CREATE UNIQUE INDEX catchment_3338_02u
ON cipsrv_nhdplus_m.catchment_3338(hydroseq);

CREATE INDEX catchment_3338_01i
ON cipsrv_nhdplus_m.catchment_3338(levelpathi);

CREATE INDEX catchment_3338_02i
ON cipsrv_nhdplus_m.catchment_3338(fcode);

CREATE INDEX catchment_3338_spx
ON cipsrv_nhdplus_m.catchment_3338 USING GIST(shape);

CREATE INDEX catchment_3338_spx2
ON cipsrv_nhdplus_m.catchment_3338 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_m.catchment_3338;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.catchment_3338;

