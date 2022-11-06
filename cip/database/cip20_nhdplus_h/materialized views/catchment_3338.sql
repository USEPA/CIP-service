DROP MATERIALIZED VIEW IF EXISTS cip20_nhdplus_h.catchment_3338 CASCADE;

CREATE MATERIALIZED VIEW cip20_nhdplus_h.catchment_3338(
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
   cip20_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('AK')
   GROUP BY
   aa.nhdplusid::BIGINT
) a
LEFT JOIN
cip20_nhdplus_h.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cip20_nhdplus_h.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cip20_nhdplus_h.catchment_3338 OWNER TO cip20;
GRANT SELECT ON cip20_nhdplus_h.catchment_3338 TO public;

CREATE UNIQUE INDEX catchment_3338_01u
ON cip20_nhdplus_h.catchment_3338(nhdplusid);

CREATE UNIQUE INDEX catchment_3338_02u
ON cip20_nhdplus_h.catchment_3338(hydroseq);

CREATE INDEX catchment_3338_01i
ON cip20_nhdplus_h.catchment_3338(levelpathi);

CREATE INDEX catchment_3338_02i
ON cip20_nhdplus_h.catchment_3338(fcode);

CREATE INDEX catchment_3338_spx
ON cip20_nhdplus_h.catchment_3338 USING GIST(shape);

CREATE INDEX catchment_3338_spx2
ON cip20_nhdplus_h.catchment_3338 USING GIST(shape_centroid);

ANALYZE cip20_nhdplus_h.catchment_3338;

--VACUUM FREEZE ANALYZE cip20_nhdplus_h.catchment_3338;

