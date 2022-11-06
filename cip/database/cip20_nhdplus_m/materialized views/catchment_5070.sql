DROP MATERIALIZED VIEW IF EXISTS cip20_nhdplus_m.catchment_5070 CASCADE;

CREATE MATERIALIZED VIEW cip20_nhdplus_m.catchment_5070(
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
   ,ST_UNION(ST_Transform(aa.shape,5070)) AS shape
   FROM
   cip20_nhdplus_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode NOT IN ('AK','HI','PR','VI','GU','MP','AS')
   GROUP BY
   aa.nhdplusid::BIGINT
) a
LEFT JOIN
cip20_nhdplus_m.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cip20_nhdplus_m.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cip20_nhdplus_m.catchment_5070 OWNER TO cip20;
GRANT SELECT ON cip20_nhdplus_m.catchment_5070 TO public;

CREATE UNIQUE INDEX catchment_5070_01u
ON cip20_nhdplus_m.catchment_5070(nhdplusid);

CREATE UNIQUE INDEX catchment_5070_02u
ON cip20_nhdplus_m.catchment_5070(hydroseq);

CREATE INDEX catchment_5070_01i
ON cip20_nhdplus_m.catchment_5070(levelpathi);

CREATE INDEX catchment_5070_02i
ON cip20_nhdplus_m.catchment_5070(fcode);

CREATE INDEX catchment_5070_spx
ON cip20_nhdplus_m.catchment_5070 USING GIST(shape);

CREATE INDEX catchment_5070_spx2
ON cip20_nhdplus_m.catchment_5070 USING GIST(shape_centroid);

ANALYZE cip20_nhdplus_m.catchment_5070;

--VACUUM FREEZE ANALYZE cip20_nhdplus_m.catchment_5070;

