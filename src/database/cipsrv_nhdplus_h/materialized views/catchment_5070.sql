DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_5070(
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
   ,tribal
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
,a.tribal
---
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    aa.nhdplusid::BIGINT
   ,bool_or(aa.tribal) AS tribal
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,5070)) AS shape
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode NOT IN ('AK','HI','PR','VI','GU','MP','AS')
   GROUP BY
   aa.nhdplusid::BIGINT
) a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_h.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_h.catchment_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_5070 TO public;

CREATE UNIQUE INDEX catchment_5070_01u
ON cipsrv_nhdplus_h.catchment_5070(nhdplusid);

CREATE UNIQUE INDEX catchment_5070_02u
ON cipsrv_nhdplus_h.catchment_5070(hydroseq);

CREATE INDEX catchment_5070_01i
ON cipsrv_nhdplus_h.catchment_5070(levelpathi);

CREATE INDEX catchment_5070_02i
ON cipsrv_nhdplus_h.catchment_5070(fcode);

CREATE INDEX catchment_5070_03i
ON cipsrv_nhdplus_h.catchment_5070(tribal);

CREATE INDEX catchment_5070_spx
ON cipsrv_nhdplus_h.catchment_5070 USING GIST(shape);

CREATE INDEX catchment_5070_spx2
ON cipsrv_nhdplus_h.catchment_5070 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_h.catchment_5070;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_5070;

