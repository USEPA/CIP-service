DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.catchment_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.catchment_32655(
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
   ,istribal
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
,a.istribal
---
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    aa.nhdplusid::BIGINT
   ,bool_or(CASE WHEN aa.istribal = 'Y' THEN TRUE ELSE FALSE END) AS istribal
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,32655)) AS shape
   FROM
   cipsrv_nhdplus_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('GU','MP')
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

ALTER TABLE cipsrv_nhdplus_m.catchment_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_32655 TO public;

CREATE UNIQUE INDEX catchment_32655_01u
ON cipsrv_nhdplus_m.catchment_32655(nhdplusid);

CREATE UNIQUE INDEX catchment_32655_02u
ON cipsrv_nhdplus_m.catchment_32655(hydroseq);

CREATE INDEX catchment_32655_01i
ON cipsrv_nhdplus_m.catchment_32655(levelpathi);

CREATE INDEX catchment_32655_02i
ON cipsrv_nhdplus_m.catchment_32655(fcode);

CREATE INDEX catchment_32655_03i
ON cipsrv_nhdplus_m.catchment_32655(istribal);

CREATE INDEX catchment_32655_spx
ON cipsrv_nhdplus_m.catchment_32655 USING GIST(shape);

CREATE INDEX catchment_32655_spx2
ON cipsrv_nhdplus_m.catchment_32655 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_m.catchment_32655;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.catchment_32655;

