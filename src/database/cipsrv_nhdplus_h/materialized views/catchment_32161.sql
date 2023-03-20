DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_32161(
    nhdplusid
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
   ,isnavigable
   ,iscoastal
   ,isocean
   ---
   ,areasqkm
   ,shape
   ,shape_centroid
)
AS
SELECT
 a.nhdplusid
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
,a.isnavigable
,a.iscoastal
,a.isocean
---
,a.areasqkm
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    CAST(aa.nhdplusid AS BIGINT) AS nhdplusid
   ,bool_or(CASE WHEN aa.istribal = 'Y'    THEN TRUE ELSE FALSE END) AS istribal
   ,bool_or(CASE WHEN aa.isnavigable = 'Y' THEN TRUE ELSE FALSE END) AS isnavigable
   ,bool_or(CASE WHEN aa.iscoastal   = 'Y' THEN TRUE ELSE FALSE END) AS iscoastal
   ,bool_or(CASE WHEN aa.isocean     = 'Y' THEN TRUE ELSE FALSE END) AS isocean
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,32161)) AS shape
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('PR','VI')
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

ALTER TABLE cipsrv_nhdplus_h.catchment_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32161 TO public;

CREATE UNIQUE INDEX catchment_32161_01u
ON cipsrv_nhdplus_h.catchment_32161(nhdplusid);

CREATE UNIQUE INDEX catchment_32161_02u
ON cipsrv_nhdplus_h.catchment_32161(hydroseq);

CREATE INDEX catchment_32161_01i
ON cipsrv_nhdplus_h.catchment_32161(levelpathi);

CREATE INDEX catchment_32161_02i
ON cipsrv_nhdplus_h.catchment_32161(fcode);

CREATE INDEX catchment_32161_03i
ON cipsrv_nhdplus_h.catchment_32161(istribal);

CREATE INDEX catchment_32161_04i
ON cipsrv_nhdplus_h.catchment_32161(isnavigable);

CREATE INDEX catchment_32161_05i
ON cipsrv_nhdplus_h.catchment_32161(iscoastal);

CREATE INDEX catchment_32161_06i
ON cipsrv_nhdplus_h.catchment_32161(isocean);

CREATE INDEX catchment_32161_spx
ON cipsrv_nhdplus_h.catchment_32161 USING GIST(shape);

CREATE INDEX catchment_32161_spx2
ON cipsrv_nhdplus_h.catchment_32161 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_h.catchment_32161;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_32161;

