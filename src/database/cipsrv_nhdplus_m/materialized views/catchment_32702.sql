DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.catchment_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.catchment_32702(
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
   ,hasvaa
   ,issink
   ,isheadwater
   ,iscoastal
   ,isocean
   ,isalaskan
   ,h3hexagonaddr
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
,a.hasvaa
,a.issink
,a.isheadwater
,a.iscoastal
,a.isocean
,a.isalaskan
,a.h3hexagonaddr
---
,a.areasqkm
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    CAST(aa.nhdplusid AS BIGINT) AS nhdplusid
   ,aa.istribal
   ,bool_or(CASE WHEN aa.isnavigable = 'Y' THEN TRUE ELSE FALSE END) AS isnavigable
   ,bool_or(CASE WHEN aa.hasvaa      = 'Y' THEN TRUE ELSE FALSE END) AS hasvaa
   ,bool_or(CASE WHEN aa.issink      = 'Y' THEN TRUE ELSE FALSE END) AS issink
   ,bool_or(CASE WHEN aa.isheadwater = 'Y' THEN TRUE ELSE FALSE END) AS isheadwater
   ,bool_or(CASE WHEN aa.iscoastal   = 'Y' THEN TRUE ELSE FALSE END) AS iscoastal
   ,bool_or(CASE WHEN aa.isocean     = 'Y' THEN TRUE ELSE FALSE END) AS isocean
   ,'N' AS isalaskan
   ,MAX(aa.h3hexagonaddr) AS h3hexagonaddr
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,32702)) AS shape
   FROM
   cipsrv_nhdplus_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('AS')
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

ALTER TABLE cipsrv_nhdplus_m.catchment_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_32702 TO public;

CREATE UNIQUE INDEX catchment_32702_01u
ON cipsrv_nhdplus_m.catchment_32702(nhdplusid);

CREATE UNIQUE INDEX catchment_32702_02u
ON cipsrv_nhdplus_m.catchment_32702(hydroseq);

CREATE INDEX catchment_32702_01i
ON cipsrv_nhdplus_m.catchment_32702(levelpathi);

CREATE INDEX catchment_32702_02i
ON cipsrv_nhdplus_m.catchment_32702(fcode);

CREATE INDEX catchment_32702_03i
ON cipsrv_nhdplus_m.catchment_32702(istribal);

CREATE INDEX catchment_32702_04i
ON cipsrv_nhdplus_m.catchment_32702(isnavigable);

CREATE INDEX catchment_32702_05i
ON cipsrv_nhdplus_m.catchment_32702(iscoastal);

CREATE INDEX catchment_32702_06i
ON cipsrv_nhdplus_m.catchment_32702(isocean);

CREATE INDEX catchment_32702_spx
ON cipsrv_nhdplus_m.catchment_32702 USING GIST(shape);

CREATE INDEX catchment_32702_spx2
ON cipsrv_nhdplus_m.catchment_32702 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_m.catchment_32702;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.catchment_32702;

