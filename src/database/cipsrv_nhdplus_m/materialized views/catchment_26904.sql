DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.catchment_26904 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_m.catchment_26904_seq;
CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_m.catchment_26904_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.catchment_26904(
    objectid
   ,nhdplusid
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
   ,istribal_areasqkm
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
   ,catchmentstatecodes
   ,vpuid
   ,statesplit
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_m.catchment_26904_seq') AS objectid
,a.nhdplusid
---
,b.hydroseq
,b.levelpathi
,b.lengthkm
,b.tonode
,b.fromnode
,b.connector_tonode
,b.connector_fromnode
---
,b.fcode::INTEGER           AS fcode
---
,a.istribal
,a.istribal_areasqkm
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
,a.catchmentstatecodes
,a.vpuid
,a.statesplit
FROM (
   SELECT
    aa.nhdplusid::BIGINT AS nhdplusid
   ,aa.istribal
   ,aa.istribal_areasqkm
   ,CASE WHEN aa.isnavigable = 'Y' THEN TRUE ELSE FALSE END AS isnavigable
   ,CASE WHEN aa.hasvaa      = 'Y' THEN TRUE ELSE FALSE END AS hasvaa
   ,CASE WHEN aa.issink      = 'Y' THEN TRUE ELSE FALSE END AS issink
   ,CASE WHEN aa.isheadwater = 'Y' THEN TRUE ELSE FALSE END AS isheadwater
   ,CASE WHEN aa.iscoastal   = 'Y' THEN TRUE ELSE FALSE END AS iscoastal
   ,CASE WHEN aa.isocean     = 'Y' THEN TRUE ELSE FALSE END AS isocean
   ,CASE WHEN aa.isalaskan   = 'Y' THEN TRUE ELSE FALSE END AS isalaskan
   ,aa.h3hexagonaddr
   ,aa.areasqkm
   ,ST_Transform(aa.shape,26904) AS shape
   ,ARRAY[aa.catchmentstatecode]::VARCHAR[] AS catchmentstatecodes
   ,aa.vpuid
   ,CASE
    WHEN aa.state_count = 1
    THEN
      0::INTEGER 
    ELSE
      1::INTEGER
    END AS statesplit
   FROM
   cipsrv_epageofab_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('HI')
   UNION ALL 
   SELECT
    bb.nhdplusid
   ,bb.istribal
   ,bb.istribal_areasqkm
   ,bb.isnavigable
   ,bb.hasvaa
   ,bb.issink
   ,bb.isheadwater
   ,bb.iscoastal
   ,bb.isocean
   ,bb.isalaskan
   ,bb.h3hexagonaddr
   ,bb.areasqkm
   ,bb.shape
   ,bb.catchmentstatecodes
   ,bb.vpuid
   ,bb.statesplit
   FROM (
      SELECT
       bbb.nhdplusid::BIGINT AS nhdplusid
      ,(array_agg(bbb.istribal ORDER BY CASE WHEN bbb.istribal = 'P' THEN 1 WHEN bbb.istribal = 'F' THEN 2 WHEN bbb.istribal = 'N' THEN 3 ELSE 4 END ASC))[1] AS istribal
      ,SUM(bbb.istribal_areasqkm) AS istribal_areasqkm
      ,bool_or(CASE WHEN bbb.isnavigable = 'Y' THEN TRUE ELSE FALSE END) AS isnavigable
      ,bool_or(CASE WHEN bbb.hasvaa      = 'Y' THEN TRUE ELSE FALSE END) AS hasvaa
      ,bool_or(CASE WHEN bbb.issink      = 'Y' THEN TRUE ELSE FALSE END) AS issink
      ,bool_or(CASE WHEN bbb.isheadwater = 'Y' THEN TRUE ELSE FALSE END) AS isheadwater
      ,bool_or(CASE WHEN bbb.iscoastal   = 'Y' THEN TRUE ELSE FALSE END) AS iscoastal
      ,bool_or(CASE WHEN bbb.isocean     = 'Y' THEN TRUE ELSE FALSE END) AS isocean
      ,bool_or(CASE WHEN bbb.isalaskan   = 'Y' THEN TRUE ELSE FALSE END) AS isalaskan
      ,MAX(bbb.h3hexagonaddr) AS h3hexagonaddr
      ,SUM(bbb.areasqkm) AS areasqkm
      ,ST_UNION(ST_Transform(bbb.shape,26904),0.001) AS shape
      ,ARRAY_AGG(bbb.catchmentstatecode)::VARCHAR[] AS catchmentstatecodes
      ,MAX(bbb.vpuid) AS vpuid
      ,2::INTEGER AS statesplit
      FROM
      cipsrv_epageofab_m.catchment_fabric bbb
      WHERE
          bbb.catchmentstatecode IN ('HI')
      AND bbb.state_count > 1
      GROUP BY
      bbb.nhdplusid::BIGINT
   ) bb
) a
LEFT JOIN
cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid;

ALTER TABLE cipsrv_nhdplus_m.catchment_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_26904 TO public;

CREATE UNIQUE INDEX catchment_26904_01u
ON cipsrv_nhdplus_m.catchment_26904(nhdplusid,statesplit);

CREATE UNIQUE INDEX catchment_26904_02u
ON cipsrv_nhdplus_m.catchment_26904(hydroseq,statesplit);

CREATE UNIQUE INDEX catchment_26904_03u
ON cipsrv_nhdplus_m.catchment_26904(objectid);

CREATE INDEX catchment_26904_01i
ON cipsrv_nhdplus_m.catchment_26904(nhdplusid);

CREATE INDEX catchment_26904_02i
ON cipsrv_nhdplus_m.catchment_26904(hydroseq);

CREATE INDEX catchment_26904_03i
ON cipsrv_nhdplus_m.catchment_26904(levelpathi);

CREATE INDEX catchment_26904_04i
ON cipsrv_nhdplus_m.catchment_26904(fcode);

CREATE INDEX catchment_26904_05i
ON cipsrv_nhdplus_m.catchment_26904(istribal);

CREATE INDEX catchment_26904_06i
ON cipsrv_nhdplus_m.catchment_26904(isnavigable);

CREATE INDEX catchment_26904_07i
ON cipsrv_nhdplus_m.catchment_26904(iscoastal);

CREATE INDEX catchment_26904_08i
ON cipsrv_nhdplus_m.catchment_26904(isocean);

CREATE INDEX catchment_26904_09i
ON cipsrv_nhdplus_m.catchment_26904(statesplit);

CREATE INDEX catchment_26904_10i
ON cipsrv_nhdplus_m.catchment_26904(vpuid);

CREATE INDEX catchment_26904_01f
ON cipsrv_nhdplus_m.catchment_26904(SUBSTR(vpuid,1,2));

CREATE INDEX catchment_26904_spx
ON cipsrv_nhdplus_m.catchment_26904 USING GIST(shape);

CREATE INDEX catchment_26904_spx2
ON cipsrv_nhdplus_m.catchment_26904 USING GIST(shape_centroid);

CREATE INDEX catchment_26904_gin
ON cipsrv_nhdplus_m.catchment_26904 USING GIN(catchmentstatecodes);

ANALYZE cipsrv_nhdplus_m.catchment_26904;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.catchment_26904;

