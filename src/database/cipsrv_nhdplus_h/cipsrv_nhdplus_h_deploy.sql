--******************************--
----- materialized views/nhdplusflowlinevaa_catnodes.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes CASCADE;

CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(
    objectid
   ,nhdplusid
   ,hydroseq
   ,levelpathi
   ,fromnode
   ,tonode
   ,connector_fromnode
   ,connector_tonode
   ,lengthkm
)
AS
WITH cat AS (
    SELECT
     aa.nhdplusid
    ,aa.hydroseq
    ,aa.levelpathi
    ,aa.uphydroseq
    ,aa.dnhydroseq
    ,aa.fromnode
    ,aa.tonode
    ,aa.lengthkm
    FROM
    cipsrv_nhdplus_h.nhdplusflowlinevaa aa
    WHERE 
    EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.catchment_fabric bb WHERE bb.nhdplusid = aa.nhdplusid)
)
,nocat AS (
   SELECT
    cc.hydroseq
   ,cc.levelpathi
   ,cc.fromnode
   ,cc.tonode
   FROM
   cipsrv_nhdplus_h.nhdplusflowlinevaa cc
   WHERE 
   NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.catchment_fabric dd WHERE dd.nhdplusid = cc.nhdplusid)
) 
SELECT
 NEXTVAL('cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes_seq') AS objectid
,a.nhdplusid
,a.hydroseq
,a.levelpathi
,a.fromnode
,a.tonode
,b.fromnode
,c.tonode
,a.lengthkm
FROM
cat a
LEFT JOIN
nocat b
ON
b.hydroseq = a.uphydroseq
LEFT JOIN 
nocat c
ON
c.hydroseq = a.dnhydroseq;

ALTER TABLE cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes TO public;

CREATE UNIQUE INDEX nhdplusflowlinevaa_catnodes_01u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(nhdplusid);

CREATE UNIQUE INDEX nhdplusflowlinevaa_catnodes_02u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(hydroseq);

CREATE UNIQUE INDEX nhdplusflowlinevaa_catnodes_03u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(objectid);

CREATE INDEX nhdplusflowlinevaa_catnodes_01i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(levelpathi);

CREATE INDEX nhdplusflowlinevaa_catnodes_02i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(fromnode);

CREATE INDEX nhdplusflowlinevaa_catnodes_03i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(tonode);

CREATE INDEX nhdplusflowlinevaa_catnodes_04i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(connector_fromnode);

CREATE INDEX nhdplusflowlinevaa_catnodes_05i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(connector_tonode);

ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes;
--******************************--
----- materialized views/nhdplusflowlinevaa_levelpathi.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi(
    objectid
   ,levelpathi
   ,max_hydroseq
   ,min_hydroseq
   ,fromnode
   ,tonode
   ,levelpathilengthkm
)
AS
SELECT
 CAST(a.objectid AS INTEGER) AS objectid
,a.levelpathi
,a.max_hydroseq
,a.min_hydroseq
,(SELECT c.fromnode FROM cipsrv_nhdplus_h.nhdplusflowlinevaa c WHERE c.hydroseq = a.max_hydroseq) AS fromnode
,(SELECT d.tonode   FROM cipsrv_nhdplus_h.nhdplusflowlinevaa d WHERE d.hydroseq = a.min_hydroseq) AS tonode
,a.levelpathilengthkm 
FROM (
   SELECT
    MAX(aa.objectid) AS objectid
   ,aa.levelpathi
   ,MAX(aa.hydroseq) AS max_hydroseq
   ,MIN(aa.hydroseq) AS min_hydroseq
   ,SUM(aa.lengthkm) AS levelpathilengthkm
   FROM
   cipsrv_nhdplus_h.nhdplusflowlinevaa aa
   GROUP BY
   aa.levelpathi
) a;

ALTER TABLE cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi TO public;

CREATE UNIQUE INDEX nhdplusflowlinevaa_levelpathi_01u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi(levelpathi);

CREATE UNIQUE INDEX nhdplusflowlinevaa_levelpathi_02u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi(objectid);

CREATE INDEX nhdplusflowlinevaa_levelpathi_01i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi(max_hydroseq);

CREATE INDEX nhdplusflowlinevaa_levelpathi_02i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi(min_hydroseq);

CREATE INDEX nhdplusflowlinevaa_levelpathi_03i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi(fromnode);

CREATE INDEX nhdplusflowlinevaa_levelpathi_04i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi(tonode);

ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi;
--******************************--
----- materialized views/catchment_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_3338 CASCADE;

CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.catchment_3338_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_3338(
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
   ,statesplit
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_h.catchment_3338_seq') AS objectid
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
,c.fcode::INTEGER           AS fcode
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
   ,ST_Transform(aa.shape,3338) AS shape
   ,ARRAY[aa.catchmentstatecode]::VARCHAR[] AS catchmentstatecodes
   ,CASE
    WHEN aa.state_count = 1
    THEN
      0::INTEGER 
    ELSE
      1::INTEGER
    END AS statesplit
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('AK')
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
      ,ST_UNION(ST_Transform(bbb.shape,3338)) AS shape
      ,ARRAY_AGG(bbb.catchmentstatecode)::VARCHAR[] AS catchmentstatecodes
      ,2::INTEGER AS statesplit
      FROM
      cipsrv_nhdplus_h.catchment_fabric bbb
      WHERE
          bbb.catchmentstatecode IN ('AK')
      AND bbb.state_count > 1
      GROUP BY
      bbb.nhdplusid::BIGINT
   ) bb
) a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_h.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_h.catchment_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_3338 TO public;

CREATE UNIQUE INDEX catchment_3338_01u
ON cipsrv_nhdplus_h.catchment_3338(catchmentstatecodes,nhdplusid);

CREATE UNIQUE INDEX catchment_3338_02u
ON cipsrv_nhdplus_h.catchment_3338(catchmentstatecodes,hydroseq);

CREATE UNIQUE INDEX catchment_3338_03u
ON cipsrv_nhdplus_h.catchment_3338(objectid);

CREATE INDEX catchment_3338_01i
ON cipsrv_nhdplus_h.catchment_3338(nhdplusid);

CREATE INDEX catchment_3338_02i
ON cipsrv_nhdplus_h.catchment_3338(hydroseq);

CREATE INDEX catchment_3338_03i
ON cipsrv_nhdplus_h.catchment_3338(levelpathi);

CREATE INDEX catchment_3338_04i
ON cipsrv_nhdplus_h.catchment_3338(fcode);

CREATE INDEX catchment_3338_05i
ON cipsrv_nhdplus_h.catchment_3338(istribal);

CREATE INDEX catchment_3338_06i
ON cipsrv_nhdplus_h.catchment_3338(isnavigable);

CREATE INDEX catchment_3338_07i
ON cipsrv_nhdplus_h.catchment_3338(iscoastal);

CREATE INDEX catchment_3338_08i
ON cipsrv_nhdplus_h.catchment_3338(isocean);

CREATE INDEX catchment_3338_09i
ON cipsrv_nhdplus_h.catchment_3338(statesplit);

CREATE INDEX catchment_3338_spx
ON cipsrv_nhdplus_h.catchment_3338 USING GIST(shape);

CREATE INDEX catchment_3338_spx2
ON cipsrv_nhdplus_h.catchment_3338 USING GIST(shape_centroid);

CREATE INDEX catchment_3338_gin
ON cipsrv_nhdplus_h.catchment_3338 USING GIN(catchmentstatecodes);

ANALYZE cipsrv_nhdplus_h.catchment_3338;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_3338;

--******************************--
----- materialized views/catchment_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_5070 CASCADE;

CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.catchment_5070_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_5070(
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
   ,statesplit
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_h.catchment_5070_seq') AS objectid
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
,c.fcode::INTEGER           AS fcode
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
   ,ST_Transform(aa.shape,5070) AS shape
   ,ARRAY[aa.catchmentstatecode]::VARCHAR[] AS catchmentstatecodes
   ,CASE
    WHEN aa.state_count = 1
    THEN
      0::INTEGER 
    ELSE
      1::INTEGER
    END AS statesplit
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode NOT IN ('AK','HI','PR','VI','GU','MP','AS')
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
      ,ST_UNION(ST_Transform(bbb.shape,5070)) AS shape
      ,ARRAY_AGG(bbb.catchmentstatecode)::VARCHAR[] AS catchmentstatecodes
      ,2::INTEGER AS statesplit
      FROM
      cipsrv_nhdplus_h.catchment_fabric bbb
      WHERE
          bbb.catchmentstatecode NOT IN ('AK','HI','PR','VI','GU','MP','AS')
      AND bbb.state_count > 1
      GROUP BY
      bbb.nhdplusid
   ) bb
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
ON cipsrv_nhdplus_h.catchment_5070(catchmentstatecodes,nhdplusid);

CREATE UNIQUE INDEX catchment_5070_02u
ON cipsrv_nhdplus_h.catchment_5070(catchmentstatecodes,hydroseq);

CREATE UNIQUE INDEX catchment_5070_03u
ON cipsrv_nhdplus_h.catchment_5070(objectid);

CREATE INDEX catchment_5070_01i
ON cipsrv_nhdplus_h.catchment_5070(nhdplusid);

CREATE INDEX catchment_5070_02i
ON cipsrv_nhdplus_h.catchment_5070(hydroseq);

CREATE INDEX catchment_5070_03i
ON cipsrv_nhdplus_h.catchment_5070(levelpathi);

CREATE INDEX catchment_5070_04i
ON cipsrv_nhdplus_h.catchment_5070(fcode);

CREATE INDEX catchment_5070_05i
ON cipsrv_nhdplus_h.catchment_5070(istribal);

CREATE INDEX catchment_5070_06i
ON cipsrv_nhdplus_h.catchment_5070(isnavigable);

CREATE INDEX catchment_5070_07i
ON cipsrv_nhdplus_h.catchment_5070(iscoastal);

CREATE INDEX catchment_5070_08i
ON cipsrv_nhdplus_h.catchment_5070(isocean);

CREATE INDEX catchment_5070_09i
ON cipsrv_nhdplus_h.catchment_5070(statesplit);

CREATE INDEX catchment_5070_spx
ON cipsrv_nhdplus_h.catchment_5070 USING GIST(shape);

CREATE INDEX catchment_5070_spx2
ON cipsrv_nhdplus_h.catchment_5070 USING GIST(shape_centroid);

CREATE INDEX catchment_5070_gin
ON cipsrv_nhdplus_h.catchment_5070 USING GIN(catchmentstatecodes);

ANALYZE cipsrv_nhdplus_h.catchment_5070;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_5070;

--******************************--
----- materialized views/catchment_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_26904 CASCADE;

CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.catchment_26904_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_26904(
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
   ,statesplit
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_h.catchment_26904_seq') AS objectid
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
,c.fcode::INTEGER           AS fcode
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
   ,CASE
    WHEN aa.state_count = 1
    THEN
      0::INTEGER 
    ELSE
      1::INTEGER
    END AS statesplit
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
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
      ,ST_UNION(ST_Transform(bbb.shape,26904)) AS shape
      ,ARRAY_AGG(bbb.catchmentstatecode)::VARCHAR[] AS catchmentstatecodes
      ,2::INTEGER AS statesplit
      FROM
      cipsrv_nhdplus_h.catchment_fabric bbb
      WHERE
          bbb.catchmentstatecode IN ('HI')
      AND bbb.state_count > 1
      GROUP BY
      bbb.nhdplusid::BIGINT
   ) bb
) a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_h.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_h.catchment_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_26904 TO public;

CREATE UNIQUE INDEX catchment_26904_01u
ON cipsrv_nhdplus_h.catchment_26904(nhdplusid,statesplit);

CREATE UNIQUE INDEX catchment_26904_02u
ON cipsrv_nhdplus_h.catchment_26904(hydroseq,statesplit);

CREATE UNIQUE INDEX catchment_26904_03u
ON cipsrv_nhdplus_h.catchment_26904(objectid);

CREATE INDEX catchment_26904_01i
ON cipsrv_nhdplus_h.catchment_26904(nhdplusid);

CREATE INDEX catchment_26904_02i
ON cipsrv_nhdplus_h.catchment_26904(hydroseq);

CREATE INDEX catchment_26904_03i
ON cipsrv_nhdplus_h.catchment_26904(levelpathi);

CREATE INDEX catchment_26904_04i
ON cipsrv_nhdplus_h.catchment_26904(fcode);

CREATE INDEX catchment_26904_05i
ON cipsrv_nhdplus_h.catchment_26904(istribal);

CREATE INDEX catchment_26904_06i
ON cipsrv_nhdplus_h.catchment_26904(isnavigable);

CREATE INDEX catchment_26904_07i
ON cipsrv_nhdplus_h.catchment_26904(iscoastal);

CREATE INDEX catchment_26904_08i
ON cipsrv_nhdplus_h.catchment_26904(isocean);

CREATE INDEX catchment_26904_09i
ON cipsrv_nhdplus_h.catchment_26904(statesplit);

CREATE INDEX catchment_26904_spx
ON cipsrv_nhdplus_h.catchment_26904 USING GIST(shape);

CREATE INDEX catchment_26904_spx2
ON cipsrv_nhdplus_h.catchment_26904 USING GIST(shape_centroid);

CREATE INDEX catchment_26904_gin
ON cipsrv_nhdplus_h.catchment_26904 USING GIN(catchmentstatecodes);

ANALYZE cipsrv_nhdplus_h.catchment_26904;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_26904;

--******************************--
----- materialized views/catchment_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_32161 CASCADE;

CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.catchment_32161_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_32161(
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
   ,statesplit
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_h.catchment_32161_seq') AS objectid
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
,c.fcode::INTEGER           AS fcode
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
   ,ST_Transform(aa.shape,32161) AS shape
   ,ARRAY[aa.catchmentstatecode]::VARCHAR[] AS catchmentstatecodes
   ,CASE
    WHEN aa.state_count = 1
    THEN
      0::INTEGER 
    ELSE
      1::INTEGER
    END AS statesplit
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('PR','VI')
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
      ,ST_UNION(ST_Transform(bbb.shape,32161)) AS shape
      ,ARRAY_AGG(bbb.catchmentstatecode)::VARCHAR[] AS catchmentstatecodes
      ,2::INTEGER AS statesplit
      FROM
      cipsrv_nhdplus_h.catchment_fabric bbb
      WHERE
          bbb.catchmentstatecode IN ('PR','VI')
      AND bbb.state_count > 1
      GROUP BY
      bbb.nhdplusid::BIGINT
   ) bb
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
ON cipsrv_nhdplus_h.catchment_32161(catchmentstatecodes,nhdplusid);

CREATE UNIQUE INDEX catchment_32161_02u
ON cipsrv_nhdplus_h.catchment_32161(catchmentstatecodes,hydroseq);

CREATE UNIQUE INDEX catchment_32161_03u
ON cipsrv_nhdplus_h.catchment_32161(objectid);

CREATE INDEX catchment_32161_01i
ON cipsrv_nhdplus_h.catchment_32161(nhdplusid);

CREATE INDEX catchment_32161_02i
ON cipsrv_nhdplus_h.catchment_32161(hydroseq);

CREATE INDEX catchment_32161_03i
ON cipsrv_nhdplus_h.catchment_32161(levelpathi);

CREATE INDEX catchment_32161_04i
ON cipsrv_nhdplus_h.catchment_32161(fcode);

CREATE INDEX catchment_32161_05i
ON cipsrv_nhdplus_h.catchment_32161(istribal);

CREATE INDEX catchment_32161_06i
ON cipsrv_nhdplus_h.catchment_32161(isnavigable);

CREATE INDEX catchment_32161_07i
ON cipsrv_nhdplus_h.catchment_32161(iscoastal);

CREATE INDEX catchment_32161_08i
ON cipsrv_nhdplus_h.catchment_32161(isocean);

CREATE INDEX catchment_32161_09i
ON cipsrv_nhdplus_h.catchment_32161(statesplit);

CREATE INDEX catchment_32161_spx
ON cipsrv_nhdplus_h.catchment_32161 USING GIST(shape);

CREATE INDEX catchment_32161_spx2
ON cipsrv_nhdplus_h.catchment_32161 USING GIST(shape_centroid);

CREATE INDEX catchment_32161_gin
ON cipsrv_nhdplus_h.catchment_32161 USING GIN(catchmentstatecodes);

ANALYZE cipsrv_nhdplus_h.catchment_32161;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_32161;

--******************************--
----- materialized views/catchment_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_32655 CASCADE;

CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.catchment_32655_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_32655(
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
   ,statesplit
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_h.catchment_32655_seq') AS objectid
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
,c.fcode::INTEGER           AS fcode
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
   ,ST_Transform(aa.shape,32655) AS shape
   ,ARRAY[aa.catchmentstatecode]::VARCHAR[] AS catchmentstatecodes
   ,CASE
    WHEN aa.state_count = 1
    THEN
      0::INTEGER 
    ELSE
      1::INTEGER
    END AS statesplit
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('GU','MP')
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
      ,ST_UNION(ST_Transform(bbb.shape,32655)) AS shape
      ,ARRAY_AGG(bbb.catchmentstatecode)::VARCHAR[] AS catchmentstatecodes
      ,2::INTEGER AS statesplit
      FROM
      cipsrv_nhdplus_h.catchment_fabric bbb
      WHERE
          bbb.catchmentstatecode IN ('GU','MP')
      AND bbb.state_count > 1
      GROUP BY
      bbb.nhdplusid::BIGINT
   ) bb
) a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_h.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_h.catchment_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32655 TO public;

CREATE UNIQUE INDEX catchment_32655_01u
ON cipsrv_nhdplus_h.catchment_32655(catchmentstatecodes,nhdplusid);

CREATE UNIQUE INDEX catchment_32655_02u
ON cipsrv_nhdplus_h.catchment_32655(catchmentstatecodes,hydroseq);

CREATE UNIQUE INDEX catchment_32655_03u
ON cipsrv_nhdplus_h.catchment_32655(objectid);

CREATE INDEX catchment_32655_01i
ON cipsrv_nhdplus_h.catchment_32655(nhdplusid);

CREATE INDEX catchment_32655_02i
ON cipsrv_nhdplus_h.catchment_32655(hydroseq);

CREATE INDEX catchment_32655_03i
ON cipsrv_nhdplus_h.catchment_32655(levelpathi);

CREATE INDEX catchment_32655_04i
ON cipsrv_nhdplus_h.catchment_32655(fcode);

CREATE INDEX catchment_32655_05i
ON cipsrv_nhdplus_h.catchment_32655(istribal);

CREATE INDEX catchment_32655_06i
ON cipsrv_nhdplus_h.catchment_32655(isnavigable);

CREATE INDEX catchment_32655_07i
ON cipsrv_nhdplus_h.catchment_32655(iscoastal);

CREATE INDEX catchment_32655_08i
ON cipsrv_nhdplus_h.catchment_32655(isocean);

CREATE INDEX catchment_32655_09i
ON cipsrv_nhdplus_h.catchment_32655(statesplit);

CREATE INDEX catchment_32655_spx
ON cipsrv_nhdplus_h.catchment_32655 USING GIST(shape);

CREATE INDEX catchment_32655_spx2
ON cipsrv_nhdplus_h.catchment_32655 USING GIST(shape_centroid);

CREATE INDEX catchment_32655_gin
ON cipsrv_nhdplus_h.catchment_32655 USING GIN(catchmentstatecodes);

ANALYZE cipsrv_nhdplus_h.catchment_32655;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_32655;

--******************************--
----- materialized views/catchment_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_32702 CASCADE;

CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.catchment_32702_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_32702(
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
   ,statesplit
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_h.catchment_32702_seq') AS objectid
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
,c.fcode::INTEGER           AS fcode
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
   ,ST_Transform(aa.shape,32702) AS shape
   ,ARRAY[aa.catchmentstatecode]::VARCHAR[] AS catchmentstatecodes
   ,CASE
    WHEN aa.state_count = 1
    THEN
      0::INTEGER 
    ELSE
      1::INTEGER
    END AS statesplit
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('AS')
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
      ,ST_UNION(ST_Transform(bbb.shape,32702)) AS shape
      ,ARRAY_AGG(bbb.catchmentstatecode)::VARCHAR[] AS catchmentstatecodes
      ,2::INTEGER AS statesplit
      FROM
      cipsrv_nhdplus_h.catchment_fabric bbb
      WHERE
          bbb.catchmentstatecode IN ('AS')
      AND bbb.state_count > 1
      GROUP BY
      bbb.nhdplusid::BIGINT
   ) bb
) a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_h.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_h.catchment_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32702 TO public;

CREATE UNIQUE INDEX catchment_32702_01u
ON cipsrv_nhdplus_h.catchment_32702(catchmentstatecodes,nhdplusid);

CREATE UNIQUE INDEX catchment_32702_02u
ON cipsrv_nhdplus_h.catchment_32702(catchmentstatecodes,hydroseq);

CREATE UNIQUE INDEX catchment_32702_03u
ON cipsrv_nhdplus_h.catchment_32702(objectid);

CREATE INDEX catchment_32702_01i
ON cipsrv_nhdplus_h.catchment_32702(nhdplusid);

CREATE INDEX catchment_32702_02i
ON cipsrv_nhdplus_h.catchment_32702(hydroseq);

CREATE INDEX catchment_32702_03i
ON cipsrv_nhdplus_h.catchment_32702(levelpathi);

CREATE INDEX catchment_32702_04i
ON cipsrv_nhdplus_h.catchment_32702(fcode);

CREATE INDEX catchment_32702_05i
ON cipsrv_nhdplus_h.catchment_32702(istribal);

CREATE INDEX catchment_32702_06i
ON cipsrv_nhdplus_h.catchment_32702(isnavigable);

CREATE INDEX catchment_32702_07i
ON cipsrv_nhdplus_h.catchment_32702(iscoastal);

CREATE INDEX catchment_32702_08i
ON cipsrv_nhdplus_h.catchment_32702(isocean);

CREATE INDEX catchment_32702_09i
ON cipsrv_nhdplus_h.catchment_32702(statesplit);

CREATE INDEX catchment_32702_spx
ON cipsrv_nhdplus_h.catchment_32702 USING GIST(shape);

CREATE INDEX catchment_32702_spx2
ON cipsrv_nhdplus_h.catchment_32702 USING GIST(shape_centroid);

CREATE INDEX catchment_32702_gin
ON cipsrv_nhdplus_h.catchment_32702 USING GIN(catchmentstatecodes);

ANALYZE cipsrv_nhdplus_h.catchment_32702;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_32702;

--******************************--
----- materialized views/nhdflowline_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_3338(
    objectid
   ,permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,hasvaa
   ,isnavigable
   ,hydroseq
   ,shape
)
AS
SELECT
 CAST(a.objectid AS INTEGER) AS objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,CASE 
 WHEN b.nhdplusid IS NOT NULL
 THEN
   TRUE
 ELSE
   FALSE
 END AS hasvaa
,CASE
 WHEN b.nhdplusid IS NOT NULL
 AND a.fcode NOT IN (56600)
 THEN
   TRUE
 ELSE
   FALSE
 END AS isnavigable
,b.hydroseq
,ST_Transform(a.shape,3338) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa b
ON
a.nhdplusid = b.nhdplusid
WHERE
SUBSTR(a.vpuid,1,2) IN ('19');

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_3338 TO public;

CREATE UNIQUE INDEX nhdflowline_3338_01u
ON cipsrv_nhdplus_h.nhdflowline_3338(nhdplusid);

CREATE UNIQUE INDEX nhdflowline_3338_02u
ON cipsrv_nhdplus_h.nhdflowline_3338(objectid);

CREATE INDEX nhdflowline_3338_02i
ON cipsrv_nhdplus_h.nhdflowline_3338(fcode);

CREATE INDEX nhdflowline_3338_03i
ON cipsrv_nhdplus_h.nhdflowline_3338(hasvaa);

CREATE INDEX nhdflowline_3338_04i
ON cipsrv_nhdplus_h.nhdflowline_3338(isnavigable);

CREATE INDEX nhdflowline_3338_05i
ON cipsrv_nhdplus_h.nhdflowline_3338(hydroseq);

CREATE INDEX nhdflowline_3338_spx
ON cipsrv_nhdplus_h.nhdflowline_3338 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_3338;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_3338;

--******************************--
----- materialized views/nhdflowline_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_5070(
    objectid
   ,permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,hasvaa
   ,isnavigable
   ,hydroseq
   ,shape
)
AS
SELECT
 CAST(a.objectid AS INTEGER) AS objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,CASE 
 WHEN b.nhdplusid IS NOT NULL
 THEN
   TRUE
 ELSE
   FALSE
 END AS hasvaa
,CASE
 WHEN b.nhdplusid IS NOT NULL
 AND a.fcode NOT IN (56600)
 THEN
   TRUE
 ELSE
   FALSE
 END AS isnavigable
,b.hydroseq
,ST_Transform(a.shape,5070) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa b
ON
a.nhdplusid = b.nhdplusid
WHERE
SUBSTR(a.vpuid,1,2) NOT IN ('19','20','21','22');

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_5070 TO public;

CREATE UNIQUE INDEX nhdflowline_5070_01u
ON cipsrv_nhdplus_h.nhdflowline_5070(nhdplusid);

CREATE UNIQUE INDEX nhdflowline_5070_02u
ON cipsrv_nhdplus_h.nhdflowline_5070(objectid);

CREATE INDEX nhdflowline_5070_02i
ON cipsrv_nhdplus_h.nhdflowline_5070(fcode);

CREATE INDEX nhdflowline_5070_03i
ON cipsrv_nhdplus_h.nhdflowline_5070(hasvaa);

CREATE INDEX nhdflowline_5070_04i
ON cipsrv_nhdplus_h.nhdflowline_5070(isnavigable);

CREATE INDEX nhdflowline_5070_05i
ON cipsrv_nhdplus_h.nhdflowline_5070(hydroseq);

CREATE INDEX nhdflowline_5070_spx
ON cipsrv_nhdplus_h.nhdflowline_5070 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_5070;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_5070;

--******************************--
----- materialized views/nhdflowline_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_26904(
    objectid
   ,permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,hasvaa
   ,isnavigable
   ,hydroseq
   ,shape
)
AS
SELECT
 CAST(a.objectid AS INTEGER) AS objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,CASE 
 WHEN b.nhdplusid IS NOT NULL
 THEN
   TRUE
 ELSE
   FALSE
 END AS hasvaa
,CASE
 WHEN b.nhdplusid IS NOT NULL
 AND a.fcode NOT IN (56600)
 THEN
   TRUE
 ELSE
   FALSE
 END AS isnavigable
,b.hydroseq
,ST_Transform(a.shape,26904) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa b
ON
a.nhdplusid = b.nhdplusid
WHERE
SUBSTR(a.vpuid,1,2) IN ('20');

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_26904 TO public;

CREATE UNIQUE INDEX nhdflowline_26904_01u
ON cipsrv_nhdplus_h.nhdflowline_26904(nhdplusid);

CREATE UNIQUE INDEX nhdflowline_26904_02u
ON cipsrv_nhdplus_h.nhdflowline_26904(objectid);

CREATE INDEX nhdflowline_26904_02i
ON cipsrv_nhdplus_h.nhdflowline_26904(fcode);

CREATE INDEX nhdflowline_26904_03i
ON cipsrv_nhdplus_h.nhdflowline_26904(hasvaa);

CREATE INDEX nhdflowline_26904_04i
ON cipsrv_nhdplus_h.nhdflowline_26904(isnavigable);

CREATE INDEX nhdflowline_26904_05i
ON cipsrv_nhdplus_h.nhdflowline_26904(hydroseq);

CREATE INDEX nhdflowline_26904_spx
ON cipsrv_nhdplus_h.nhdflowline_26904 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_26904;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_26904;

--******************************--
----- materialized views/nhdflowline_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_32161(
    objectid
   ,permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,hasvaa
   ,isnavigable
   ,hydroseq
   ,shape
)
AS
SELECT
 CAST(a.objectid AS INTEGER) AS objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,CASE 
 WHEN b.nhdplusid IS NOT NULL
 THEN
   TRUE
 ELSE
   FALSE
 END AS hasvaa
,CASE
 WHEN b.nhdplusid IS NOT NULL
 AND a.fcode NOT IN (56600)
 THEN
   TRUE
 ELSE
   FALSE
 END AS isnavigable
,b.hydroseq
,ST_Transform(a.shape,32161) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa b
ON
a.nhdplusid = b.nhdplusid
WHERE
SUBSTR(a.vpuid,1,2) IN ('21');

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_32161 TO public;

CREATE UNIQUE INDEX nhdflowline_32161_01u
ON cipsrv_nhdplus_h.nhdflowline_32161(nhdplusid);

CREATE UNIQUE INDEX nhdflowline_32161_02u
ON cipsrv_nhdplus_h.nhdflowline_32161(objectid);

CREATE INDEX nhdflowline_32161_02i
ON cipsrv_nhdplus_h.nhdflowline_32161(fcode);

CREATE INDEX nhdflowline_32161_03i
ON cipsrv_nhdplus_h.nhdflowline_32161(hasvaa);

CREATE INDEX nhdflowline_32161_04i
ON cipsrv_nhdplus_h.nhdflowline_32161(isnavigable);

CREATE INDEX nhdflowline_32161_05i
ON cipsrv_nhdplus_h.nhdflowline_32161(hydroseq);

CREATE INDEX nhdflowline_32161_spx
ON cipsrv_nhdplus_h.nhdflowline_32161 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_32161;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_32161;

--******************************--
----- materialized views/nhdflowline_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_32655(
    objectid
   ,permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,hasvaa
   ,isnavigable
   ,hydroseq
   ,shape
)
AS
SELECT
 CAST(a.objectid AS INTEGER) AS objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,CASE 
 WHEN b.nhdplusid IS NOT NULL
 THEN
   TRUE
 ELSE
   FALSE
 END AS hasvaa
,CASE
 WHEN b.nhdplusid IS NOT NULL
 AND a.fcode NOT IN (56600)
 THEN
   TRUE
 ELSE
   FALSE
 END AS isnavigable
,b.hydroseq
,ST_Transform(a.shape,32655) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa b
ON
a.nhdplusid = b.nhdplusid
WHERE
SUBSTR(a.vpuid,1,4) IN ('2201','2202');

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_32655 TO public;

CREATE UNIQUE INDEX nhdflowline_32655_01u
ON cipsrv_nhdplus_h.nhdflowline_32655(nhdplusid);

CREATE UNIQUE INDEX nhdflowline_32655_02u
ON cipsrv_nhdplus_h.nhdflowline_32655(objectid);

CREATE INDEX nhdflowline_32655_02i
ON cipsrv_nhdplus_h.nhdflowline_32655(fcode);

CREATE INDEX nhdflowline_32655_03i
ON cipsrv_nhdplus_h.nhdflowline_32655(hasvaa);

CREATE INDEX nhdflowline_32655_04i
ON cipsrv_nhdplus_h.nhdflowline_32655(isnavigable);

CREATE INDEX nhdflowline_32655_05i
ON cipsrv_nhdplus_h.nhdflowline_32655(hydroseq);

CREATE INDEX nhdflowline_32655_spx
ON cipsrv_nhdplus_h.nhdflowline_32655 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_32655;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_32655;

--******************************--
----- materialized views/nhdflowline_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_32702(
    objectid
   ,permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,hasvaa
   ,isnavigable
   ,hydroseq
   ,shape
)
AS
SELECT
 CAST(a.objectid AS INTEGER) AS objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,CASE 
 WHEN b.nhdplusid IS NOT NULL
 THEN
   TRUE
 ELSE
   FALSE
 END AS hasvaa
,CASE
 WHEN b.nhdplusid IS NOT NULL
 AND a.fcode NOT IN (56600)
 THEN
   TRUE
 ELSE
   FALSE
 END AS isnavigable
,b.hydroseq
,ST_Transform(a.shape,32702) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa b
ON
a.nhdplusid = b.nhdplusid
WHERE
SUBSTR(a.vpuid,1,4) = '2203';

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_32702 TO public;

CREATE UNIQUE INDEX nhdflowline_32702_01u
ON cipsrv_nhdplus_h.nhdflowline_32702(nhdplusid);

CREATE UNIQUE INDEX nhdflowline_32702_02u
ON cipsrv_nhdplus_h.nhdflowline_32702(objectid);

CREATE INDEX nhdflowline_32702_02i
ON cipsrv_nhdplus_h.nhdflowline_32702(fcode);

CREATE INDEX nhdflowline_32702_03i
ON cipsrv_nhdplus_h.nhdflowline_32702(hasvaa);

CREATE INDEX nhdflowline_32702_04i
ON cipsrv_nhdplus_h.nhdflowline_32702(isnavigable);

CREATE INDEX nhdflowline_32702_05i
ON cipsrv_nhdplus_h.nhdflowline_32702(hydroseq);

CREATE INDEX nhdflowline_32702_spx
ON cipsrv_nhdplus_h.nhdflowline_32702 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_32702;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_32702;

--******************************--
----- materialized views/nhdplusflowlinevaa_nav.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_nav;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(
    objectid
   ,nhdplusid
   ,hydroseq
   ,fmeasure
   ,tmeasure
   ,levelpathi
   ,terminalpa
   ,uphydroseq
   ,dnhydroseq
   ,dnminorhyd
   ,divergence
   ,fromnode
   ,tonode
   /* ++++++++++ */
   ,lengthkm
   ,totma
   ,pathlength
   ,pathtimema
   /* ++++++++++ */
   ,force_main_line
   ,ary_upstream_hydroseq
   ,ary_downstream_hydroseq
   /* ++++++++++ */
   ,headwater
   ,coastal_connection
   ,network_end
   /* ++++++++++ */
   ,vpuid
)
AS
SELECT
 CAST(a.objectid AS INTEGER) AS objectid
,CAST(a.nhdplusid  AS BIGINT) AS nhdplusid
,CAST(a.hydroseq   AS BIGINT) AS hydroseq 
,b.fmeasure
,b.tmeasure
,CAST(a.levelpathi AS BIGINT) AS levelpathi
,CAST(a.terminalpa AS BIGINT) AS terminalpa
,CASE
 WHEN a.uphydroseq = 0
 THEN
   CAST(NULL AS BIGINT)
 ELSE
   CAST(a.uphydroseq AS BIGINT)
 END AS uphydroseq
,CASE
 WHEN a.dnhydroseq = 0
 THEN
   CAST(NULL AS BIGINT)
 ELSE
   CAST(a.dnhydroseq AS BIGINT)
 END AS dnhydroseq
,CASE
 WHEN a.dnminorhyd = 0
 THEN
   CAST(NULL AS BIGINT)
 ELSE
   CAST(a.dnminorhyd AS BIGINT)
 END AS dnminorhyd
,a.divergence
,CAST(a.fromnode AS BIGINT)   AS fromnode
,CAST(a.tonode AS BIGINT)     AS tonode
/* ++++++++++ */
,CAST(b.lengthkm AS NUMERIC) AS lengthkm
,CASE
 WHEN a.totma IN (-9999,-9998)
 THEN
   CAST(NULL AS NUMERIC)
 ELSE
   CAST(a.totma AS NUMERIC)
 END AS totma
,CAST(a.pathlength AS NUMERIC) AS pathlength
,CASE
 WHEN a.pathtimema IN (-9999,-9998)
 THEN
   CAST(NULL AS NUMERIC)
 ELSE
   CAST(a.pathtimema AS NUMERIC)
 END AS pathtimema
/* ++++++++++ */
,CASE
 WHEN a.hydroseq IN (
   0
 )
 THEN
   TRUE
 ELSE
   FALSE
 END AS force_main_line
,ARRAY(SELECT CAST(bb.fromhydroseq AS BIGINT) FROM cipsrv_nhdplus_h.nhdplusflow bb WHERE bb.tohydroseq = a.hydroseq) AS ary_upstream_hydroseq
,CASE
 WHEN a.dndraincou = 1
 THEN
   ARRAY[CAST(a.dnhydroseq AS BIGINT)]
 WHEN a.dndraincou = 2
 THEN
   ARRAY[CAST(a.dnhydroseq AS BIGINT),CAST(a.dnminorhyd AS BIGINT)]
 WHEN a.dndraincou > 2
 THEN
   ARRAY(SELECT CAST(cc.tohydroseq AS BIGINT) FROM cipsrv_nhdplus_h.nhdplusflow cc WHERE cc.fromhydroseq = a.hydroseq)
 ELSE
   NULL
 END AS ary_downstream_hydroseq
,CASE
 WHEN a.startflag = 1
 THEN
   TRUE
 ELSE
   FALSE
 END AS headwater
,CASE
 WHEN EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.nhdplusflow dd WHERE dd.fromhydroseq = a.hydroseq AND dd.direction = 714)
 THEN
   TRUE
 ELSE
   FALSE
 END AS coastal_connection
,CASE
 WHEN EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.nhdplusflow ee WHERE ee.fromhydroseq = a.hydroseq AND ee.direction = 713)
 THEN
   TRUE
 ELSE
   FALSE
 END AS network_end
/* ++++++++++ */
,a.vpuid
FROM
cipsrv_nhdplus_h.nhdplusflowlinevaa a
JOIN
cipsrv_nhdplus_h.nhdflowline b
ON
a.nhdplusid = b.nhdplusid
WHERE
    a.pathlength NOT IN (-9999,-9998)
AND b.fcode <> 56600;

ALTER TABLE cipsrv_nhdplus_h.nhdplusflowlinevaa_nav OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav TO public;

CREATE UNIQUE INDEX nhdplusflowlinevaa_nav_01u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(nhdplusid);

CREATE UNIQUE INDEX nhdplusflowlinevaa_nav_02u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(hydroseq);

CREATE UNIQUE INDEX nhdplusflowlinevaa_nav_03u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(objectid);

CREATE INDEX nhdplusflowlinevaa_nav_01i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(levelpathi);

CREATE INDEX nhdplusflowlinevaa_nav_02i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(divergence);

CREATE INDEX nhdplusflowlinevaa_nav_03i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(uphydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_04i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(dnhydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_05i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(dnminorhyd);

CREATE INDEX nhdplusflowlinevaa_nav_06i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(pathlength);

CREATE INDEX nhdplusflowlinevaa_nav_07i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(pathtimema);

CREATE INDEX nhdplusflowlinevaa_nav_08i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(terminalpa);

CREATE INDEX nhdplusflowlinevaa_nav_09i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(force_main_line);

CREATE INDEX nhdplusflowlinevaa_nav_gn1
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav USING GIN(ary_upstream_hydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_gn2
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav USING GIN(ary_downstream_hydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_10i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(headwater);

CREATE INDEX nhdplusflowlinevaa_nav_11i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(coastal_connection);

CREATE INDEX nhdplusflowlinevaa_nav_12i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(network_end);

CREATE INDEX nhdplusflowlinevaa_nav_13i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(vpuid);

ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_nav;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_nav;

--******************************--
----- views/catchment_3338_full.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_3338_full
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_3338 a
WHERE
a.statesplit IN (0,2);

ALTER TABLE cipsrv_nhdplus_h.catchment_3338_full OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_3338_full TO public;
--******************************--
----- views/catchment_3338_state.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_3338_state
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_3338 a
WHERE
a.statesplit IN (0,1);

ALTER TABLE cipsrv_nhdplus_h.catchment_3338_state OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_3338_state TO public;
--******************************--
----- views/catchment_5070_full.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_5070_full
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_5070 a
WHERE
a.statesplit IN (0,2);

ALTER TABLE cipsrv_nhdplus_h.catchment_5070_full OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_5070_full TO public;
--******************************--
----- views/catchment_5070_state.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_5070_state
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_5070 a
WHERE
a.statesplit IN (0,1);

ALTER TABLE cipsrv_nhdplus_h.catchment_5070_state OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_5070_state TO public;
--******************************--
----- views/catchment_26904_full.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_26904_full
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_26904 a
WHERE
a.statesplit IN (0,2);

ALTER TABLE cipsrv_nhdplus_h.catchment_26904_full OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_26904_full TO public;
--******************************--
----- views/catchment_26904_state.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_26904_state
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_26904 a
WHERE
a.statesplit IN (0,1);

ALTER TABLE cipsrv_nhdplus_h.catchment_26904_state OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_26904_state TO public;
--******************************--
----- views/catchment_32161_full.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_32161_full
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_32161 a
WHERE
a.statesplit IN (0,2);

ALTER TABLE cipsrv_nhdplus_h.catchment_32161_full OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32161_full TO public;
--******************************--
----- views/catchment_32161_state.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_32161_state
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_32161 a
WHERE
a.statesplit IN (0,1);

ALTER TABLE cipsrv_nhdplus_h.catchment_32161_state OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32161_state TO public;
--******************************--
----- views/catchment_32655_full.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_32655_full
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_32655 a
WHERE
a.statesplit IN (0,2);

ALTER TABLE cipsrv_nhdplus_h.catchment_32655_full OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32655_full TO public;
--******************************--
----- views/catchment_32655_state.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_32655_state
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_32655 a
WHERE
a.statesplit IN (0,1);

ALTER TABLE cipsrv_nhdplus_h.catchment_32655_state OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32655_state TO public;
--******************************--
----- views/catchment_32702_full.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_32702_full
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_32702 a
WHERE
a.statesplit IN (0,2);

ALTER TABLE cipsrv_nhdplus_h.catchment_32702_full OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32702_full TO public;
--******************************--
----- views/catchment_32702_state.sql 

CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_32702_state
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_32702 a
WHERE
a.statesplit IN (0,1);

ALTER TABLE cipsrv_nhdplus_h.catchment_32702_state OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32702_state TO public;
--******************************--
----- types/flowline.sql 

DROP TYPE IF EXISTS cipsrv_nhdplus_h.flowline CASCADE;

CREATE TYPE cipsrv_nhdplus_h.flowline 
AS(
    nhdplusid             BIGINT
   ,hydroseq              BIGINT
   ,fmeasure              NUMERIC
   ,tmeasure              NUMERIC
   ,levelpathi            BIGINT
   ,terminalpa            BIGINT
   ,uphydroseq            BIGINT
   ,dnhydroseq            BIGINT
   ,dnminorhyd            BIGINT
   ,divergence            INTEGER
   ,streamleve            INTEGER
   ,arbolatesu            NUMERIC
   ,fromnode              BIGINT
   ,tonode                BIGINT
   ,vpuid                 VARCHAR(8)
   /* ++++++++++ */
   ,permanent_identifier  VARCHAR(40)
   ,reachcode             VARCHAR(14)
   ,fcode                 INTEGER
   /* ++++++++++ */
   ,lengthkm              NUMERIC
   ,lengthkm_ratio        NUMERIC
   ,flowtimeday           NUMERIC
   ,flowtimeday_ratio     NUMERIC
   /* ++++++++++ */
   ,pathlengthkm          NUMERIC
   ,pathflowtimeday       NUMERIC
   /* ++++++++++ */
   ,out_grid_srid         INTEGER
   ,out_measure           NUMERIC
   ,out_lengthkm          NUMERIC
   ,out_flowtimeday       NUMERIC
   ,out_pathlengthkm      NUMERIC
   ,out_pathflowtimeday   NUMERIC
   ,out_node              BIGINT
);

ALTER TYPE cipsrv_nhdplus_h.flowline OWNER TO cipsrv;

GRANT USAGE ON TYPE cipsrv_nhdplus_h.flowline TO PUBLIC;

--******************************--
----- types/snapflowline.sql 

DROP TYPE IF EXISTS cipsrv_nhdplus_h.snapflowline CASCADE;

CREATE TYPE cipsrv_nhdplus_h.snapflowline 
AS(
    permanent_identifier        VARCHAR(40)
   ,fdate                       DATE
   ,resolution                  INTEGER
   ,gnis_id                     VARCHAR(10)
   ,gnis_name                   VARCHAR(65)
   ,lengthkm                    NUMERIC
   ,reachcode                   VARCHAR(14)
   ,flowdir                     INTEGER
   ,wbarea_permanent_identifier VARCHAR(40)
   ,ftype                       INTEGER
   ,fcode                       INTEGER
   ,mainpath                    INTEGER
   ,innetwork                   INTEGER
   ,visibilityfilter            INTEGER
   ,nhdplusid                   BIGINT
   ,vpuid                       VARCHAR(16)
   ,enabled                     INTEGER
   ,fmeasure                    NUMERIC
   ,tmeasure                    NUMERIC
   ,hydroseq                    BIGINT
   ,shape                       GEOMETRY
   -----
   ,snap_measure                NUMERIC
   ,snap_distancekm             NUMERIC
   ,snap_point                  GEOMETRY
);

ALTER TYPE cipsrv_nhdplus_h.snapflowline OWNER TO cipsrv;

GRANT USAGE ON TYPE cipsrv_nhdplus_h.snapflowline TO PUBLIC;

--******************************--
----- functions/catconstrained_index.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.catconstrained_index';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.catconstrained_index(
    IN  p_point                     GEOMETRY
   ,IN  p_return_link_path          BOOLEAN
   ,IN  p_known_region              VARCHAR
   ,IN  p_known_catchment_nhdplusid BIGINT DEFAULT NULL
   ,OUT out_flowlines               cipsrv_nhdplus_h.snapflowline[]
   ,OUT out_path_distance_km        NUMERIC
   ,OUT out_end_point               GEOMETRY
   ,OUT out_indexing_line           GEOMETRY
   ,OUT out_region                  VARCHAR
   ,OUT out_nhdplusid               BIGINT
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                     RECORD;
   boo_return_link_path    BOOLEAN  := p_return_link_path;
   int_raster_srid         INTEGER;
   sdo_input               GEOMETRY;
   rec_candidate           cipsrv_nhdplus_h.snapflowline;
   int_nhdplusid           BIGINT;
   boo_issink              BOOLEAN;
   boo_isocean             BOOLEAN;
   boo_isalaskan           BOOLEAN;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF p_point IS NULL OR ST_IsEmpty(p_point) 
   OR ST_GeometryType(p_point) <> 'ST_Point'
   THEN
      out_return_code    := -99;
      out_status_message := 'Input must be point geometry.';
      RETURN;
      
   END IF;
   
   IF boo_return_link_path IS NULL
   THEN
      boo_return_link_path := FALSE;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   out_region         := rec.out_srid::VARCHAR;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
     
   --------------------------------------------------------------------------
   -- Step 30
   -- Project input point if required
   --------------------------------------------------------------------------
   IF ST_SRID(p_point) = int_raster_srid
   THEN
      sdo_input := p_point;
      
   ELSE
      sdo_input := ST_Transform(p_point,int_raster_srid);
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 40
   -- Determine the catchment if not provided
   --------------------------------------------------------------------------
   IF p_known_catchment_nhdplusid IS NOT NULL
   THEN
      int_nhdplusid := p_known_catchment_nhdplusid;
      
   ELSE
      IF int_raster_srid = 5070
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          int_nhdplusid
         ,boo_issink
         ,boo_isocean
         ,boo_isalaskan
         FROM
         cipsrv_nhdplus_h.catchment_5070 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSIF int_raster_srid = 3338
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          int_nhdplusid
         ,boo_issink
         ,boo_isocean
         ,boo_isalaskan
         FROM
         cipsrv_nhdplus_h.catchment_3338 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSIF int_raster_srid = 22904
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          int_nhdplusid
         ,boo_issink
         ,boo_isocean
         ,boo_isalaskan
         FROM
         cipsrv_nhdplus_h.catchment_22904 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSIF int_raster_srid = 32161
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          int_nhdplusid
         ,boo_issink
         ,boo_isocean
         ,boo_isalaskan
         FROM
         cipsrv_nhdplus_h.catchment_32161 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSIF int_raster_srid = 32655
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          int_nhdplusid
         ,boo_issink
         ,boo_isocean
         ,boo_isalaskan
         FROM
         cipsrv_nhdplus_h.catchment_32655 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSIF int_raster_srid = 32702
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
         int_nhdplusid
         FROM
         cipsrv_nhdplus_h.catchment_32702 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSE
         RAISE EXCEPTION 'err';
      
      END IF;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Bail if no results
   --------------------------------------------------------------------------
   IF int_nhdplusid IS NULL
   THEN
      out_return_code    := -2;
      out_status_message := 'no results found';
      RETURN;
   
   END IF;
   
   IF boo_issink
   OR boo_isocean
   OR boo_isalaskan
   THEN
      out_return_code    := -3;
      out_status_message := 'catchment without flowline for indexing';
      out_nhdplusid      := int_nhdplusid;
      RETURN;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Pull the matching flowline
   --------------------------------------------------------------------------
   IF int_raster_srid = 5070
   THEN
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,ROUND(
           a.snap_measure::NUMERIC
          ,5
       ) AS snap_measure
      ,a.snap_distancekm
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
      ,rec_candidate.reachcode
      ,rec_candidate.flowdir
      ,rec_candidate.wbarea_permanent_identifier
      ,rec_candidate.ftype
      ,rec_candidate.fcode
      ,rec_candidate.mainpath
      ,rec_candidate.innetwork
      ,rec_candidate.visibilityfilter
      ,rec_candidate.nhdplusid
      ,rec_candidate.vpuid
      ,rec_candidate.enabled
      ,rec_candidate.fmeasure
      ,rec_candidate.tmeasure
      ,rec_candidate.hydroseq
      ,rec_candidate.shape
      ,rec_candidate.snap_measure
      ,rec_candidate.snap_distancekm
      ,rec_candidate.snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_5070 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
   
   ELSIF int_raster_srid = 3338
   THEN
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,ROUND(
           a.snap_measure::NUMERIC
          ,5
       ) AS snap_measure
      ,a.snap_distancekm
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
      ,rec_candidate.reachcode
      ,rec_candidate.flowdir
      ,rec_candidate.wbarea_permanent_identifier
      ,rec_candidate.ftype
      ,rec_candidate.fcode
      ,rec_candidate.mainpath
      ,rec_candidate.innetwork
      ,rec_candidate.visibilityfilter
      ,rec_candidate.nhdplusid
      ,rec_candidate.vpuid
      ,rec_candidate.enabled
      ,rec_candidate.fmeasure
      ,rec_candidate.tmeasure
      ,rec_candidate.hydroseq
      ,rec_candidate.shape
      ,rec_candidate.snap_measure
      ,rec_candidate.snap_distancekm
      ,rec_candidate.snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_3338 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
   
   ELSIF int_raster_srid = 26904
   THEN
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,ROUND(
           a.snap_measure::NUMERIC
          ,5
       ) AS snap_measure
      ,a.snap_distancekm
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
      ,rec_candidate.reachcode
      ,rec_candidate.flowdir
      ,rec_candidate.wbarea_permanent_identifier
      ,rec_candidate.ftype
      ,rec_candidate.fcode
      ,rec_candidate.mainpath
      ,rec_candidate.innetwork
      ,rec_candidate.visibilityfilter
      ,rec_candidate.nhdplusid
      ,rec_candidate.vpuid
      ,rec_candidate.enabled
      ,rec_candidate.fmeasure
      ,rec_candidate.tmeasure
      ,rec_candidate.hydroseq
      ,rec_candidate.shape
      ,rec_candidate.snap_measure
      ,rec_candidate.snap_distancekm
      ,rec_candidate.snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_26904 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32161
   THEN
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,ROUND(
           a.snap_measure::NUMERIC
          ,5
       ) AS snap_measure
      ,a.snap_distancekm
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
      ,rec_candidate.reachcode
      ,rec_candidate.flowdir
      ,rec_candidate.wbarea_permanent_identifier
      ,rec_candidate.ftype
      ,rec_candidate.fcode
      ,rec_candidate.mainpath
      ,rec_candidate.innetwork
      ,rec_candidate.visibilityfilter
      ,rec_candidate.nhdplusid
      ,rec_candidate.vpuid
      ,rec_candidate.enabled
      ,rec_candidate.fmeasure
      ,rec_candidate.tmeasure
      ,rec_candidate.hydroseq
      ,rec_candidate.shape
      ,rec_candidate.snap_measure
      ,rec_candidate.snap_distancekm
      ,rec_candidate.snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32161 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32655
   THEN
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,ROUND(
           a.snap_measure::NUMERIC
          ,5
       ) AS snap_measure
      ,a.snap_distancekm
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
      ,rec_candidate.reachcode
      ,rec_candidate.flowdir
      ,rec_candidate.wbarea_permanent_identifier
      ,rec_candidate.ftype
      ,rec_candidate.fcode
      ,rec_candidate.mainpath
      ,rec_candidate.innetwork
      ,rec_candidate.visibilityfilter
      ,rec_candidate.nhdplusid
      ,rec_candidate.vpuid
      ,rec_candidate.enabled
      ,rec_candidate.fmeasure
      ,rec_candidate.tmeasure
      ,rec_candidate.hydroseq
      ,rec_candidate.shape
      ,rec_candidate.snap_measure
      ,rec_candidate.snap_distancekm
      ,rec_candidate.snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32655 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32702
   THEN
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,ROUND(
           a.snap_measure::NUMERIC
          ,5
       ) AS snap_measure
      ,a.snap_distancekm
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
      ,rec_candidate.reachcode
      ,rec_candidate.flowdir
      ,rec_candidate.wbarea_permanent_identifier
      ,rec_candidate.ftype
      ,rec_candidate.fcode
      ,rec_candidate.mainpath
      ,rec_candidate.innetwork
      ,rec_candidate.visibilityfilter
      ,rec_candidate.nhdplusid
      ,rec_candidate.vpuid
      ,rec_candidate.enabled
      ,rec_candidate.fmeasure
      ,rec_candidate.tmeasure
      ,rec_candidate.hydroseq
      ,rec_candidate.shape
      ,rec_candidate.snap_measure
      ,rec_candidate.snap_distancekm
      ,rec_candidate.snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32702 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Generate snap path if requested
   --------------------------------------------------------------------------
   out_flowlines[1]     := rec_candidate;
   out_path_distance_km := out_flowlines[1].snap_distancekm;
   out_end_point        := out_flowlines[1].snap_point; 
   out_nhdplusid        := out_flowlines[1].nhdplusid;
   
   IF p_return_link_path
   AND out_path_distance_km > 0.00005
   THEN
      out_indexing_line := ST_MakeLine(
          ST_Transform(sdo_input,4269)
         ,out_end_point
      );
  
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 90
   -- Check for problems and mismatches
   --------------------------------------------------------------------------
   IF out_nhdplusid IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'Error matching catchment to flowline <<' || int_nhdplusid::VARCHAR || '>>';
      RETURN;
      
   END IF;


END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.catconstrained_index(
    GEOMETRY
   ,BOOLEAN
   ,VARCHAR
   ,BIGINT
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.catconstrained_index(
    GEOMETRY
   ,BOOLEAN
   ,VARCHAR
   ,BIGINT
) TO PUBLIC;
--******************************--
----- functions/delineate.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.delineate';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.delineate(
    IN  p_search_type                  VARCHAR
   ,IN  p_start_nhdplusid              BIGINT
   ,IN  p_start_permanent_identifier   VARCHAR
   ,IN  p_start_reachcode              VARCHAR
   ,IN  p_start_hydroseq               BIGINT
   ,IN  p_start_measure                NUMERIC
   ,IN  p_stop_nhdplusid               BIGINT
   ,IN  p_stop_permanent_identifier    VARCHAR
   ,IN  p_stop_reachcode               VARCHAR
   ,IN  p_stop_hydroseq                BIGINT
   ,IN  p_stop_measure                 NUMERIC
   ,IN  p_max_distancekm               NUMERIC
   ,IN  p_max_flowtimeday              NUMERIC
   ,IN  p_aggregation_engine           VARCHAR
   ,IN  p_split_initial_catchment      BOOLEAN
   ,IN  p_fill_basin_holes             BOOLEAN
   ,IN  p_force_no_cache               BOOLEAN
   ,IN  p_return_delineation_geometry  BOOLEAN
   ,IN  p_return_flowlines             BOOLEAN   
   ,IN  p_return_flowline_details      BOOLEAN
   ,IN  p_return_flowline_geometry     BOOLEAN
   ,IN  p_known_region                 VARCHAR DEFAULT NULL
   
   ,OUT out_aggregation_used           VARCHAR
   ,OUT out_start_nhdplusid            BIGINT
   ,OUT out_start_permanent_identifier VARCHAR
   ,OUT out_start_measure              NUMERIC
   ,OUT out_grid_srid                  INTEGER
   ,OUT out_stop_nhdplusid             BIGINT
   ,OUT out_stop_measure               NUMERIC
   ,OUT out_flowline_count             INTEGER
   ,OUT out_return_flowlines           BOOLEAN
   ,OUT out_return_code                NUMERIC
   ,OUT out_status_message             VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                             RECORD;
   str_search_type                 VARCHAR(16) := UPPER(p_search_type); 
   obj_start_flowline              cipsrv_nhdplus_h.flowline;
   obj_stop_flowline               cipsrv_nhdplus_h.flowline;
   num_maximum_distancekm          NUMERIC     := p_max_distancekm;
   num_maximum_flowtimeday         NUMERIC     := p_max_flowtimeday;
   int_counter                     INTEGER;
   int_check                       INTEGER;
   int_catchments                  INTEGER;
   sdo_splitpoint                  GEOMETRY;
   sdo_split_catchment             GEOMETRY;
   sdo_topo_output                 GEOMETRY;
   sdo_output                      GEOMETRY;
   num_area                        NUMERIC;
   str_aggregation_engine          VARCHAR := UPPER(p_aggregation_engine);
   boo_aggregation_flag            BOOLEAN;
   boo_topology_flag               BOOLEAN;
   boo_split_initial_catchment     BOOLEAN;
   boo_fill_basin_holes            BOOLEAN;
   boo_return_delineation_geometry BOOLEAN;
   boo_return_flowlines            BOOLEAN;
   boo_return_flowline_details     BOOLEAN;
   boo_return_flowline_geometry    BOOLEAN;
   boo_zero_length_delin           BOOLEAN;   
   boo_force_no_cache              BOOLEAN;
   boo_cached_watershed            BOOLEAN;

BEGIN

   out_return_code := cipsrv_engine.create_delineation_temp_tables();

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   out_return_code    := 0;
   obj_start_flowline := NULL::cipsrv_nhdplus_h.flowline;
   obj_stop_flowline  := NULL::cipsrv_nhdplus_h.flowline;
   
   IF str_search_type IN ('PP','POINT TO POINT','POINT-TO-POINT')
   THEN
      str_search_type := 'PP';
      
   ELSIF str_search_type IN ('PPALL')
   THEN
      str_search_type := 'PPALL';
   
   ELSIF str_search_type IN ('UT','UPSTREAM WITH TRIBUTARIES')
   THEN
      str_search_type := 'UT';
   
   ELSIF str_search_type IN ('UM','UPSTREAM MAIN PATH ONLY')
   THEN
      str_search_type := 'UM';
   
   ELSIF str_search_type IN ('DD','DOWNSTREAM WITH DIVERGENCES')
   THEN
      str_search_type := 'DD';
   
   ELSIF str_search_type IN ('DM','DOWNSTREAM MAIN PATH ONLY')
   THEN
      str_search_type := 'DM';
      
   ELSE
      out_return_code    := -1;
      out_status_message := 'Valid SearchType codes are UM, UT, DM, DD and PP.';

   END IF;

   IF str_search_type IN ('PP','PPALL')
   THEN
      num_maximum_distancekm  := NULL;
      num_maximum_flowtimeday := NULL;

   END IF;

   IF  num_maximum_distancekm  IS NOT NULL
   AND num_maximum_flowtimeday IS NOT NULL
   THEN
      num_maximum_flowtimeday := NULL;

   END IF;
   
   IF str_aggregation_engine IN ('SPATIAL')
   THEN
      str_aggregation_engine := 'SPATIAL';
      boo_aggregation_flag   := TRUE;
      boo_topology_flag      := FALSE;
      
   ELSIF str_aggregation_engine IN ('TOPO')
   THEN
      str_aggregation_engine := 'TOPO';
      boo_aggregation_flag   := TRUE;
      boo_topology_flag      := TRUE;
   
   ELSIF str_aggregation_engine IN ('NONE')
   THEN
      str_aggregation_engine := 'NONE';
      boo_aggregation_flag   := FALSE;
      boo_topology_flag      := FALSE;
   
   ELSE
      str_aggregation_engine := 'TOPO';
      boo_aggregation_flag   := TRUE;
      boo_topology_flag      := TRUE;
      
   END IF;
   
   boo_split_initial_catchment := p_split_initial_catchment;
   IF boo_split_initial_catchment IS NULL
   THEN
      boo_split_initial_catchment := TRUE;
      
   END IF;
   
   boo_fill_basin_holes := p_fill_basin_holes;
   IF boo_fill_basin_holes IS NULL
   THEN
      boo_fill_basin_holes := TRUE;
      
   END IF;
   
   boo_force_no_cache := p_force_no_cache;
   IF boo_force_no_cache IS NULL
   THEN
      boo_force_no_cache := FALSE;
      
   END IF;
   
   boo_return_delineation_geometry := p_return_delineation_geometry;
   IF boo_return_delineation_geometry IS NULL
   THEN
      boo_return_delineation_geometry := TRUE;
      
   END IF;
   
   boo_return_flowlines := p_return_flowlines;
   IF boo_return_flowlines IS NULL
   THEN
      boo_return_flowlines := FALSE;
      
   END IF;
   
   boo_return_flowline_details := p_return_flowline_details;
   IF boo_return_flowline_details IS NULL
   THEN
      boo_return_flowline_details := TRUE;
      
   END IF;
   
   boo_return_flowline_geometry := p_return_flowline_geometry;
   IF boo_return_flowline_geometry IS NULL
   THEN
      boo_return_flowline_geometry := TRUE;
      
   END IF;
   
   boo_cached_watershed  := FALSE;
   boo_zero_length_delin := FALSE;
   
   BEGIN
      out_grid_srid := p_known_region::INTEGER;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine special conditions
   ----------------------------------------------------------------------------
   IF  num_maximum_distancekm IS NULL
   AND num_maximum_flowtimeday IS NULL
   AND NOT boo_split_initial_catchment
   AND NOT boo_return_flowlines
   AND str_aggregation_engine != 'NONE'
   THEN
      boo_cached_watershed := TRUE;
      
   ELSIF num_maximum_distancekm  = 0
   OR    num_maximum_flowtimeday = 0
   THEN
      boo_zero_length_delin := TRUE;
      
   END IF;
   
   IF boo_force_no_cache
   THEN
      boo_cached_watershed := FALSE;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- For cached watershed or zero delineation, 
   -- verify that nhdplusid matches actual existing catchment
   ----------------------------------------------------------------------------
   IF boo_cached_watershed
   OR boo_zero_length_delin
   THEN
      rec := cipsrv_nhdplus_h.get_flowline(
          p_direction            := str_search_type
         ,p_nhdplusid            := p_start_nhdplusid
         ,p_permanent_identifier := p_start_permanent_identifier
         ,p_reachcode            := p_start_reachcode
         ,p_hydroseq             := p_start_hydroseq
         ,p_measure              := p_start_measure
         ,p_known_region         := out_grid_srid::VARCHAR
      );
      out_return_code        := rec.out_return_code;
      out_status_message     := rec.out_status_message;

      IF out_return_code <> 0
      THEN
         IF out_return_code = -10
         THEN
            out_flowline_count := 0;
            
            out_status_message := 'Flowline ' || COALESCE(
                p_start_nhdplusid::VARCHAR
               ,p_start_permanent_identifier
               ,p_start_reachcode
               ,p_start_hydroseq::VARCHAR
               ,'err'
            );
            
            IF p_start_measure IS NOT NULL
            THEN
               out_status_message := out_status_message || ' at measure ' || p_start_measure::VARCHAR;
               
            END IF;
            
            out_status_message := out_status_message || ' not found in NHDPlus stream network.';
            
         END IF;
         
         RETURN;
         
      END IF;

      out_grid_srid          := rec.out_region::INTEGER;
      out_start_nhdplusid    := (rec.out_flowline).nhdplusid;
      out_start_measure      := (rec.out_flowline).out_measure;
      out_start_permanent_identifier := (rec.out_flowline).permanent_identifier;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Run the Navigation Service
   ----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   THEN
      rec := cipsrv_nhdplus_h.navigate(
          p_search_type                := str_search_type
         ,p_start_nhdplusid            := p_start_nhdplusid
         ,p_start_permanent_identifier := p_start_permanent_identifier
         ,p_start_reachcode            := p_start_reachcode
         ,p_start_hydroseq             := p_start_hydroseq
         ,p_start_measure              := p_start_measure
         ,p_stop_nhdplusid             := p_stop_nhdplusid
         ,p_stop_permanent_identifier  := p_stop_permanent_identifier
         ,p_stop_reachcode             := p_stop_reachcode
         ,p_stop_hydroseq              := p_stop_hydroseq
         ,p_stop_measure               := p_stop_measure
         ,p_max_distancekm             := num_maximum_distancekm
         ,p_max_flowtimeday            := num_maximum_flowtimeday
         ,p_return_flowline_details    := boo_return_flowline_details
         ,p_return_flowline_geometry   := boo_return_flowline_geometry
         ,p_known_region               := out_grid_srid::VARCHAR
      );
      
      out_return_code                := rec.out_return_code;
      out_status_message             := rec.out_status_message;
      out_start_nhdplusid            := rec.out_start_nhdplusid;
      out_start_measure              := rec.out_start_measure;
      out_start_permanent_identifier := rec.out_start_permanent_identifier;
      out_grid_srid                  := rec.out_grid_srid;   
      out_flowline_count             := rec.out_flowline_count;
      out_stop_nhdplusid             := rec.out_stop_nhdplusid;
      out_stop_measure               := rec.out_stop_measure;
      
   END IF;
   raise warning '%',out_grid_srid;
   ----------------------------------------------------------------------------
   -- Step 60
   -- Examine the navigation results
   ----------------------------------------------------------------------------
   IF  out_return_code = -56600
   AND str_search_type <> 'PP'
   THEN
       -- When directional navigation hits the coastal response, flip to zero length
      out_return_code := 0;
      out_status_message := NULL;
      boo_zero_length_delin := TRUE;
      
      rec := cipsrv_nhdplus_h.get_flowline(
          p_direction            := str_search_type
         ,p_nhdplusid            := p_start_nhdplusid
         ,p_permanent_identifier := p_start_permanent_identifier
         ,p_reachcode            := p_start_reachcode
         ,p_hydroseq             := p_start_hydroseq
         ,p_measure              := p_start_measure
         ,p_known_region         := out_grid_srid::VARCHAR
      );
      out_return_code        := rec.out_return_code;
      out_status_message     := rec.out_status_message;
      
      IF out_return_code <> 0
      THEN
         IF out_return_code = -10
         THEN
            out_flowline_count := 0;
            
            out_status_message := 'Flowline ' || COALESCE(
                p_start_nhdplusid::VARCHAR
               ,p_start_permanent_identifier
               ,p_start_reachcode
               ,p_start_hydroseq::VARCHAR
               ,'err'
            );
            
            IF p_start_measure IS NOT NULL
            THEN
               out_status_message := out_status_message || ' at measure ' || p_start_measure::VARCHAR;
               
            END IF;
            
            out_status_message := out_status_message || ' not found in NHDPlus stream network.';
            
         END IF;
         
         RETURN;
         
      END IF;
      
   END IF;
   
   IF out_return_code <> 0
   THEN
      IF out_return_code = 302
      THEN
         -- Message is per Tommy Dewald.  Do not change without checking with Tommy first
         out_status_message := 'The selected flowline (stream segment) is not connected to the NHDPlus stream network';
         
      END IF;
      
      RETURN;
      
   END IF;

   -----------------------------------------------------------------------------
   -- Step 70
   -- Run Catchment Delineation Preprocessing 
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   AND NOT boo_return_flowlines
   THEN
      rec := cipsrv_nhdplus_h.delineation_preprocessing(
          p_aggregation_flag             := boo_aggregation_flag
         ,p_srid                         := out_grid_srid
         ,p_return_delineation_geometry  := boo_return_delineation_geometry
         ,p_return_topology_results      := FALSE
         ,p_extra_catchments             := NULL
      );
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      int_catchments     := rec.out_records_inserted;

      IF out_return_code <> 0
      THEN
         out_flowline_count := 0;
         RETURN;
      
      END IF;
      
   END IF;

   -----------------------------------------------------------------------------
   -- Step 80
   -- Fetch the split catchment geometry if required
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   AND boo_split_initial_catchment
   THEN
      IF str_search_type <> 'UT'
      THEN
         out_return_code := -700;
         out_status_message := 'Catchment split only supported for upstream with tributary navigation';
         out_flowline_count := 0;
         
         RETURN;
         
      END IF;
   
      -- This might need to be adjusted to be a bit further downstream, dunno
      sdo_splitpoint := cipsrv_nhdplus_h.point_at_measure(
          p_nhdplusid            := out_start_nhdplusid
         ,p_permanent_identifier := NULL
         ,p_reachcode            := NULL 
         ,p_measure              := out_start_measure
         ,p_2d_flag              := TRUE
      );

      sdo_split_catchment := cipsrv_nhdplus_h.split_catchment(
          p_split_point  := ST_Transform(sdo_splitpoint,out_grid_srid)
         ,p_nhdplusid    := out_start_nhdplusid
         ,p_known_region := out_grid_srid::VARCHAR
      );
      --raise warning '% %',int_catchments, st_astext(st_transform(st_collect(sdo_splitpoint,NULL),4269));
      
      IF sdo_split_catchment IS NULL
      THEN
         out_flowline_count := 0;
         out_return_code := -702;
         out_status_message := 'Catchment split process failed';
         
         RETURN;
         
      END IF;
   
   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 90
   -- Swap in the split catchment for catchment and geometric aggregation
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   AND boo_split_initial_catchment
   THEN
      IF boo_topology_flag
      THEN
         DELETE FROM tmp_catchments a
         WHERE
         a.nhdplusid  = out_start_nhdplusid;
         
      ELSE
         IF out_grid_srid = 5070
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_5070  = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = out_start_nhdplusid;
            
         ELSIF out_grid_srid = 3338
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_3338  = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = out_start_nhdplusid;
            
         ELSIF out_grid_srid = 26904
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_26904 = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = out_start_nhdplusid;
            
         ELSIF out_grid_srid = 32161
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_32161 = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = out_start_nhdplusid;
            
         ELSIF out_grid_srid = 32655
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_32655 = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = out_start_nhdplusid;
            
         ELSIF out_grid_srid = 32702
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_32702 = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = out_start_nhdplusid;
            
         END IF;

      END IF;
      
   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 100
   -- Branch based on aggregation decision - return aggregated catchment poly
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   THEN
      IF boo_topology_flag
      THEN
         rec := cipsrv_nhdplus_h.load_topo_catchment(
             p_grid_srid       := out_grid_srid
            ,p_catchment_count := int_catchments
         );
         out_return_code     := rec.out_return_code;
         out_status_message  := rec.out_status_message;
         sdo_topo_output     := rec.out_geometry;
      
         IF out_return_code <> 0
         THEN
            out_flowline_count := 0;
            RETURN;
         
         END IF;
         
      ELSE
         rec := cipsrv_nhdplus_h.load_aggregated_catchment(
             p_grid_srid       := out_grid_srid
         );
         out_return_code     := rec.out_return_code;
         out_status_message  := rec.out_status_message;
      
         IF out_return_code <> 0
         THEN
            out_flowline_count := 0;
            RETURN;
         
         END IF;         
         
      END IF;
   
   END IF;
   
    -----------------------------------------------------------------------------
   -- Step 110
   -- Add split catchment to topo aggregation if requested
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   AND boo_split_initial_catchment
   AND boo_topology_flag
   THEN
      SELECT
      ST_Union(ST_SnaptoGrid(a.geom,0.05))
      INTO sdo_topo_output
      FROM (
         SELECT 
         (ST_Dump(
            ST_Collect(sdo_topo_output,ST_Buffer(sdo_split_catchment,0.01))
         )).*
      ) a;      
      
      UPDATE tmp_catchments a
      SET
       shape    = ST_Transform(sdo_topo_output,4269)
      ,areasqkm = ROUND(ST_Area(sdo_topo_output)::NUMERIC * 0.000001,5)
      WHERE
      a.nhdplusid  = -9999999;

   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 120
   -- Migrate catchment geometry if requested
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   AND boo_aggregation_flag
   THEN
      IF out_grid_srid = 5070
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_5070,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      ELSIF out_grid_srid = 3338
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_3338,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      ELSIF out_grid_srid = 26904
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_26904,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      ELSIF out_grid_srid = 32161
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_32161,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      ELSIF out_grid_srid = 32655
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_32655,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      ELSIF out_grid_srid = 32702
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_32702,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      END IF;
   
   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 130
   -- Fetch cached watershed if requested
   -----------------------------------------------------------------------------
   IF boo_cached_watershed
   THEN
      INSERT INTO tmp_catchments(
          nhdplusid
         ,sourcefc
         ,gridcode
         ,areasqkm
         ,shape
      )
      SELECT
       -9999999
      ,'AGGR'
      ,-9999
      ,a.areasqkm
      ,a.shape
      FROM
      cipsrv_nhdpluswshd_h.catchment_watershed a
      WHERE
      a.nhdplusid = out_start_nhdplusid;

   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 140
   -- Fetch zero distance catchment
   -----------------------------------------------------------------------------
   IF boo_zero_length_delin
   THEN
      IF boo_return_flowlines
      THEN
         INSERT INTO tmp_navigation_results(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,lengthkm
            ,flowtimeday
            /* ++++++++++ */
            ,network_distancekm
            ,network_flowtimeday
            /* ++++++++++ */
            ,permanent_identifier
            ,reachcode
            ,fcode
            ,gnis_id
            ,gnis_name
            ,wbarea_permanent_identifier
            /* ++++++++++ */
            ,navtermination_flag
            ,shape
            ,nav_order
         )
         SELECT
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,a.lengthkm
         ,a.flowtimeday
         /* ++++++++++ */
         ,a.lengthkm    AS network_distancekm
         ,a.flowtimeday AS network_flowtimeday
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         ,a.gnis_id
         ,a.gnis_name
         ,a.wbarea_permanent_identifier
         /* ++++++++++ */
         ,NULL
         ,a.shape
         ,1
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE
         a.nhdplusid = out_start_nhdplusid;
         
      END IF;
      
      IF boo_split_initial_catchment
      THEN
         sdo_splitpoint := cipsrv_nhdplus_h.point_at_measure(
             p_nhdplusid             := out_start_nhdplusid
            ,p_permanent_identifier  := NULL
            ,p_reachcode             := NULL 
            ,p_measure               := out_start_measure
            ,p_2d_flag               := 'TRUE'
         );

         sdo_split_catchment := cipsrv_nhdplus_h.split_catchment(
             p_split_point  := ST_Transform(sdo_splitpoint,out_grid_srid)
            ,p_nhdplusid    := out_start_nhdplusid
            ,p_known_region := out_grid_srid::VARCHAR
         );

         IF sdo_split_catchment IS NULL
         THEN
            out_return_code := -702;
            out_status_message := 'Catchment split process failed';
            out_flowline_count := 0;
            
            RETURN;
            
         END IF;
         
         num_area := ROUND(
             ST_Area(ST_Transform(sdo_split_catchment,4326)::GEOGRAPHY)::NUMERIC * 0.000001
            ,5
         );
         
         sdo_output := ST_Transform(sdo_split_catchment,4269);
         
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,gridcode
            ,areasqkm
            ,shape
         ) VALUES (
             -9999999
            ,'AGGR'
            ,-9999
            ,num_area
            ,sdo_output
         );
  
      END IF;

   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 150
   -- Remove holes if requested
   -----------------------------------------------------------------------------
   IF boo_fill_basin_holes
   THEN
      UPDATE tmp_catchments a
      SET
      shape = cipsrv_engine.remove_holes(a.shape)
      WHERE
      a.sourcefc = 'AGGR';
      
      UPDATE tmp_catchments a
      SET
      areasqkm = ROUND(ST_Area(ST_Transform(a.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001,5)
      WHERE
      a.sourcefc = 'AGGR';

   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.delineate(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,VARCHAR
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.delineate(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,VARCHAR
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
)  TO PUBLIC;

--******************************--
----- functions/delineation_preprocessing.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.delineation_preprocessing';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.delineation_preprocessing(
    IN  p_aggregation_flag            BOOLEAN
   ,IN  p_srid                        INTEGER
   ,IN  p_return_delineation_geometry BOOLEAN
   ,IN  p_return_topology_results     BOOLEAN
   ,IN  p_extra_catchments            BIGINT[]
   ,OUT out_records_inserted          INTEGER
   ,OUT out_return_code               NUMERIC
   ,OUT out_status_message            VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   int_inserted INTEGER;
   
BEGIN
   
   out_return_code := 0;
   out_records_inserted := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- If no aggregation requested, then just load nad83 column
   ---------------------------------------------------------------------------- 
   IF NOT p_aggregation_flag
   THEN
      INSERT INTO tmp_catchments(
          nhdplusid
         ,sourcefc
         ,gridcode
         ,areasqkm
         ,hydroseq
         ,shape
      )
      SELECT
       b.nhdplusid
      ,b.areasqkm
      ,a.hydroseq
      ,CASE WHEN p_return_delineation_geometry THEN b.shape ELSE NULL::GEOMETRY END AS shape
      FROM
      tmp_navigation_results a
      JOIN
      cipsrv_nhdplus_h.nhdpluscatchment b
      ON
      b.nhdplusid = a.nhdplusid;

      GET DIAGNOSTICS int_inserted = ROW_COUNT;
      out_records_inserted := out_records_inserted + int_inserted;
      
      IF array_length(p_extra_catchments,1) > 0
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,areasqkm
            ,hydroseq
            ,shape
         )
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,0
         ,CASE WHEN p_return_delineation_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         cipsrv_nhdplus_h.nhdpluscatchment a
         WHERE
         a.nhdplusid = ANY(p_extra_catchments)
         ON CONFLICT DO NOTHING;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;

      END IF;
      
   -----------------------------------------------------------------------------
   -- Step 20
   -- Load the temp table based on the grid locality value
   -----------------------------------------------------------------------------
   ELSE
      IF p_srid = 5070
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,areasqkm
            ,hydroseq
            ,shape_5070
         )
         SELECT
          b.nhdplusid
         ,b.areasqkm
         ,a.hydroseq
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_5070_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,areasqkm
               ,hydroseq
               ,shape_5070
            )
            SELECT
             a.nhdplusid
            ,a.areasqkm
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_5070_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSIF p_srid = 3338
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,areasqkm
            ,hydroseq
            ,shape_3338
         )
         SELECT
          b.nhdplusid
         ,b.areasqkm
         ,a.hydroseq
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_3338_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,areasqkm
               ,hydroseq
               ,shape_3338
            )
            SELECT
             a.nhdplusid
            ,a.areasqkm
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_3338_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSIF p_srid = 26904
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,areasqkm
            ,hydroseq
            ,shape_26904
         )
         SELECT
          b.nhdplusid
         ,b.areasqkm
         ,a.hydroseq
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_26904_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,areasqkm
               ,hydroseq
               ,shape_26904
            )
            SELECT
             a.nhdplusid
            ,a.areasqkm
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_26904_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSIF p_srid = 32161
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,areasqkm
            ,hydroseq
            ,shape_32161
         )
         SELECT
          b.nhdplusid
         ,b.areasqkm
         ,a.hydroseq
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_32161_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,areasqkm
               ,hydroseq
               ,shape_32161
            )
            SELECT
             a.nhdplusid
            ,a.areasqkm
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_32161_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSIF p_srid = 32655
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,areasqkm
            ,hydroseq
            ,shape_32655
         )
         SELECT
          b.nhdplusid
         ,b.areasqkm
         ,a.hydroseq
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_32655_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,areasqkm
               ,hydroseq
               ,shape_32655
            )
            SELECT
             a.nhdplusid
            ,a.areasqkm
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_32655_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSIF p_srid = 32702
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,areasqkm
            ,hydroseq
            ,shape_32702
         )
         SELECT
          b.nhdplusid
         ,b.areasqkm
         ,a.hydroseq
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_32702_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,areasqkm
               ,hydroseq
               ,shape_32702
            )
            SELECT
             a.nhdplusid
            ,a.areasqkm
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_32702_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSE
         RAISE EXCEPTION 'err';
         
      END IF;   
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- I guess that went okay
   ----------------------------------------------------------------------------
   IF out_records_inserted IS NULL
   OR out_records_inserted = 0
   THEN
      out_return_code    := -101;
      out_status_message := 'No Catchments match navigation results';
      RETURN;

   END IF;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.delineation_preprocessing(
    BOOLEAN
   ,INTEGER
   ,BOOLEAN
   ,BOOLEAN
   ,BIGINT[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.delineation_preprocessing(
    BOOLEAN
   ,INTEGER
   ,BOOLEAN
   ,BOOLEAN
   ,BIGINT[]
) TO PUBLIC;

--******************************--
----- functions/determine_grid_srid.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.determine_grid_srid';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.determine_grid_srid(
    IN  p_geometry          GEOMETRY
   ,IN  p_known_region      VARCHAR
   ,OUT out_srid            INTEGER
   ,OUT out_grid_size       NUMERIC
   ,OUT out_return_code     INTEGER
   ,OUT out_status_message  VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   sdo_results        GEOMETRY;
   str_region         VARCHAR(255) := p_known_region;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  p_geometry IS NULL
   AND str_region IS NULL
   THEN
      RAISE EXCEPTION 'input geometry and known region cannot both be null';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine the region from geometry if known region value not provided
   ----------------------------------------------------------------------------
   IF str_region IS NULL
   THEN
      str_region := cipsrv_nhdplus_h.query_generic_common_mbr(
         p_input := p_geometry
      );
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Validate region and determine srid
   ----------------------------------------------------------------------------
   IF str_region IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'Geometry is outside nhdplus_h coverage.';
      RETURN;

   ELSIF str_region IN ('5070','CONUS','USA',
   'AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA',
   'IA','ID','IL','IN','KS','KY','LA','MA','MD','ME',
   'MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ',
   'NM','NV','NY','OH','OK','OR','PA','RI','SC','SD',
   'TN','TX','UT','VA','VT','WA','WI','WV','WY')
   THEN
      out_srid       := 5070;
      out_grid_size  := 10;
      
   ELSIF str_region IN ('3338','AK')
   THEN  
      out_srid       := 3338;
      out_grid_size  := NULL;
   
   ELSIF str_region IN ('32702','SAMOA','AS')
   THEN
      out_srid       := 32702;
      out_grid_size  := 10;
      
   ELSIF str_region IN ('32655','GUMP','GU','MP')
   THEN
      out_srid       := 32655;
      out_grid_size  := 10;
      
   ELSIF str_region IN ('26904','HI')
   THEN
      out_srid       := 26904;
      out_grid_size  := 10;
      
   ELSIF str_region IN ('32161','PRVI','PR','VI')
   THEN
      out_srid       := 32161;
      out_grid_size  := 10;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return what we got
   ----------------------------------------------------------------------------
   out_return_code := 0;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/distance_index.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.distance_index';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.distance_index(
    IN  p_point                     GEOMETRY
   ,IN  p_fcode_allow               INTEGER[]
   ,IN  p_fcode_deny                INTEGER[]
   ,IN  p_distance_max_distkm       NUMERIC
   ,IN  p_limit_innetwork           BOOLEAN
   ,IN  p_limit_navigable           BOOLEAN
   ,IN  p_return_link_path          BOOLEAN
   ,IN  p_known_region              VARCHAR
   ,OUT out_flowlines               cipsrv_nhdplus_h.snapflowline[]
   ,OUT out_path_distance_km        NUMERIC
   ,OUT out_end_point               GEOMETRY
   ,OUT out_indexing_line           GEOMETRY
   ,OUT out_region                  VARCHAR
   ,OUT out_nhdplusid               BIGINT
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                     RECORD;
   curs_candidates         REFCURSOR;
   num_distance_max_distkm NUMERIC  := p_distance_max_distkm;
   boo_limit_innetwork     BOOLEAN  := p_limit_innetwork;
   boo_limit_navigable     BOOLEAN  := p_limit_navigable;
   boo_return_link_path    BOOLEAN  := p_return_link_path;
   int_raster_srid         INTEGER;
   sdo_input               GEOMETRY;
   sdo_temp                GEOMETRY;
   rec_flowline            RECORD;
   rec_candidate           cipsrv_nhdplus_h.snapflowline;
   int_counter             INTEGER;
   num_nearest             NUMERIC;
   boo_check_fcode_allow   BOOLEAN := FALSE;
   boo_check_fcode_deny    BOOLEAN := FALSE;
   int_limit               INTEGER := 16;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF p_point IS NULL OR ST_IsEmpty(p_point) 
   OR ST_GeometryType(p_point) <> 'ST_Point'
   THEN
      out_return_code    := -99;
      out_status_message := 'Input must be point geometry.';
      RETURN;
      
   END IF;
   
   IF num_distance_max_distkm IS NULL
   OR num_distance_max_distkm <= 0
   THEN
      num_distance_max_distkm := 99999;
      
   END IF;
   
   IF p_fcode_allow IS NOT NULL
   AND array_length(p_fcode_allow,1) > 0
   THEN
      boo_check_fcode_allow := TRUE;
   
   END IF;
   
   IF p_fcode_deny IS NOT NULL
   AND array_length(p_fcode_deny,1) > 0
   THEN
      boo_check_fcode_deny := TRUE;
   
   END IF;
   
   IF boo_limit_innetwork IS NULL
   THEN
      boo_limit_innetwork := FALSE;
      
   END IF;
   
   IF boo_limit_navigable IS NULL
   THEN
      boo_limit_navigable := FALSE;
      
   END IF;
   
   IF boo_return_link_path IS NULL
   THEN
      boo_return_link_path := FALSE;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   out_region         := rec.out_srid::VARCHAR;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 30
   -- Project input point if required
   --------------------------------------------------------------------------
   IF ST_SRID(p_point) = int_raster_srid
   THEN
      sdo_input := p_point;
      
   ELSE
      sdo_input := ST_Transform(p_point,int_raster_srid);
      
   END IF;

   -------------------------------------------------------------------------
   -- Step 40
   -- Open the cursor required
   -------------------------------------------------------------------------
   IF int_raster_srid = 5070
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_5070 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
      
   ELSIF int_raster_srid = 3338
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_3338 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
   
   ELSIF int_raster_srid = 26904
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_26904 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
      
   ELSIF int_raster_srid = 32161
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32161 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
      
   ELSIF int_raster_srid = 32655
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32655 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
      
   ELSIF int_raster_srid = 32702
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32702 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
      
   ELSE
      RAISE EXCEPTION 'err';
   
   END IF;

   -------------------------------------------------------------------------
   -- Step 50
   -- Iterate the cursor into array of output type
   --------------------------------------------------------------------------
   num_nearest := 0;
   int_counter := 1;
   FETCH NEXT FROM curs_candidates INTO rec_flowline;
   WHILE (FOUND) 
   LOOP 
      rec_candidate.permanent_identifier        := rec_flowline.permanent_identifier;
      rec_candidate.fdate                       := rec_flowline.fdate;
      rec_candidate.resolution                  := rec_flowline.resolution::integer;
      rec_candidate.gnis_id                     := rec_flowline.gnis_id;
      rec_candidate.gnis_name                   := rec_flowline.gnis_name;
      rec_candidate.lengthkm                    := rec_flowline.lengthkm;
      rec_candidate.reachcode                   := rec_flowline.reachcode;
      rec_candidate.flowdir                     := rec_flowline.flowdir;
      rec_candidate.wbarea_permanent_identifier := rec_flowline.wbarea_permanent_identifier;
      rec_candidate.ftype                       := rec_flowline.ftype;
      rec_candidate.fcode                       := rec_flowline.fcode;
      rec_candidate.mainpath                    := rec_flowline.mainpath; 
      rec_candidate.innetwork                   := rec_flowline.innetwork;
      rec_candidate.visibilityfilter            := rec_flowline.visibilityfilter;
      rec_candidate.nhdplusid                   := rec_flowline.nhdplusid;
      rec_candidate.vpuid                       := rec_flowline.vpuid;
      rec_candidate.enabled                     := rec_flowline.enabled;
      rec_candidate.fmeasure                    := rec_flowline.fmeasure;
      rec_candidate.tmeasure                    := rec_flowline.tmeasure;
      rec_candidate.hydroseq                    := rec_flowline.hydroseq;
      rec_candidate.shape                       := ST_Transform(rec_flowline.shape,4269);
      
      rec_candidate.snap_measure := ROUND(ST_InterpolatePoint(
          rec_flowline.shape
         ,sdo_input
      )::NUMERIC,5);
      
      rec_candidate.snap_distancekm             := rec_flowline.snap_distancekm;
      
      sdo_temp := ST_GeometryN(ST_LocateAlong(
          rec_flowline.shape
         ,rec_candidate.snap_measure
      ),1);
      
      -- This is some weird numeric nuttiness which is corrected by reifying as text 
      IF sdo_temp IS NULL
      OR ST_IsEmpty(sdo_temp)
      THEN
         sdo_temp := ST_GeometryN(ST_LocateAlong(
             ST_GeomFromEWKT(ST_AsEWKT(rec_flowline.shape))
            ,rec_candidate.snap_measure
         ),1);
         
      END IF;
 
      rec_candidate.snap_point := ST_Transform(ST_Force2D(
         sdo_temp
      ),4269);

      IF num_nearest = 0
      THEN
         num_nearest = rec_candidate.snap_distancekm;
         
      END IF;
      
      EXIT WHEN rec_candidate.snap_distancekm > num_nearest;
      
      out_flowlines[int_counter] := rec_candidate;
      int_counter := int_counter + 1;
      
      FETCH NEXT FROM curs_candidates INTO rec_flowline;
      
   END LOOP; 
   
   CLOSE curs_candidates;

   --------------------------------------------------------------------------
   -- Step 60
   -- Bail if no results
   --------------------------------------------------------------------------
   IF int_counter = 1
   OR out_flowlines IS NULL
   OR array_length(out_flowlines,1) = 0
   THEN
      out_return_code    := -1;
      out_status_message := 'No results found';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Finalize the output
   --------------------------------------------------------------------------
   out_path_distance_km := out_flowlines[1].snap_distancekm;
   out_end_point        := out_flowlines[1].snap_point;
   out_nhdplusid        := out_flowlines[1].nhdplusid;
   
   IF p_return_link_path
   AND out_path_distance_km > 0.00005
   THEN
      out_indexing_line := ST_MakeLine(
          ST_Transform(sdo_input,4269)
         ,out_end_point
      );
  
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.distance_index(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.distance_index(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) TO PUBLIC;
--******************************--
----- functions/distance_index_simple.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.distance_index_simple';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.distance_index_simple(
    IN  p_point                     GEOMETRY
   ,IN  p_fcode_allow               INTEGER[]
   ,IN  p_fcode_deny                INTEGER[]
   ,IN  p_distance_max_distkm       NUMERIC
   ,IN  p_limit_innetwork           BOOLEAN
   ,IN  p_limit_navigable           BOOLEAN
   ,IN  p_known_region              VARCHAR
   ,OUT out_path_distance_km        NUMERIC
   ,OUT out_region                  VARCHAR
   ,OUT out_nhdplusid               BIGINT
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                     RECORD;
   curs_candidates         REFCURSOR;
   num_distance_max_distkm NUMERIC  := p_distance_max_distkm;
   boo_limit_innetwork     BOOLEAN  := p_limit_innetwork;
   boo_limit_navigable     BOOLEAN  := p_limit_navigable;
   int_raster_srid         INTEGER;
   sdo_input               GEOMETRY;
   rec_flowline            RECORD;
   int_counter             INTEGER;
   boo_check_fcode_allow   BOOLEAN := FALSE;
   boo_check_fcode_deny    BOOLEAN := FALSE;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF p_point IS NULL OR ST_IsEmpty(p_point) 
   OR ST_GeometryType(p_point) <> 'ST_Point'
   THEN
      out_return_code    := -99;
      out_status_message := 'Input must be point geometry.';
      RETURN;
      
   END IF;
   
   IF num_distance_max_distkm IS NULL
   OR num_distance_max_distkm <= 0
   THEN
      num_distance_max_distkm := 99999;
      
   END IF;
   
   IF p_fcode_allow IS NOT NULL
   AND array_length(p_fcode_allow,1) > 0
   THEN
      boo_check_fcode_allow := TRUE;
   
   END IF;
   
   IF p_fcode_deny IS NOT NULL
   AND array_length(p_fcode_deny,1) > 0
   THEN
      boo_check_fcode_deny := TRUE;
   
   END IF;
   
   IF boo_limit_innetwork IS NULL
   THEN
      boo_limit_innetwork := FALSE;
      
   END IF;
   
   IF boo_limit_navigable IS NULL
   THEN
      boo_limit_navigable := FALSE;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
     
   --------------------------------------------------------------------------
   -- Step 30
   -- Project input point if required
   --------------------------------------------------------------------------
   IF ST_SRID(p_point) = int_raster_srid
   THEN
      sdo_input := p_point;
      
   ELSE
      sdo_input := ST_Transform(p_point,int_raster_srid);
      
   END IF;

   -------------------------------------------------------------------------
   -- Step 40
   -- Open the cursor required
   -------------------------------------------------------------------------
   IF int_raster_srid = 5070
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_5070 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSIF int_raster_srid = 3338
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_3338 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSIF int_raster_srid = 26904
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_26904 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSIF int_raster_srid = 32161
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32161 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSIF int_raster_srid = 32655
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32655 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSIF int_raster_srid = 32702
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32702 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;
   
   -------------------------------------------------------------------------
   -- Step 50
   -- Iterate the cursor into array of output type
   --------------------------------------------------------------------------
   int_counter := 0;
   LOOP 
      FETCH curs_candidates INTO rec_flowline; 
      EXIT WHEN NOT FOUND; 
      
      int_counter          := int_counter + 1;
      out_nhdplusid        := rec_flowline.nhdplusid;
      out_path_distance_km := rec_flowline.snap_distancekm;
      
   END LOOP; 
   
   CLOSE curs_candidates; 
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Error out if no results
   --------------------------------------------------------------------------
   IF int_counter = 0
   OR out_nhdplusid IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'No results found';
      RETURN;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.distance_index_simple(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.distance_index_simple(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) TO PUBLIC;
--******************************--
----- functions/fetch_grids_by_geometry.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.fetch_grids_by_geometry';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.fetch_grids_by_geometry(
    IN  p_FDR_input                 GEOMETRY
   ,IN  p_FAC_input                 GEOMETRY
   ,IN  p_known_region              VARCHAR DEFAULT NULL
   ,IN  p_FDR_nodata                NUMERIC DEFAULT NULL
   ,IN  p_FAC_nodata                NUMERIC DEFAULT NULL
   ,IN  p_crop                      BOOLEAN DEFAULT TRUE
   ,OUT out_FDR                     RASTER
   ,OUT out_FAC                     RASTER
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
)
STABLE
AS
$BODY$ 
DECLARE
   rec                RECORD;
   int_raster_srid    INTEGER;
   int_original_srid  INTEGER;
   sdo_test_input     GEOMETRY;
   sdo_fdr_selector   GEOMETRY := p_FDR_input;
   sdo_fac_selector   GEOMETRY := p_FAC_input;
   int_fdr_nodata     INTEGER  := p_FDR_nodata;
   int_fac_nodata     INTEGER  := p_FAC_nodata;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  sdo_fdr_selector IS NULL
   AND sdo_fac_selector IS NULL
   THEN
      RAISE EXCEPTION 'input selections cannot both be null';
      
   ELSIF sdo_fdr_selector IS NULL
   THEN
      sdo_test_input := sdo_fac_selector;
      
   ELSE
      sdo_test_input := sdo_fdr_selector;
   
   END IF;
   
   IF  sdo_fdr_selector IS NOT NULL 
   AND ST_GeometryType(sdo_fdr_selector) NOT IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      RAISE EXCEPTION 'fdr geometry selector must be polygon';
      
   END IF;
   
   IF  sdo_fac_selector IS NOT NULL 
   AND ST_GeometryType(sdo_fac_selector) NOT IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      RAISE EXCEPTION 'fac geometry selector must be polygon';
      
   END IF;
   
   IF int_fdr_nodata IS NULL
   THEN
      int_fdr_nodata := 255;
      
   END IF;
   
   IF int_fac_nodata IS NULL
   THEN
      int_fac_nodata := 2147483647;
      
   END IF;
   
   IF int_fdr_nodata < 0
   THEN
      RAISE EXCEPTION 'fdr nodata value must be unsigned 8 bit integer';
   
   END IF;   
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the grid projection 
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := sdo_fdr_selector
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Project initial point
   --------------------------------------------------------------------------
   int_original_srid := ST_Srid(sdo_test_input);

   IF  int_original_srid <> int_raster_srid
   THEN
      IF sdo_fdr_selector IS NOT NULL
      THEN
         sdo_fdr_selector := ST_Transform(sdo_fdr_selector,int_raster_srid);
      
      END IF;
      
      IF sdo_fac_selector IS NOT NULL
      THEN
         sdo_fac_selector := ST_Transform(sdo_fac_selector,int_raster_srid);
      
      END IF;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Fetch the FDR raster if requested
   --------------------------------------------------------------------------
   IF sdo_fdr_selector IS NOT NULL
   THEN
      IF int_raster_srid = 5070
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_5070_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSIF int_raster_srid = 3338
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_3338_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSIF int_raster_srid = 26904
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_26904_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSIF int_raster_srid = 32161
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_32161_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSIF int_raster_srid = 32655
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_32655_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSIF int_raster_srid = 32702
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_32702_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSE
         RAISE EXCEPTION 'err';
         
      END IF;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Fetch the FAC raster if requested
   --------------------------------------------------------------------------
   IF sdo_fac_selector IS NOT NULL
   THEN
      IF int_raster_srid = 5070
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_5070_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector);

      ELSIF int_raster_srid = 3338
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_3338_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector); 

      ELSIF int_raster_srid = 26904
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_26904_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector); 

      ELSIF int_raster_srid = 32161
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_32161_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector);

      ELSIF int_raster_srid = 32655
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_32655_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector);

      ELSIF int_raster_srid = 32702
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_32702_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector);

      ELSE
         RAISE EXCEPTION 'err';
         
      END IF;
   
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.fetch_grids_by_geometry(
    GEOMETRY
   ,GEOMETRY
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.fetch_grids_by_geometry(
    GEOMETRY
   ,GEOMETRY
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
) TO PUBLIC;
--******************************--
----- functions/generic_common_mbr.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.generic_common_mbr(
   IN  p_input  VARCHAR
) RETURNS GEOMETRY 
IMMUTABLE
AS
$BODY$ 
DECLARE
   str_input VARCHAR(4000) := UPPER(p_input);
   
BEGIN
   
   IF str_input IN ('5070','CONUS','US','USA',
   'AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA',
   'IA','ID','IL','IN','KS','KY','LA','MA','MD','ME',
   'MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ',
   'NM','NV','NY','OH','OK','OR','PA','RI','SC','SD',
   'TN','TX','UT','VA','VT','WA','WI','WV','WY')
   THEN
      RETURN ST_PolygonFromText('POLYGON((-128.0 20.2,-64.0 20.2,-64.0 52.0,-128.0 52.0,-128.0 20.2))',4326)::geography;
      
   ELSIF str_input IN ('3338','AK','ALASKA')
   THEN
      RETURN ST_MPolyFromText('MULTIPOLYGON(((-180 48,-128 48,-128 90,-180 90,-180 48)),((168 48,180 48,180 90,168 90,168 48)))',4326)::geography;
      
   ELSIF str_input IN ('26904','HI','HAWAII')
   THEN
      RETURN ST_PolygonFromText('POLYGON((-180.0 10.0,-146.0 10.0,-146.0 35.0,-180.0 35.0,-180.0 10.0))',4326)::geography;
      
   ELSIF str_input IN ('32161','PR','VI','PR/VI','PRVI')
   THEN
      RETURN ST_PolygonFromText('POLYGON((-69.0 16.0,-63.0 16.0,-63.0 20.0,-69.0 20.0,-69.0 16.0))',4326)::geography;
   
   ELSIF str_input IN ('32655','GUMP','GUAM','MP','GU')
   THEN
      RETURN ST_PolyFromText('POLYGON((136.0 8.0,154.0 8.0,154.0 25.0,136.0 25.0,136.0 8.0))',4326)::geography;
         
   ELSIF str_input IN ('32702','SAMOA','AS')
   THEN
      RETURN ST_PolyFromText('POLYGON((-178.0 -20.0, -163.0 -20.0, -163.0 -5.0, -178.0 -5.0, -178.0 -20.0))',4326)::geography;
        
   ELSE
      RAISE EXCEPTION 'unknown generic mbr code';
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.generic_common_mbr(
   VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.generic_common_mbr(
   VARCHAR
) TO PUBLIC;

--******************************--
----- functions/get_flowline.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.get_flowline';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.get_flowline(
    IN  p_direction                    VARCHAR DEFAULT NULL
   ,IN  p_nhdplusid                    BIGINT  DEFAULT NULL
   ,IN  p_permanent_identifier         VARCHAR DEFAULT NULL
   ,IN  p_reachcode                    VARCHAR DEFAULT NULL
   ,IN  p_hydroseq                     BIGINT  DEFAULT NULL
   ,IN  p_measure                      NUMERIC DEFAULT NULL
   ,IN  p_known_region                 VARCHAR DEFAULT NULL
   ,OUT out_flowline                   cipsrv_nhdplus_h.flowline
   ,OUT out_region                     VARCHAR
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                RECORD;
   str_direction      VARCHAR(5) := UPPER(p_direction);
   num_difference     NUMERIC;
   num_end_of_line    NUMERIC := 0.0001;
   sdo_point          GEOMETRY;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF  p_nhdplusid IS NULL
   AND p_permanent_identifier IS NULL
   AND p_reachcode IS NULL
   AND p_hydroseq IS NULL
   THEN
      out_return_code    := -2;
      out_status_message := 'NHDPlusID, Permanent Identifier, Reach Code or Hydrosequence value is required.';
      RETURN;
      
   END IF;
   
   IF str_direction IN ('UT','UM')
   THEN
      str_direction := 'U';
      
   ELSIF str_direction IN ('DD','DM','PP','PPALL')
   THEN
      str_direction := 'D';
      
   END IF;
   
   IF str_direction NOT IN ('D','U')
   THEN
      str_direction := 'D';
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Check when nhdplusid provided
   --------------------------------------------------------------------------
   IF p_nhdplusid IS NOT NULL
   THEN
      IF p_measure IS NULL
      THEN
         SELECT 
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_h.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE
         a.nhdplusid = p_nhdplusid;

         out_flowline.out_lengthkm           := out_flowline.lengthkm;
         out_flowline.out_flowtimeday        := out_flowline.flowtimeday;
         
         IF str_direction = 'D'
         THEN
            out_flowline.out_measure         := out_flowline.tmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm     + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
      
         ELSIF str_direction = 'U'
         THEN  
            out_flowline.out_measure         := out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.fromnode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday;
      
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema 
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_h.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE 
             a.nhdplusid = p_nhdplusid
         AND (
            a.fmeasure = p_measure
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
         );
         
         out_flowline.out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_flowline.fmeasure
            AND out_flowline.hydroseq IS NOT NULL
            AND out_flowline.hydroseq = out_flowline.terminalpa
            THEN
               out_flowline.out_measure := out_flowline.fmeasure + num_end_of_line;
            
            END IF;
            
            num_difference                 := out_flowline.out_measure - out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
            
         ELSIF str_direction = 'U'
         THEN  
            IF p_measure = out_flowline.tmeasure
            AND out_flowline.uphydroseq = 0
            THEN
               out_flowline.out_measure := out_flowline.tmeasure - num_end_of_line;
            
            END IF;
            
            num_difference                   := out_flowline.tmeasure - out_flowline.out_measure;
            out_flowline.out_node            := out_flowline.fromnode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + (( 100 - num_difference ) * out_flowline.lengthkm_ratio);
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + (( 100 - num_difference ) * out_flowline.flowtimeday_ratio);
      
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;

      END IF;
   
   ELSIF p_permanent_identifier IS NOT NULL
   THEN
      IF p_measure IS NULL
      THEN
         SELECT 
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_h.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE
         a.permanent_identifier = p_permanent_identifier;

         out_flowline.out_lengthkm           := out_flowline.lengthkm;
         out_flowline.out_flowtimeday        := out_flowline.flowtimeday;
         
         IF str_direction = 'D'
         THEN
            out_flowline.out_measure         := out_flowline.tmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm     + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
      
         ELSIF str_direction = 'U'
         THEN  
            out_flowline.out_measure         := out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.fromnode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday;
      
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema 
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_h.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE
             a.permanent_identifier = p_permanent_identifier
         AND (
            a.fmeasure = p_measure
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
         );
         
         out_flowline.out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_flowline.fmeasure
            AND out_flowline.hydroseq IS NOT NULL
            AND out_flowline.hydroseq = out_flowline.terminalpa
            THEN
               out_flowline.out_measure := out_flowline.fmeasure + num_end_of_line;
            
            END IF;
            
            num_difference                 := out_flowline.out_measure - out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
            
         ELSIF str_direction = 'U'
         THEN  
            IF p_measure = out_flowline.tmeasure
            AND out_flowline.uphydroseq = 0
            THEN
               out_flowline.out_measure := out_flowline.tmeasure - num_end_of_line;
            
            END IF;
            
            num_difference                   := out_flowline.tmeasure - out_flowline.out_measure;
            out_flowline.out_node            := out_flowline.fromnode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + (( 100 - num_difference ) * out_flowline.lengthkm_ratio);
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + (( 100 - num_difference ) * out_flowline.flowtimeday_ratio);
      
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;

      END IF;
         
   ELSIF p_hydroseq IS NOT NULL
   THEN
      IF p_measure IS NULL
      THEN
         SELECT 
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_h.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE
         b.hydroseq = p_hydroseq;

         out_flowline.out_lengthkm           := out_flowline.lengthkm;
         out_flowline.out_flowtimeday        := out_flowline.flowtimeday;
         
         IF str_direction = 'D'
         THEN
            out_flowline.out_measure         := out_flowline.tmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm     + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
      
         ELSIF str_direction = 'U'
         THEN  
            out_flowline.out_measure         := out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.fromnode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday;
      
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema 
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_h.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE
             b.hydroseq = p_hydroseq
         AND (
            a.fmeasure = p_measure
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
         );
         
         out_flowline.out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_flowline.fmeasure
            AND out_flowline.hydroseq IS NOT NULL
            AND out_flowline.hydroseq = out_flowline.terminalpa
            THEN
               out_flowline.out_measure := out_flowline.fmeasure + num_end_of_line;
            
            END IF;
            
            num_difference                 := out_flowline.out_measure - out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
            
         ELSIF str_direction = 'U'
         THEN  
            IF p_measure = out_flowline.tmeasure
            AND out_flowline.uphydroseq = 0
            THEN
               out_flowline.out_measure := out_flowline.tmeasure - num_end_of_line;
            
            END IF;
            
            num_difference                   := out_flowline.tmeasure - out_flowline.out_measure;
            out_flowline.out_node            := out_flowline.fromnode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + (( 100 - num_difference ) * out_flowline.lengthkm_ratio);
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + (( 100 - num_difference ) * out_flowline.flowtimeday_ratio);
      
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;

      END IF; 

   --------------------------------------------------------------------------
   -- Step 40
   -- Check when reach code provided
   --------------------------------------------------------------------------
   ELSIF p_reachcode IS NOT NULL
   THEN
      IF p_measure IS NULL
      THEN
         SELECT 
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema 
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_h.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE 
             a.reachcode = p_reachcode 
         AND (
               (str_direction = 'D' AND a.tmeasure = 100)
            OR (str_direction = 'U' AND a.fmeasure = 0 )
         );
         
         out_flowline.out_lengthkm           := out_flowline.lengthkm;
         out_flowline.out_flowtimeday        := out_flowline.flowtimeday;
         
         IF str_direction = 'D'
         THEN
            out_flowline.out_measure         := 100;
            out_flowline.out_node            := out_flowline.tonode;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
            
         ELSIF str_direction = 'U'
         THEN
            out_flowline.out_measure         := 0;
            out_flowline.out_node            := out_flowline.fromnode;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday;
         
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_h.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE 
             a.reachcode = p_reachcode 
         AND (
            (p_measure = 0 AND a.fmeasure = 0)
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
         );
         
         out_flowline.out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_flowline.fmeasure
            AND out_flowline.hydroseq IS NOT NULL
            AND out_flowline.hydroseq = out_flowline.terminalpa
            THEN
               out_flowline.out_measure := out_flowline.fmeasure + num_end_of_line;
            
            END IF;
            
            num_difference                   := p_measure - out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
            
         ELSIF str_direction = 'U'
         THEN
            IF p_measure = out_flowline.tmeasure
            AND out_flowline.uphydroseq = 0
            THEN
               out_flowline.out_measure := out_flowline.tmeasure - num_end_of_line;
            
            END IF;
            
            num_difference                   := out_flowline.tmeasure - p_measure;
            out_flowline.out_node            := out_flowline.fromnode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + (( 100 - num_difference ) * out_flowline.lengthkm_ratio);
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + (( 100 - num_difference ) * out_flowline.flowtimeday_ratio);
      
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;
 
      END IF;
    
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Determine grid srid
   --------------------------------------------------------------------------
   IF p_known_region IS NOT NULL
   THEN
      out_region                 := p_known_region::VARCHAR;
      out_flowline.out_grid_srid := p_known_region::INTEGER;
      
   ELSE
      SELECT 
      ST_StartPoint(a.shape)
      INTO
      sdo_point
      FROM 
      cipsrv_nhdplus_h.nhdflowline a
      WHERE
      a.nhdplusid = out_flowline.nhdplusid;
      
      rec := cipsrv_nhdplus_h.determine_grid_srid(
          p_geometry      := sdo_point
         ,p_known_region  := p_known_region
      );
      out_region                 := rec.out_srid::VARCHAR;
      out_return_code            := rec.out_return_code;
      out_status_message         := rec.out_status_message;
      out_flowline.out_grid_srid := rec.out_srid;
 
   END IF;
   
EXCEPTION

   WHEN NO_DATA_FOUND
   THEN
      out_return_code    := -10;
      out_status_message := 'no results found in cipsrv_nhdplus_h nhdflowline';
      RETURN;

   WHEN OTHERS
   THEN
      RAISE;   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.get_flowline(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.get_flowline(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/index_area_artpath.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.index_area_artpath';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.index_area_artpath(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_areasqkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_cat_threshold_perc      NUMERIC
   ,IN  p_evt_threshold_perc      NUMERIC
   ,IN  p_permid_joinkey          UUID
   ,IN  p_permid_geometry         GEOMETRY
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   str_known_region       VARCHAR;
   int_srid               INTEGER;
   geom_input             GEOMETRY;
   num_cat_threshold      NUMERIC;
   num_evt_threshold      NUMERIC;
   num_geometry_areasqkm  NUMERIC;
   permid_geometry        GEOMETRY;

BEGIN

   IF p_cat_threshold_perc IS NULL
   THEN
      num_cat_threshold := 0;
   
   ELSE
      num_cat_threshold := p_cat_threshold_perc / 100;
      
   END IF;
   
   IF p_evt_threshold_perc IS NULL
   THEN
      num_evt_threshold := 0;
   
   ELSE
      num_evt_threshold := p_evt_threshold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
   
   IF p_geometry_areasqkm IS NULL
   THEN
      num_geometry_areasqkm := ROUND(ST_Area(ST_Transform(
          p_geometry
         ,int_srid
      ))::NUMERIC / 1000000,8);
      
   ELSE
      num_geometry_areasqkm := p_geometry_areasqkm;
      
   END IF;
      
   IF str_known_region = '5070'
   THEN
      geom_input      := ST_Transform(p_geometry,5070);
      permid_geometry := ST_Transform(p_permid_geometry,5070);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         ,aa.fcode
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,aaaa.fcode
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_5070_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
          a.fcode = 55800
      OR (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '3338'
   THEN
      geom_input      := ST_Transform(p_geometry,3338);
      permid_geometry := ST_Transform(p_permid_geometry,3338);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         ,aa.fcode
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,aaaa.fcode
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_3338_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
          a.fcode = 55800
      OR (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input      := ST_Transform(p_geometry,26904);
      permid_geometry := ST_Transform(p_permid_geometry,26904);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         ,aa.fcode
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,aaaa.fcode
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_26904_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
          a.fcode = 55800
      OR (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input      := ST_Transform(p_geometry,32161);
      permid_geometry := ST_Transform(p_permid_geometry,32161);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         ,aa.fcode
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,aaaa.fcode
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32161_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
          a.fcode = 55800
      OR (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input      := ST_Transform(p_geometry,32655);
      permid_geometry := ST_Transform(p_permid_geometry,32655);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         ,aa.fcode
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,aaaa.fcode
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32655_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
          a.fcode = 55800
      OR (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input      := ST_Transform(p_geometry,32702);
      permid_geometry := ST_Transform(p_permid_geometry,32702);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         ,aa.fcode
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,aaaa.fcode
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32702_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
          a.fcode = 55800
      OR (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.index_area_artpath(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.index_area_artpath(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) TO PUBLIC;

--******************************--
----- functions/index_area_centroid.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.index_area_centroid';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.index_area_centroid(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_areasqkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_cat_threshold_perc      NUMERIC
   ,IN  p_evt_threshold_perc      NUMERIC   
   ,IN  p_permid_joinkey          UUID
   ,IN  p_permid_geometry         GEOMETRY
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   str_known_region       VARCHAR;
   int_srid               INTEGER;
   geom_input             GEOMETRY;
   num_cat_threshold      NUMERIC;
   num_evt_threshold      NUMERIC;
   num_geometry_areasqkm  NUMERIC;
   permid_geometry        GEOMETRY;

BEGIN

   IF p_cat_threshold_perc IS NULL
   THEN
      num_cat_threshold := 0;
   
   ELSE
      num_cat_threshold := p_cat_threshold_perc / 100;
      
   END IF;
   
   IF p_evt_threshold_perc IS NULL
   THEN
      num_evt_threshold := 0;
   
   ELSE
      num_evt_threshold := p_evt_threshold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry      := p_geometry
      ,p_known_region  := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
   
   IF p_geometry_areasqkm IS NULL
   THEN
      num_geometry_areasqkm := ROUND(ST_Area(ST_Transform(
          p_geometry
         ,int_srid
      ))::NUMERIC / 1000000,8);
      
   ELSE
      num_geometry_areasqkm := p_geometry_areasqkm;
      
   END IF;
      
   IF str_known_region = '5070'
   THEN
      geom_input      := ST_Transform(p_geometry,5070);
      permid_geometry := ST_Transform(p_permid_geometry,5070);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_5070_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '3338'
   THEN
      geom_input      := ST_Transform(p_geometry,3338);
      permid_geometry := ST_Transform(p_permid_geometry,3338);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_3338_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input      := ST_Transform(p_geometry,26904);
      permid_geometry := ST_Transform(p_permid_geometry,26904);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_26904_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input      := ST_Transform(p_geometry,32161);
      permid_geometry := ST_Transform(p_permid_geometry,32161);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32161_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input      := ST_Transform(p_geometry,32655);
      permid_geometry := ST_Transform(p_permid_geometry,32655);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32655_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input      := ST_Transform(p_geometry,32702);
      permid_geometry := ST_Transform(p_permid_geometry,32702);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32702_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.index_area_centroid(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.index_area_centroid(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) TO PUBLIC;

--******************************--
----- functions/index_area_simple.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.index_area_simple';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.index_area_simple(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_areasqkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_cat_threshold_perc      NUMERIC
   ,IN  p_evt_threshold_perc      NUMERIC
   ,IN  p_permid_joinkey          UUID
   ,IN  p_permid_geometry         GEOMETRY
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   str_known_region       VARCHAR;
   int_srid               INTEGER;
   geom_input             GEOMETRY;
   num_cat_threshold      NUMERIC;
   num_evt_threshold      NUMERIC;
   num_geometry_areasqkm  NUMERIC;
   permid_geometry        GEOMETRY;

BEGIN

   IF p_cat_threshold_perc IS NULL
   THEN
      num_cat_threshold := 0;
   
   ELSE
      num_cat_threshold := p_cat_threshold_perc / 100;
      
   END IF;
   
   IF p_evt_threshold_perc IS NULL
   THEN
      num_evt_threshold := 0;
   
   ELSE
      num_evt_threshold := p_evt_threshold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
   
   IF p_geometry_areasqkm IS NULL
   THEN
      num_geometry_areasqkm := ROUND(ST_Area(ST_Transform(
          p_geometry
         ,int_srid
      ))::NUMERIC / 1000000,8);
      
   ELSE
      num_geometry_areasqkm := p_geometry_areasqkm;
      
   END IF;
      
   IF str_known_region = '5070'
   THEN
      geom_input      := ST_Transform(p_geometry,5070);
      permid_geometry := ST_Transform(p_permid_geometry,5070);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_5070_state aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '3338'
   THEN
      geom_input      := ST_Transform(p_geometry,3338);
      permid_geometry := ST_Transform(p_permid_geometry,3338);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )  
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_3338_state aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input      := ST_Transform(p_geometry,26904);
      permid_geometry := ST_Transform(p_permid_geometry,26904);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_26904_state aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input      := ST_Transform(p_geometry,32161);
      permid_geometry := ST_Transform(p_permid_geometry,32161);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32161_state aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input      := ST_Transform(p_geometry,32655);
      permid_geometry := ST_Transform(p_permid_geometry,32655);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )  
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32655_state aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input      := ST_Transform(p_geometry,32702);
      permid_geometry := ST_Transform(p_permid_geometry,32702);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )  
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN num_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32702_state aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.index_area_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.index_area_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) TO PUBLIC;

--******************************--
----- functions/index_line_levelpath.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.index_line_levelpath';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.index_line_levelpath(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_lengthkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_line_threshold_perc     NUMERIC
   ,IN  p_permid_joinkey          UUID
   ,IN  p_permid_geometry         GEOMETRY
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   str_known_region       VARCHAR;
   int_srid               INTEGER;
   geom_input             GEOMETRY;
   geom_part              GEOMETRY;
   num_line_threshold     NUMERIC;
   int_count              INTEGER;
   int_count2             INTEGER;
   num_main_levelpathi    BIGINT;
   ary_main_lp_int_nodes  BIGINT[];
   ary_done_levelpathis   BIGINT[];
   num_fromnode           BIGINT;
   num_tonode             BIGINT;
   num_connector_fromnode BIGINT;
   num_connector_tonode   BIGINT;
   num_min_hydroseq       BIGINT;
   num_max_hydroseq       BIGINT;
   boo_check              BOOLEAN;
   int_debug              INTEGER;
   str_debug              VARCHAR;
   int_geom_count         INTEGER;   
   num_geometry_lengthkm  NUMERIC;
   permid_geometry        GEOMETRY;

BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_line_threshold_perc IS NULL
   THEN
      num_line_threshold := 0;
   
   ELSE
      num_line_threshold := p_line_threshold_perc / 100;
      
   END IF;
   
   str_known_region := p_known_region;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Validate the known region
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
   
   IF num_geometry_lengthkm IS NULL
   THEN
      num_geometry_lengthkm := ROUND(ST_Length(ST_Transform(
          p_geometry
         ,int_srid
      ))::NUMERIC * 0.001,8);
      
   ELSE
      num_geometry_lengthkm := p_geometry_lengthkm;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Loop through any multi-part linestrings
   ----------------------------------------------------------------------------
   int_geom_count := ST_NumGeometries(p_geometry);
   FOR i IN 1 .. int_geom_count
   LOOP
      out_return_code := cipsrv_engine.create_line_temp_tables();
      geom_part := ST_GeometryN(p_geometry,i);
      
   ----------------------------------------------------------------------------
   -- Step 40
   -- Load the temp table
   ----------------------------------------------------------------------------      
      IF str_known_region = '5070'
      THEN
         geom_input      := ST_Transform(geom_part,5070);
         permid_geometry := ST_Transform(p_permid_geometry,5070);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,permid_geometry
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_h.catchment_5070_full aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '3338'
      THEN
         geom_input      := ST_Transform(geom_part,3338);
         permid_geometry := ST_Transform(p_permid_geometry,3338);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,permid_geometry
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_h.catchment_3338_full aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
      
      ELSIF str_known_region = '26904'
      THEN
         geom_input      := ST_Transform(geom_part,26904);
         permid_geometry := ST_Transform(p_permid_geometry,26904);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,permid_geometry
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_h.catchment_26904_full aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '32161'
      THEN
         geom_input      := ST_Transform(geom_part,32161);
         permid_geometry := ST_Transform(p_permid_geometry,32161);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,permid_geometry
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_h.catchment_32161_full aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '32655'
      THEN
         geom_input      := ST_Transform(geom_part,32655);
         permid_geometry := ST_Transform(p_permid_geometry,32655);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,permid_geometry
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_h.catchment_32655_full aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '32702'
      THEN
         geom_input      := ST_Transform(geom_part,32702);
         permid_geometry := ST_Transform(p_permid_geometry,32702);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
            ,fcode
            ,isnavigable
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         ,a.fcode
         ,a.isnavigable
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= num_geometry_lengthkm
             THEN
               1
             WHEN num_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            ,aa.fcode
            ,aa.isnavigable
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               ,aaa.fcode
               ,aaa.isnavigable
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,permid_geometry
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  ,aaaa.fcode
                  ,aaaa.isnavigable
                  FROM
                  cipsrv_nhdplus_h.catchment_32702_full aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
      
      ELSE
         RAISE EXCEPTION 'err %',str_known_region;
         
      END IF;
      
      GET DIAGNOSTICS int_count = ROW_COUNT;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Determine the best fit mainpath right upfront
   -- Collect other levelpathis excluding any that touch interior of main
   ----------------------------------------------------------------------------
      IF int_count > 0
      THEN
      
         SELECT
          a.levelpathi
         ,MAX(a.hydroseq)
         ,MIN(a.hydroseq)
         INTO
          num_main_levelpathi
         ,num_max_hydroseq
         ,num_min_hydroseq
         FROM
         tmp_line a
         WHERE
            a.isnavigable
         OR (num_line_threshold IS NULL OR a.nhdpercentage > num_line_threshold)
         OR a.eventpercentage = 1
         GROUP BY
         a.levelpathi
         ORDER BY
         SUM(a.eventpercentage) DESC
         LIMIT 1;
         
         SELECT ARRAY(
            SELECT 
            a.tonode::BIGINT
            FROM
            cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes a
            WHERE
                a.levelpathi = num_main_levelpathi
            AND a.hydroseq >= num_min_hydroseq
            AND a.hydroseq <  num_max_hydroseq
         ) 
         INTO ary_main_lp_int_nodes;
         
         INSERT INTO tmp_line_levelpathi(
             levelpathi
            ,max_hydroseq
            ,min_hydroseq
            ,totaleventpercentage
            ,totaloverlapmeasure
            ,levelpathilengthkm     
            ,fromnode
            ,tonode
            ,connector_fromnode
            ,connector_tonode
         )
         SELECT
          a.levelpathi
         ,a.max_hydroseq
         ,a.min_hydroseq
         ,a.totaleventpercentage
         ,a.totaloverlapmeasure
         ,b.levelpathilengthkm
         ,c.fromnode
         ,d.tonode
         ,c.connector_fromnode
         ,d.connector_tonode
         FROM (
            SELECT
             aa.levelpathi
            ,MAX(aa.hydroseq)        AS max_hydroseq
            ,MIN(aa.hydroseq)        AS min_hydroseq
            ,SUM(aa.eventpercentage) AS totaleventpercentage
            ,SUM(aa.overlapmeasure)  AS totaloverlapmeasure
            FROM
            tmp_line aa
            WHERE (
                  aa.levelpathi = num_main_levelpathi
               OR NOT (aa.tonode = ANY(ary_main_lp_int_nodes))
            )
            AND ( 
               (num_line_threshold IS NULL OR aa.nhdpercentage > num_line_threshold)
               OR aa.eventpercentage = 1
            )
            GROUP BY
            aa.levelpathi
         ) a
         JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi b
         ON
         a.levelpathi = b.levelpathi
         JOIN
         tmp_line c
         ON
         a.max_hydroseq = c.hydroseq
         JOIN
         tmp_line d
         ON
         a.min_hydroseq = d.hydroseq;
         
         GET DIAGNOSTICS int_count = ROW_COUNT;

         INSERT INTO tmp_cip(
             permid_joinkey
            ,nhdplusid
            ,overlap_measure
         ) 
         SELECT
          p_permid_joinkey
         ,a.nhdplusid
         ,COALESCE(b.overlapmeasure,0)
         FROM
         cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes a
         LEFT JOIN
         tmp_line b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE
             a.levelpathi = num_main_levelpathi
         AND a.hydroseq >= num_min_hydroseq
         AND a.hydroseq <= num_max_hydroseq
         ON CONFLICT DO NOTHING;
         
         GET DIAGNOSTICS int_count2 = ROW_COUNT;
         
         -- This is a bit dodgy but if the input is entirely outside the scope of navigable catchments
         -- e.g. out in the ocean or emulating a coastal flowline, then provide back the spatial intersect
         IF int_count2 = 0
         THEN
            INSERT INTO tmp_cip(
                permid_joinkey
               ,nhdplusid
               ,overlap_measure
            ) 
            SELECT 
             p_permid_joinkey
            ,a.nhdplusid
            ,a.overlapmeasure
            FROM
            tmp_line a
            WHERE
            NOT a.isnavigable
            ON CONFLICT DO NOTHING;
            
         END IF;
 
   ----------------------------------------------------------------------------
   -- Step 70
   -- Loop through the additional levelpathis extending main levelpathi
   ----------------------------------------------------------------------------
         IF int_count > 1
         THEN
            SELECT
             a.levelpathi
            ,a.max_hydroseq
            ,a.min_hydroseq 
            ,a.fromnode
            ,a.tonode
            ,a.connector_fromnode
            ,a.connector_tonode
            INTO
             num_main_levelpathi
            ,num_max_hydroseq
            ,num_min_hydroseq
            ,num_fromnode
            ,num_tonode
            ,num_connector_fromnode
            ,num_connector_tonode
            FROM
            tmp_line_levelpathi a
            WHERE
            a.levelpathi = num_main_levelpathi;
            
            ary_done_levelpathis := '{}';
            
            <<sanity_loop>>
            FOR i IN 1 .. 10
            LOOP
               FOR rec IN (
                  SELECT
                   a.levelpathi
                  ,a.max_hydroseq
                  ,a.min_hydroseq 
                  ,a.fromnode
                  ,a.tonode
                  ,a.connector_fromnode
                  ,a.connector_tonode
                  FROM
                  tmp_line_levelpathi a
                  WHERE
                  a.levelpathi != num_main_levelpathi
                  AND NOT (a.levelpathi = ANY(ary_done_levelpathis))
               ) LOOP
                  boo_check := FALSE;
                  
                  IF num_fromnode IN (rec.fromnode,rec.connector_fromnode)
                  OR num_connector_fromnode IN (rec.fromnode,rec.connector_fromnode)
                  THEN
                     boo_check := TRUE;
                     num_fromnode := rec.tonode;
                     num_connector_fromnode := rec.connector_tonode;
                  
                  ELSIF num_fromnode IN (rec.tonode,rec.connector_tonode)
                  OR num_connector_fromnode IN (rec.tonode,rec.connector_tonode)
                  THEN
                     boo_check := TRUE;
                     num_fromnode := rec.fromnode;
                     num_connector_fromnode := rec.connector_fromnode;
                     
                  ELSIF num_tonode IN (rec.fromnode,rec.connector_fromnode)
                  OR num_connector_tonode IN (rec.fromnode,rec.connector_fromnode)
                  THEN
                     boo_check := TRUE;
                     num_tonode := rec.tonode;
                     num_connector_tonode := rec.connector_tonode;
                  
                  ELSIF num_tonode IN (rec.tonode,rec.connector_tonode)
                  OR num_connector_tonode IN (rec.tonode,rec.connector_tonode)
                  THEN
                     boo_check := TRUE;
                     num_tonode := rec.fromnode;
                     num_connector_tonode := rec.connector_fromnode;   
                     
                  END IF;
                  
                  IF boo_check
                  THEN
                     ary_done_levelpathis := array_append(ary_done_levelpathis,rec.levelpathi);
                     
                     INSERT INTO tmp_cip(
                         permid_joinkey
                        ,nhdplusid
                        ,overlap_measure
                     ) 
                     SELECT
                      p_permid_joinkey
                     ,a.nhdplusid
                     ,COALESCE(b.overlapmeasure,0)
                     FROM
                     cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes a
                     JOIN
                     tmp_line b
                     ON 
                     a.nhdplusid = b.nhdplusid
                     WHERE
                         a.levelpathi = rec.levelpathi
                     AND a.hydroseq >= rec.min_hydroseq
                     AND a.hydroseq <= rec.max_hydroseq
                     ON CONFLICT DO NOTHING;
                  
                  ELSE
                     EXIT sanity_loop;
                     
                  END IF;
                  
               END LOOP;
               
            END LOOP;
            
         END IF;
         
      END IF;

   END LOOP;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.index_line_levelpath(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.index_line_levelpath(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) TO PUBLIC;

--******************************--
----- functions/index_line_simple.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.index_line_simple';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.index_line_simple(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_lengthkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_line_threshold_perc     NUMERIC
   ,IN  p_permid_joinkey          UUID
   ,IN  p_permid_geometry         GEOMETRY
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   str_known_region       VARCHAR;
   int_srid               INTEGER;
   geom_input             GEOMETRY;
   num_line_threshold     NUMERIC;
   int_count              INTEGER;
   num_geometry_lengthkm  NUMERIC;
   permid_geometry        GEOMETRY;

BEGIN

   IF p_line_threshold_perc IS NULL
   THEN
      num_line_threshold := 0;
   
   ELSE
      num_line_threshold := p_line_threshold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;

   IF p_geometry_lengthkm IS NULL
   THEN
      num_geometry_lengthkm := ROUND(ST_Length(ST_Transform(
          p_geometry
         ,int_srid
      ))::NUMERIC * 0.001,8);
      
   ELSE
      num_geometry_lengthkm := p_geometry_lengthkm;
      
   END IF;

   IF str_known_region = '5070'
   THEN
      geom_input      := ST_Transform(p_geometry,5070);
      permid_geometry := ST_Transform(p_permid_geometry,5070);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= num_geometry_lengthkm
          THEN
            1
          WHEN num_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_h.catchment_5070_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
      ON CONFLICT DO NOTHING;

   ELSIF str_known_region = '3338'
   THEN
      geom_input      := ST_Transform(p_geometry,3338);
      permid_geometry := ST_Transform(p_permid_geometry,3338);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= num_geometry_lengthkm
          THEN
            1
          WHEN num_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_h.catchment_3338_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input      := ST_Transform(p_geometry,26904);
      permid_geometry := ST_Transform(p_permid_geometry,26904);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= num_geometry_lengthkm
          THEN
            1
          WHEN num_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_h.catchment_26904_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input      := ST_Transform(p_geometry,32161);
      permid_geometry := ST_Transform(p_permid_geometry,32161);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= num_geometry_lengthkm
          THEN
            1
          WHEN num_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_h.catchment_32161_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input      := ST_Transform(p_geometry,32655);
      permid_geometry := ST_Transform(p_permid_geometry,32655);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= num_geometry_lengthkm
          THEN
            1
          WHEN num_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_h.catchment_32655_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input      := ST_Transform(p_geometry,32702);
      permid_geometry := ST_Transform(p_permid_geometry,32702);
      
      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.catchmentstatecodes
         ,aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= num_geometry_lengthkm
          THEN
            1
          WHEN num_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / num_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.catchmentstatecodes
            ,aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.catchmentstatecodes
               ,aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_h.catchment_32702_full aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         num_line_threshold IS NULL 
      OR a.nhdpercentage >= num_line_threshold
      OR a.overlapmeasure = num_geometry_lengthkm
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;

   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.index_line_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.index_line_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,UUID
   ,GEOMETRY
) TO PUBLIC;

--******************************--
----- functions/index_point_simple.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.index_point_simple';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.index_point_simple(
    IN  p_geometry                GEOMETRY
   ,IN  p_known_region            VARCHAR
   ,IN  p_permid_joinkey          UUID
   ,IN  p_permid_geometry         GEOMETRY
   ,IN  p_return_full_catchment   BOOLEAN DEFAULT TRUE
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   str_known_region       VARCHAR;
   int_srid               INTEGER;
   geom_input             GEOMETRY;
   permid_geometry        GEOMETRY;
   int_splitselector      INTEGER;

BEGIN

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry      := p_geometry
      ,p_known_region  := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;

   IF out_return_code != 0
   THEN
      RETURN;

   END IF;

   str_known_region := int_srid::VARCHAR;
   
   IF p_return_full_catchment IS NULL
   OR p_return_full_catchment
   THEN
      int_splitselector := 2;
      
   ELSE
      int_splitselector := 1;
   
   END IF;

   IF str_known_region = '5070'
   THEN
      geom_input      := ST_Transform(p_geometry,5070);
      permid_geometry := ST_Transform(p_permid_geometry,5070);

      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_5070 a
      WHERE
          a.statesplit IN (0,int_splitselector)
      AND ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;

   ELSIF str_known_region = '3338'
   THEN
      geom_input      := ST_Transform(p_geometry,3338);
      permid_geometry := ST_Transform(p_permid_geometry,3338);

      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_3338 a
      WHERE
          a.statesplit IN (0,int_splitselector)
      AND ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;

   ELSIF str_known_region = '26904'
   THEN
      geom_input      := ST_Transform(p_geometry,26904);
      permid_geometry := ST_Transform(p_permid_geometry,26904);

      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_26904 a
      WHERE
          a.statesplit IN (0,int_splitselector)
      AND ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;

   ELSIF str_known_region = '32161'
   THEN
      geom_input      := ST_Transform(p_geometry,32161);
      permid_geometry := ST_Transform(p_permid_geometry,32161);

      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_32161 a
      WHERE
          a.statesplit IN (0,int_splitselector)
      AND ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;

   ELSIF str_known_region = '32655'
   THEN
      geom_input      := ST_Transform(p_geometry,32655);
      permid_geometry := ST_Transform(p_permid_geometry,32655);

      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_32655 a
      WHERE
          a.statesplit IN (0,int_splitselector)
      AND ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;

   ELSIF str_known_region = '32702'
   THEN
      geom_input      := ST_Transform(p_geometry,32702);
      permid_geometry := ST_Transform(p_permid_geometry,32702);

      INSERT INTO tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.catchmentstatecodes
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_32702 a
      WHERE
          a.statesplit IN (0,int_splitselector)
      AND ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;

   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;

   END IF;

   RETURN;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.index_point_simple(
    GEOMETRY
   ,VARCHAR
   ,UUID
   ,GEOMETRY
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.index_point_simple(
    GEOMETRY
   ,VARCHAR
   ,UUID
   ,GEOMETRY
   ,BOOLEAN
) TO PUBLIC;

--******************************--
----- functions/load_aggregated_catchment.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.load_aggregated_catchment';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.load_aggregated_catchment(
    IN  p_grid_srid               INTEGER
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   num_areasqkm    NUMERIC;
   sdo_output      GEOMETRY;
   
BEGIN

   IF p_grid_srid = 5070
   THEN
      SELECT 
      ST_Union(ST_SnapToGrid(shape_5070,0.05))
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_5070
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;
   
   ELSIF p_grid_srid = 3338
   THEN
      SELECT 
      ST_Union(ST_SnapToGrid(shape_3338,0.05))
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_3338
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;
   
   ELSIF p_grid_srid = 26904
   THEN
      SELECT 
      ST_Union(ST_SnapToGrid(a.shape_26904,0.05))
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_26904
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;
      
   ELSIF p_grid_srid = 32161
   THEN
      SELECT 
      ST_Union(ST_SnapToGrid(a.shape_32161,0.05))
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_32161
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;
      
   ELSIF p_grid_srid = 32655
   THEN
      SELECT 
      ST_Union(ST_SnapToGrid(a.shape_32655,0.05))
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_32655
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;

   ELSIF p_grid_srid = 32702
   THEN
      SELECT 
      ST_Union(ST_SnapToGrid(a.shape_32702,0.05))
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_32702
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;
      
   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;
   
   IF sdo_output IS NULL
   THEN
      out_return_code    := -101;
      out_status_message := 'Catchment aggregation failed';
      RETURN;
      
   END IF;
   
   num_areasqkm := ST_Area(sdo_output) * 0.000001;
   
   sdo_output := ST_Transform(sdo_output,4269);
   
   INSERT INTO tmp_catchments(
       nhdplusid
      ,sourcefc
      ,areasqkm
      ,shape
   ) VALUES (
       -9999999
      ,'AGGR'
      ,num_areasqkm
      ,sdo_output
   );
   
   out_return_code := 0;
   
   RETURN;   
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.load_aggregated_catchment(
    INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.load_aggregated_catchment(
    INTEGER
) TO PUBLIC;
--******************************--
----- functions/load_topo_catchment.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.load_topo_catchment';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.load_topo_catchment(
    IN  p_grid_srid               INTEGER
   ,IN  p_catchment_count         INTEGER DEFAULT NULL
   ,OUT out_total_areasqkm        NUMERIC
   ,OUT out_geometry              GEOMETRY
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   rec               RECORD;
   ary_edges         INTEGER[];
   ary_rings         INTEGER[];
   int_edge_count    INTEGER;
   sdo_ring          GEOMETRY;
   int_sanity        INTEGER;
   ary_polygons      GEOMETRY[];
   ary_holes         GEOMETRY[];
   boo_running       BOOLEAN;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Create temporary edge table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_delineation_edges')
   THEN
      TRUNCATE TABLE tmp_delineation_edges;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_delineation_edges(
          edge_id        INTEGER 
         ,interior_side  VARCHAR(1)
         ,start_node_id  INTEGER
         ,end_node_id    INTEGER
         ,shape          GEOMETRY 
         ,touch_count    INTEGER
      );

      CREATE UNIQUE INDEX tmp_delineation_edges_pk
      ON tmp_delineation_edges(edge_id);
      
      CREATE INDEX tmp_delineation_edges_01i
      ON tmp_delineation_edges(start_node_id);
            
      CREATE INDEX tmp_delineation_edges_02i
      ON tmp_delineation_edges(end_node_id);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create temporary ring table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_delineation_rings')
   THEN
      TRUNCATE TABLE tmp_delineation_rings;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_delineation_rings(
          ring_id        INTEGER
         ,ring_type      VARCHAR(1)
         ,shape          GEOMETRY
      );

      CREATE UNIQUE INDEX tmp_delineation_rings_pk
      ON tmp_delineation_rings(ring_id);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Create temporary ring table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_delineation_faces')
   THEN
      TRUNCATE TABLE tmp_delineation_faces;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_delineation_faces(
          face_id        INTEGER
      );

      CREATE UNIQUE INDEX tmp_delineation_faces_pk
      ON tmp_delineation_faces(face_id);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Load the edges into temporary table
   ----------------------------------------------------------------------------   
   IF p_grid_srid = 5070
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_5070.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_5070_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;

      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_5070.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_5070.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);

   ELSIF p_grid_srid = 3338
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_3338.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_3338_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;

      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_3338.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_3338.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);

   ELSIF p_grid_srid = 26904
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_3338.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_26904_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;
      
      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_26904.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_26904.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);
      
   ELSIF p_grid_srid = 32161
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32161.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_32161_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;
      
      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_32161.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32161.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);
   
   ELSIF p_grid_srid = 32655
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32655.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_32655_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;
      
      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_32655.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32655.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);
   
   ELSIF p_grid_srid = 32702
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32702.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_32702_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;
   
      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_32702.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32702.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);
   
   ELSE
      RAISE EXCEPTION 'err';

   END IF;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Pull out any single edge rings
   ----------------------------------------------------------------------------
   WITH insertedges AS (
      INSERT INTO tmp_delineation_rings(
          ring_id
         ,ring_type
         ,shape
      )
      SELECT
       a.edge_id
      ,CASE
       WHEN   (ST_IsPolygonCCW(ST_MakePolygon(a.shape)) AND a.interior_side = 'L')
       OR (NOT ST_IsPolygonCCW(ST_MakePolygon(a.shape)) AND a.interior_side = 'R')
       THEN
         'E'
       ELSE
         'I'
       END AS ring_type
      ,a.shape
      FROM
      tmp_delineation_edges a
      WHERE
      a.start_node_id = a.end_node_id
      RETURNING ring_id
   )
   SELECT
   array_agg(ring_id)
   INTO ary_edges
   FROM
   insertedges;
   
   DELETE FROM tmp_delineation_edges
   WHERE edge_id = ANY(ary_edges);
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Recursively pull out rings
   ----------------------------------------------------------------------------
   SELECT 
   COUNT(*)
   INTO int_edge_count
   FROM
   tmp_delineation_edges a; 
   
   int_sanity := 1;
   boo_running := TRUE;
   WHILE boo_running
   LOOP
      boo_running := cipsrv_engine.edges2rings();
      
      int_sanity := int_sanity + 1;
      
      IF int_sanity > 1000
      THEN
         RAISE EXCEPTION 'sanity check';
         
      END IF;
      
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Organize the polygons outer and inner rings
   ----------------------------------------------------------------------------
   FOR rec IN 
      SELECT
       a.ring_id 
      ,a.shape
      FROM
      tmp_delineation_rings a
      WHERE
      a.ring_type = 'E'
      ORDER BY
      ST_Area(ST_MakePolygon(a.shape)) ASC
   LOOP
      ary_holes := NULL;
      
      SELECT
       array_agg(b.ring_id)
      ,array_agg(b.shape)
      INTO 
       ary_rings
      ,ary_holes
      FROM
      tmp_delineation_rings b
      WHERE
          b.ring_type = 'I'
      AND ST_Intersects(
          ST_MakePolygon(rec.shape)
         ,b.shape
      );
   
      IF ary_rings IS NULL
      OR array_length(ary_rings,1) = 0
      THEN
         sdo_ring := ST_MakePolygon(rec.shape);
         
      ELSE
         sdo_ring := ST_MakePolygon(rec.shape,ary_holes);
         
         DELETE FROM tmp_delineation_rings
         WHERE ring_id = ANY(ary_rings);
         
      END IF;
   
      ary_polygons := array_append(ary_polygons,sdo_ring);
   
   END LOOP;
      
   ----------------------------------------------------------------------------
   -- Step 80
   -- Generate the final results
   ----------------------------------------------------------------------------
   IF array_length(ary_polygons,1) = 1
   THEN
      out_geometry := ST_ForcePolygonCCW(ary_polygons[1]);
      
   ELSE
      out_geometry := ST_ForcePolygonCCW(ST_Collect(ary_polygons));
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 90
   -- Calculate the area
   ----------------------------------------------------------------------------
   out_total_areasqkm := ST_Area(ST_Transform(out_geometry,4326)::GEOGRAPHY) * 0.0000010;

   ----------------------------------------------------------------------------
   -- Step 100
   -- Insert results as nad83
   ----------------------------------------------------------------------------
   INSERT INTO tmp_catchments(
       nhdplusid
      ,sourcefc
      ,gridcode
      ,areasqkm
      ,shape
   ) VALUES (
       -9999999
      ,'AGGR'
      ,-9999
      ,out_total_areasqkm
      ,ST_Transform(out_geometry,4269)
   );
   
   out_return_code := 0;
   
   RETURN; 
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.load_topo_catchment(
    INTEGER
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.load_topo_catchment(
    INTEGER
   ,INTEGER
) TO PUBLIC;

--******************************--
----- functions/measure_areasqkm.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.measure_areasqkm';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.measure_areasqkm(
    IN  p_geometry          GEOMETRY
   ,IN  p_known_region      VARCHAR DEFAULT NULL
) RETURNS NUMERIC
STABLE
AS $BODY$ 
DECLARE
   rec                RECORD;
   int_srid           INTEGER;
   int_return_code    INTEGER;
   str_status_message VARCHAR;
   sdo_geometry       GEOMETRY;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_geometry IS NULL
   THEN
      RAISE EXCEPTION 'input geometry required';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine the region from geometry if known region value not provided
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   int_return_code    := rec.out_return_code;
   str_status_message := rec.out_status_message;
  
   ----------------------------------------------------------------------------
   -- Step 30
   -- Project as needed
   ----------------------------------------------------------------------------
   sdo_geometry := ST_Transform(
       p_geometry
      ,int_srid
   );
   
   IF ST_GeometryType(sdo_geometry) = 'ST_GeometryCollection'
   THEN
      sdo_geometry := ST_CollectionExtract(sdo_geometry,3);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return results in km
   ----------------------------------------------------------------------------
   IF sdo_geometry IS NULL OR ST_IsEmpty(sdo_geometry)
   THEN
      RETURN 0;
      
   ELSIF ST_GeometryType(sdo_geometry) IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      RETURN ROUND(ST_Area(sdo_geometry)::NUMERIC / 1000000,8);
      
   ELSE
      RAISE EXCEPTION 'measure areasqkm requires polygon geometry - %',ST_GeometryType(sdo_geometry);
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.measure_areasqkm(
    GEOMETRY
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.measure_areasqkm(
    GEOMETRY
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/measure_lengthkm.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.measure_lengthkm';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.measure_lengthkm(
    IN  p_geometry          GEOMETRY
   ,IN  p_known_region      VARCHAR DEFAULT NULL
) RETURNS NUMERIC
STABLE
AS $BODY$ 
DECLARE
   rec                RECORD;
   int_srid           INTEGER;
   int_return_code    INTEGER;
   str_status_message VARCHAR;
   sdo_geometry       GEOMETRY;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_geometry IS NULL
   THEN
      RAISE EXCEPTION 'input geometry required';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine the region from geometry if known region value not provided
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   int_return_code    := rec.out_return_code;
   str_status_message := rec.out_status_message;
  
   ----------------------------------------------------------------------------
   -- Step 30
   -- Project as needed
   ----------------------------------------------------------------------------
   sdo_geometry := ST_Transform(
       p_geometry
      ,int_srid
   );
   
   IF ST_GeometryType(sdo_geometry) = 'ST_GeometryCollection'
   THEN
      sdo_geometry := ST_CollectionExtract(sdo_geometry,2);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return results in km
   ----------------------------------------------------------------------------
   IF sdo_geometry IS NULL OR ST_IsEmpty(sdo_geometry)
   THEN
      RETURN 0;
      
   ELSIF ST_GeometryType(sdo_geometry) IN ('ST_LineString','ST_MultiLineString')
   THEN
      RETURN ROUND(ST_Length(sdo_geometry)::NUMERIC * 0.001,8);
      
   ELSE
      RAISE EXCEPTION 'measure lengthkm requires linear geometry - %',ST_GeometryType(sdo_geometry);
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.measure_lengthkm(
    GEOMETRY
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.measure_lengthkm(
    GEOMETRY
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/nav_dd.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_dd(
    IN  obj_start_flowline       cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distancekm   NUMERIC
   ,IN  num_maximum_flowtimeday  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   int_collect            INTEGER;
   int_count              INTEGER;
   int_min_hydrosequence  BIGINT;
   int_min_levelpathid    BIGINT;
   num_pathlength_buffer  NUMERIC := 100;
   num_pathtime_buffer    NUMERIC := 10;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Run DM first to establish mainline
   ----------------------------------------------------------------------------
   WITH RECURSIVE dm(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,base_pathlength
      ,base_pathtime
      ,nav_order
      ,selected
   )
   AS (
      SELECT
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,obj_start_flowline.fmeasure
      ,obj_start_flowline.out_measure
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,obj_start_flowline.pathlengthkm    + obj_start_flowline.out_lengthkm
      ,obj_start_flowline.pathflowtimeday + obj_start_flowline.out_flowtimeday
      ,0 AS nav_order
      ,TRUE
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm  -- segment lengthkm
      ,mq.totma
      ,dm.base_pathlength - mq.pathlength
      ,dm.base_pathtime   - mq.pathtimema
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,dm.base_pathlength -- base pathlength
      ,dm.base_pathtime
      ,dm.nav_order + 10000
      ,CASE 
       WHEN (
         num_maximum_distancekm IS NULL
         AND
         num_maximum_flowtimeday IS NULL
       ) 
       OR dm.network_distancekm <= num_maximum_distancekm
       OR (
         mq.totma IS NOT NULL
         AND
         dm.network_flowtimeday <= num_maximum_flowtimeday
       )
       THEN
         TRUE
       ELSE
         FALSE
       END AS selected
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      dm
      WHERE
          mq.hydroseq   = dm.dnhydroseq
      AND mq.terminalpa = dm.terminalpa
      AND (
            num_maximum_distancekm IS NULL
         OR dm.network_distancekm <= num_maximum_distancekm + num_pathlength_buffer
      )
      AND (
            num_maximum_flowtimeday IS NULL
         OR ( 
            mq.totma IS NOT NULL
            AND
            dm.network_flowtimeday <= num_maximum_flowtimeday + num_pathtime_buffer
         )
      )
   )
   INSERT INTO tmp_navigation_working30(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,nav_order
      ,selected
   )
   SELECT
    a.nhdplusid
   ,a.hydroseq
   ,a.fmeasure
   ,a.tmeasure
   ,a.lengthkm
   ,a.flowtimeday
   ,a.network_distancekm
   ,a.network_flowtimeday
   ,a.levelpathi
   ,a.terminalpa
   ,a.uphydroseq
   ,a.dnhydroseq
   ,a.nav_order
   ,a.selected
   FROM
   dm a; 
   
   GET DIAGNOSTICS int_count = ROW_COUNT;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Tag the nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,b.coastal_connection
      ,b.network_end
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
          a.selected = TRUE   
      AND a.navtermination_flag IS NULL
   )
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN a.nav_order = (SELECT MAX(b.nav_order) FROM tmp_navigation_working30 b LIMIT 1)
   THEN
      CASE
      WHEN cte.coastal_connection
      THEN
         3
      WHEN cte.network_end
      THEN
         5
      ELSE
         1
      END
   ELSE
      0
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Extract the divergences off the mainline
   ----------------------------------------------------------------------------
   LOOP
      FOR rec IN 
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,a.totma               AS flowtimeday
         ,b.network_distancekm  AS base_pathlength
         ,b.network_flowtimeday AS base_pathtime
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,b.nav_order + 1       AS nav_order
         FROM 
         cipsrv_nhdplus_h.nhdplusflowlinevaa_nav a
         JOIN 
         tmp_navigation_working30 b
         ON
             a.ary_upstream_hydroseq @> ARRAY[b.hydroseq]
         AND a.hydroseq <> b.dnhydroseq
         WHERE
             b.selected IS TRUE
         AND NOT EXISTS (
            SELECT
            1
            FROM
            tmp_navigation_working30 cc
            WHERE
            cc.hydroseq = a.hydroseq
         )
         ORDER BY
         a.hydroseq DESC
      
      LOOP
         WITH RECURSIVE dm(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,base_pathlength
            ,base_pathtime
            ,nav_order
            ,selected
         )
         AS (
            SELECT
             rec.nhdplusid
            ,rec.hydroseq
            ,rec.fmeasure
            ,rec.tmeasure
            ,rec.lengthkm
            ,rec.flowtimeday
            ,rec.base_pathlength  + rec.lengthkm
            ,rec.base_pathtime    + rec.flowtimeday
            ,rec.levelpathi
            ,rec.terminalpa
            ,rec.uphydroseq
            ,rec.dnhydroseq
            ,rec.base_pathlength 
            ,rec.base_pathtime
            ,rec.nav_order
            ,TRUE
            UNION
            SELECT
             mq.nhdplusid
            ,mq.hydroseq
            ,mq.fmeasure
            ,mq.tmeasure
            ,mq.lengthkm
            ,mq.totma
            ,dm.network_distancekm  + mq.lengthkm
            ,dm.network_flowtimeday + mq.totma
            ,mq.levelpathi
            ,mq.terminalpa
            ,mq.uphydroseq
            ,mq.dnhydroseq
            ,dm.base_pathlength
            ,dm.base_pathtime
            ,dm.nav_order + 1
            ,TRUE AS selected
            FROM
            cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
            CROSS JOIN
            dm
            WHERE
                mq.hydroseq   = dm.dnhydroseq
            AND mq.terminalpa = dm.terminalpa
            AND (
                  num_maximum_distancekm IS NULL
               OR dm.network_distancekm <= num_maximum_distancekm
            )
            AND (
                  num_maximum_flowtimeday IS NULL
               OR ( 
                  mq.totma IS NOT NULL
                  AND
                  dm.network_flowtimeday <= num_maximum_flowtimeday
               )
            )
            AND NOT EXISTS (
               SELECT
               1
               FROM
               tmp_navigation_working30 cc
               WHERE
               cc.hydroseq = mq.hydroseq
            )
         )
         INSERT INTO tmp_navigation_working30(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,nav_order
            ,selected
         )
         SELECT
          a.nhdplusid
         ,a.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,a.flowtimeday
         ,a.network_distancekm
         ,a.network_flowtimeday
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.nav_order
         ,a.selected
         FROM
         dm a
         ON CONFLICT DO NOTHING;         
         
         GET DIAGNOSTICS int_collect = ROW_COUNT;
         int_count := int_count + int_collect;

      END LOOP;
   
      EXIT WHEN NOT FOUND;
   
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Tag the downstream nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,b.ary_downstream_hydroseq
      ,b.coastal_connection
      ,b.network_end
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
          a.selected = TRUE   
      AND a.navtermination_flag IS NULL
   )
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN EXISTS ( SELECT 1 FROM tmp_navigation_working30 d WHERE d.hydroseq = ANY(cte.ary_downstream_hydroseq) )
   THEN
      0
   ELSE
      CASE
      WHEN cte.coastal_connection
      THEN
         3
      WHEN cte.network_end
      THEN
         5
      ELSE
         1
      END
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Return total count of results
   ----------------------------------------------------------------------------
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_dd(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC   
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_dd(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
)  TO PUBLIC;

--******************************--
----- functions/nav_dm.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_dm(
    IN  obj_start_flowline       cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distancekm   NUMERIC
   ,IN  num_maximum_flowtimeday  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   int_count INTEGER;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Return total count of results
   ----------------------------------------------------------------------------
   WITH RECURSIVE dm(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,base_pathlength
      ,base_pathtime
      ,nav_order
   )
   AS (
      SELECT
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,obj_start_flowline.fmeasure
      ,obj_start_flowline.out_measure
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,obj_start_flowline.pathlengthkm    + obj_start_flowline.out_lengthkm
      ,obj_start_flowline.pathflowtimeday + obj_start_flowline.out_flowtimeday
      ,0 AS nav_order
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm  -- segment lengthkm
      ,mq.totma
      ,dm.base_pathlength - mq.pathlength
      ,dm.base_pathtime   - mq.pathtimema
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,dm.base_pathlength -- base pathlength
      ,dm.base_pathtime
      ,dm.nav_order + 1 
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      dm
      WHERE
          mq.hydroseq   = dm.dnhydroseq
      AND mq.terminalpa = dm.terminalpa
      AND (
            num_maximum_distancekm IS NULL
         OR dm.network_distancekm <= num_maximum_distancekm
      )
      AND (
            num_maximum_flowtimeday IS NULL
         OR ( 
            mq.totma IS NOT NULL
            AND
            dm.network_flowtimeday <= num_maximum_flowtimeday
         )
      )
   )
   INSERT INTO tmp_navigation_working30(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,nav_order
      ,selected
   )
   SELECT
    a.nhdplusid
   ,a.hydroseq
   ,a.fmeasure
   ,a.tmeasure
   ,a.lengthkm
   ,a.flowtimeday
   ,a.network_distancekm
   ,a.network_flowtimeday
   ,a.levelpathi
   ,a.terminalpa
   ,a.uphydroseq
   ,a.dnhydroseq
   ,a.nav_order
   ,TRUE
   FROM
   dm a;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Tag the nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,b.coastal_connection
      ,b.network_end
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
          a.selected = TRUE   
      AND a.navtermination_flag IS NULL
   )
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN a.nav_order = (SELECT MAX(b.nav_order) FROM tmp_navigation_working30 b LIMIT 1)
   THEN
      CASE
      WHEN cte.coastal_connection
      THEN
         3
      WHEN cte.network_end
      THEN
         5
      ELSE
         1
      END
   ELSE
      0
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return total count of results
   ----------------------------------------------------------------------------
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_dm(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC   
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_dm(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
)  TO PUBLIC;

--******************************--
----- functions/nav_pp.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_pp(
    IN  obj_start_flowline        cipsrv_nhdplus_h.flowline
   ,IN  obj_stop_flowline         cipsrv_nhdplus_h.flowline
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   rec         RECORD;
   int_count   INTEGER;
   int_check   INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_network_working30 temp table
   ----------------------------------------------------------------------------
   IF cipsrv_nhdplus_h.temp_table_exists('tmp_network_working30')
   THEN
      TRUNCATE TABLE tmp_network_working30;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_network_working30(
          nhdplusid                   BIGINT
         ,hydroseq                    BIGINT
         ,fmeasure                    NUMERIC
         ,tmeasure                    NUMERIC
         ,lengthkm                    NUMERIC
         ,flowtimeday                 NUMERIC
         ,network_distancekm          NUMERIC
         ,network_flowtimeday         NUMERIC
         ,levelpathi                  BIGINT
         ,terminalpa                  BIGINT
         ,uphydroseq                  BIGINT
         ,dnhydroseq                  BIGINT
         ,fromnode                    BIGINT
         ,tonode                      BIGINT
         ,cost                        FLOAT8
      );

      CREATE INDEX tmp_network_working30_01i
      ON tmp_network_working30(nhdplusid);
      
      CREATE INDEX tmp_network_working30_02i
      ON tmp_network_working30(hydroseq);
      
      CREATE INDEX tmp_network_working30_03i
      ON tmp_network_working30(fromnode);
      
      CREATE INDEX tmp_network_working30_04i
      ON tmp_network_working30(tonode);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Run downstream mainline as most probable solution
   ----------------------------------------------------------------------------
   WITH RECURSIVE dm(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,base_pathlength
      ,base_pathtime
      ,fromnode
      ,tonode
      ,cost
   )
   AS (
      SELECT
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,obj_start_flowline.fmeasure
      ,obj_start_flowline.out_measure
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,obj_start_flowline.pathlengthkm    + obj_start_flowline.out_lengthkm
      ,obj_start_flowline.pathflowtimeday + obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.fromnode
      ,obj_start_flowline.tonode
      ,1::FLOAT8
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm  -- segment lengthkm
      ,mq.totma
      ,dm.base_pathlength - mq.pathlength
      ,dm.base_pathtime   - mq.pathtimema 
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,dm.base_pathlength -- base pathlength
      ,dm.base_pathtime
      ,mq.fromnode
      ,mq.tonode
      ,1::FLOAT8
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      dm
      WHERE
          mq.hydroseq   =  dm.dnhydroseq
      AND mq.terminalpa =  dm.terminalpa
      AND mq.hydroseq   >= obj_stop_flowline.hydroseq
   )
   INSERT INTO tmp_network_working30(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,fromnode
      ,tonode
      ,cost
   )
   SELECT
    a.nhdplusid
   ,a.hydroseq
   ,a.fmeasure
   ,a.tmeasure
   ,a.lengthkm
   ,a.flowtimeday
   ,a.network_distancekm
   ,a.network_flowtimeday
   ,a.levelpathi
   ,a.terminalpa
   ,a.uphydroseq
   ,a.dnhydroseq
   ,a.fromnode
   ,a.tonode
   ,a.cost
   FROM
   dm a;

   SELECT
   COUNT(*)
   INTO int_count
   FROM 
   tmp_network_working30 a
   WHERE
   a.nhdplusid = obj_stop_flowline.nhdplusid;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- If found then dump into working30 and exit
   ----------------------------------------------------------------------------
   IF int_count > 0
   THEN   
      INSERT INTO tmp_navigation_working30(
          nhdplusid
         ,hydroseq
         ,fmeasure
         ,tmeasure
         ,lengthkm
         ,flowtimeday
         ,network_distancekm
         ,network_flowtimeday
         ,levelpathi
         ,terminalpa
         ,uphydroseq
         ,dnhydroseq
         ,selected
      )
      SELECT
       b.nhdplusid
      ,b.hydroseq
      ,b.fmeasure
      ,b.tmeasure
      ,b.lengthkm
      ,b.flowtimeday
      ,b.network_distancekm
      ,b.network_flowtimeday
      ,b.levelpathi
      ,b.terminalpa
      ,b.uphydroseq
      ,b.dnhydroseq
      ,TRUE
      FROM
      tmp_network_working30 b;
      
      UPDATE tmp_navigation_working30 a
      SET
       fmeasure            = obj_stop_flowline.out_measure
      ,tmeasure            = obj_stop_flowline.tmeasure
      ,lengthkm            = obj_stop_flowline.out_lengthkm
      ,flowtimeday         = obj_stop_flowline.out_flowtimeday
      ,network_distancekm  = a.network_distancekm  + obj_stop_flowline.out_lengthkm    - a.lengthkm
      ,network_flowtimeday = a.network_flowtimeday + obj_stop_flowline.out_flowtimeday - a.flowtimeday
      WHERE
      a.nhdplusid = obj_stop_flowline.nhdplusid;
      
   -------------------------------------------------------------------------
   -- Step 40
   -- Otherwise run divergences downstream
   -------------------------------------------------------------------------
   ELSE
   
      LOOP
         FOR rec IN 
            SELECT 
             a.nhdplusid
            ,a.hydroseq
            ,a.fmeasure
            ,a.tmeasure
            ,a.lengthkm
            ,a.totma               AS flowtimeday
            ,b.network_distancekm  AS base_pathlength
            ,b.network_flowtimeday AS base_pathtime
            ,a.levelpathi
            ,a.terminalpa
            ,a.uphydroseq
            ,a.dnhydroseq
            ,a.fromnode
            ,a.tonode
            FROM 
            cipsrv_nhdplus_h.nhdplusflowlinevaa_nav a
            JOIN 
            tmp_network_working30 b
            ON
                a.ary_upstream_hydroseq @> ARRAY[b.hydroseq]
            AND a.hydroseq <> b.dnhydroseq
            WHERE
            NOT EXISTS (
               SELECT
               1
               FROM
               tmp_network_working30 cc
               WHERE
               cc.hydroseq = a.hydroseq
            )
            ORDER BY
            a.hydroseq DESC
         LOOP
         
            WITH RECURSIVE dm(
                nhdplusid
               ,hydroseq
               ,fmeasure
               ,tmeasure
               ,lengthkm
               ,flowtimeday
               ,network_distancekm
               ,network_flowtimeday
               ,levelpathi
               ,terminalpa
               ,uphydroseq
               ,dnhydroseq
               ,base_pathlength
               ,base_pathtime
               ,fromnode
               ,tonode
               ,cost
            )
            AS (
               SELECT
                rec.nhdplusid
               ,rec.hydroseq
               ,rec.fmeasure
               ,rec.tmeasure
               ,rec.lengthkm
               ,rec.flowtimeday
               ,rec.base_pathlength  + rec.lengthkm
               ,rec.base_pathtime    + rec.flowtimeday
               ,rec.levelpathi
               ,rec.terminalpa
               ,rec.uphydroseq
               ,rec.dnhydroseq
               ,rec.base_pathlength 
               ,rec.base_pathtime
               ,rec.fromnode
               ,rec.tonode
               ,100::FLOAT8 AS cost
               UNION
               SELECT
                mq.nhdplusid
               ,mq.hydroseq
               ,mq.fmeasure
               ,mq.tmeasure
               ,mq.lengthkm
               ,mq.totma
               ,dm.network_distancekm  + mq.lengthkm
               ,dm.network_flowtimeday + mq.totma
               ,mq.levelpathi
               ,mq.terminalpa
               ,mq.uphydroseq
               ,mq.dnhydroseq
               ,dm.base_pathlength
               ,dm.base_pathtime
               ,mq.fromnode
               ,mq.tonode
               ,100::FLOAT8 AS cost
               FROM
               cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
               CROSS JOIN
               dm
               WHERE
                   mq.hydroseq   = dm.dnhydroseq
               AND mq.terminalpa = dm.terminalpa
               AND mq.hydroseq   >= obj_stop_flowline.hydroseq
               AND NOT EXISTS (
                  SELECT
                  1
                  FROM
                  tmp_network_working30 cc
                  WHERE
                  cc.hydroseq = mq.hydroseq
               )
            )
            INSERT INTO tmp_network_working30(
                nhdplusid
               ,hydroseq
               ,fmeasure
               ,tmeasure
               ,lengthkm
               ,flowtimeday
               ,network_distancekm
               ,network_flowtimeday
               ,levelpathi 
               ,terminalpa
               ,uphydroseq 
               ,dnhydroseq
               ,fromnode
               ,tonode
               ,cost
            )
            SELECT
             a.nhdplusid
            ,a.hydroseq
            ,a.fmeasure
            ,a.tmeasure
            ,a.lengthkm
            ,a.flowtimeday
            ,a.network_distancekm
            ,a.network_flowtimeday
            ,a.levelpathi
            ,a.terminalpa
            ,a.uphydroseq
            ,a.dnhydroseq
            ,a.fromnode
            ,a.tonode
            ,a.cost
            FROM
            dm a
            ON CONFLICT DO NOTHING;         

         END LOOP;
      
         EXIT WHEN NOT FOUND;
      
      END LOOP;
      
      SELECT
      COUNT(*)
      INTO int_count
      FROM 
      tmp_network_working30 a
      WHERE
      a.nhdplusid = obj_stop_flowline.nhdplusid;

      IF int_count > 0
      THEN
         -- Remove duplicate flowlines keeping the cheapest cost
         DELETE FROM tmp_network_working30 a
         WHERE a.ctid IN (
            SELECT b.ctid FROM (
               SELECT
                bb.ctid
               ,ROW_NUMBER() OVER (PARTITION BY bb.nhdplusid ORDER BY bb.cost ASC) AS rnum
               FROM
               tmp_network_working30 bb
            ) b
            WHERE
            b.rnum > 1
         );
         
         -- Determine dikstra shortest route from start to stop
         WITH dijk AS(
            SELECT
             a.seq
            ,a.node
            ,a.edge
            ,a.cost
            FROM
            pgr_dijkstra(
                'SELECT nhdplusid AS id,fromnode AS source,tonode AS target,cost,-1::FLOAT8 AS reverse_cost FROM tmp_network_working30'
               ,obj_start_flowline.out_node
               ,obj_stop_flowline.out_node
               ,TRUE
            ) a
         )
         INSERT INTO tmp_navigation_working30(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,selected
         )
         SELECT
          b.nhdplusid
         ,b.hydroseq
         ,b.fmeasure
         ,b.tmeasure
         ,b.lengthkm
         ,b.flowtimeday
         ,b.network_distancekm
         ,b.network_flowtimeday
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,TRUE
         FROM
         dijk a
         JOIN
         tmp_network_working30 b
         ON
         a.edge = b.nhdplusid;
         
         -- Replace the start and stop segments
         INSERT INTO tmp_navigation_working30(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,nav_order
            ,selected
         ) VALUES (
             obj_start_flowline.nhdplusid
            ,obj_start_flowline.hydroseq
            ,obj_start_flowline.fmeasure
            ,obj_start_flowline.out_measure
            ,obj_start_flowline.out_lengthkm
            ,obj_start_flowline.out_flowtimeday
            ,obj_start_flowline.out_lengthkm
            ,obj_start_flowline.out_flowtimeday
            ,obj_start_flowline.levelpathi
            ,obj_start_flowline.terminalpa
            ,obj_start_flowline.uphydroseq
            ,obj_start_flowline.dnhydroseq
            ,0
            ,TRUE
         );
         
         INSERT INTO tmp_navigation_working30(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,nav_order
            ,selected
         ) VALUES (
             obj_stop_flowline.nhdplusid
            ,obj_stop_flowline.hydroseq
            ,obj_stop_flowline.out_measure
            ,obj_stop_flowline.tmeasure
            ,obj_stop_flowline.out_lengthkm
            ,obj_stop_flowline.out_flowtimeday
            ,obj_start_flowline.out_pathlengthkm    - obj_stop_flowline.out_pathlengthkm
            ,obj_start_flowline.out_pathflowtimeday - obj_stop_flowline.out_pathflowtimeday
            ,obj_stop_flowline.levelpathi
            ,obj_stop_flowline.terminalpa
            ,obj_stop_flowline.uphydroseq
            ,obj_stop_flowline.dnhydroseq
            ,99999999
            ,TRUE            
         );
         
      END IF;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Tag the downstream nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,a.fmeasure
      ,a.tmeasure
      ,b.ary_downstream_hydroseq
      ,b.coastal_connection
      ,b.network_end
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
          a.selected = TRUE   
      AND a.navtermination_flag IS NULL
   )
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN EXISTS ( SELECT 1 FROM tmp_navigation_working30 d WHERE d.hydroseq = ANY(cte.ary_downstream_hydroseq) )
   THEN
      0
   ELSE
      CASE
      WHEN cte.hydroseq = obj_stop_flowline.hydroseq
      THEN
         CASE
         WHEN obj_stop_flowline.fmeasure = cte.fmeasure 
         AND  obj_stop_flowline.tmeasure = cte.tmeasure 
         THEN
            CASE
            WHEN cte.coastal_connection
            THEN
               3
            WHEN cte.network_end
            THEN
               5
            ELSE
               1
            END
         ELSE
            2
         END
      END
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Return total count of results
   ----------------------------------------------------------------------------
   SELECT 
   COUNT(*) 
   INTO int_count 
   FROM 
   tmp_navigation_working30 a
   WHERE 
   a.selected IS TRUE;
   
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_pp(
    cipsrv_nhdplus_h.flowline
   ,cipsrv_nhdplus_h.flowline  
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_pp(
    cipsrv_nhdplus_h.flowline
   ,cipsrv_nhdplus_h.flowline
)  TO PUBLIC;

--******************************--
----- functions/nav_ppall.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_ppall(
    IN  obj_start_flowline        cipsrv_nhdplus_h.flowline
   ,IN  obj_stop_flowline         cipsrv_nhdplus_h.flowline
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   rec         RECORD;
   int_count   INTEGER;
   int_check   INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_network_working30 temp table
   ----------------------------------------------------------------------------
   IF cipsrv_nhdplus_h.temp_table_exists('tmp_network_working30d')
   THEN
      TRUNCATE TABLE tmp_network_working30d;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_network_working30d(
          nhdplusid                   BIGINT
         ,hydroseq                    BIGINT
         ,fmeasure                    NUMERIC
         ,tmeasure                    NUMERIC
         ,lengthkm                    NUMERIC
         ,flowtimeday                 NUMERIC
         ,network_distancekm          NUMERIC
         ,network_flowtimeday         NUMERIC
         ,levelpathi                  BIGINT
         ,terminalpa                  BIGINT
         ,dnhydroseq                  BIGINT
         ,uphydroseq                  BIGINT
         ,fromnode                    BIGINT
         ,tonode                      BIGINT
         ,cost                        FLOAT8
      );

      CREATE INDEX tmp_network_working30d_01i
      ON tmp_network_working30d(nhdplusid);
      
      CREATE INDEX tmp_network_working30d_02i
      ON tmp_network_working30d(hydroseq);
      
      CREATE INDEX tmp_network_working30d_03i
      ON tmp_network_working30d(fromnode);
      
      CREATE INDEX tmp_network_working30d_04i
      ON tmp_network_working30d(tonode);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Run downstream mainline
   ----------------------------------------------------------------------------
   WITH RECURSIVE dm(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,base_pathlength
      ,base_pathtime
      ,fromnode
      ,tonode
      ,cost
   )
   AS (
      SELECT
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,obj_start_flowline.fmeasure
      ,obj_start_flowline.out_measure
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,obj_start_flowline.pathlengthkm    + obj_start_flowline.out_lengthkm
      ,obj_start_flowline.pathflowtimeday + obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.fromnode
      ,obj_start_flowline.tonode
      ,1::FLOAT8
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm  -- segment lengthkm
      ,mq.totma
      ,dm.base_pathlength - mq.pathlength
      ,dm.base_pathtime   - mq.pathtimema 
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,dm.base_pathlength -- base pathlength
      ,dm.base_pathtime
      ,mq.fromnode
      ,mq.tonode
      ,1::FLOAT8
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      dm
      WHERE
          mq.hydroseq   =  dm.dnhydroseq
      AND mq.terminalpa =  dm.terminalpa
      AND mq.hydroseq   >= obj_stop_flowline.hydroseq
   )
   INSERT INTO tmp_network_working30d(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi 
      ,terminalpa
      ,uphydroseq 
      ,dnhydroseq
      ,fromnode
      ,tonode
      ,cost
   )
   SELECT
    a.nhdplusid
   ,a.hydroseq
   ,a.fmeasure
   ,a.tmeasure
   ,a.lengthkm
   ,a.flowtimeday
   ,a.network_distancekm
   ,a.network_flowtimeday
   ,a.levelpathi
   ,a.terminalpa
   ,a.uphydroseq
   ,a.dnhydroseq
   ,a.fromnode
   ,a.tonode
   ,a.cost
   FROM
   dm a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Traverse any divergences
   ----------------------------------------------------------------------------
   LOOP
      FOR rec IN 
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,a.totma               AS flowtimeday
         ,b.network_distancekm  AS base_pathlength
         ,b.network_flowtimeday AS base_pathtime
         ,a.levelpathi
         ,a.terminalpa 
         ,a.uphydroseq
         ,a.dnhydroseq 
         ,a.fromnode
         ,a.tonode
         FROM 
         cipsrv_nhdplus_h.nhdplusflowlinevaa_nav a
         JOIN 
         tmp_network_working30d b
         ON
             a.ary_upstream_hydroseq @> ARRAY[b.hydroseq]
         AND a.hydroseq <> b.dnhydroseq
         WHERE
         NOT EXISTS (
            SELECT
            1
            FROM
            tmp_network_working30d cc
            WHERE
            cc.hydroseq = a.hydroseq
         )
         ORDER BY
         a.hydroseq DESC
      LOOP
      
         WITH RECURSIVE dm(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,base_pathlength
            ,base_pathtime
            ,fromnode
            ,tonode
            ,cost
         )
         AS (
            SELECT
             rec.nhdplusid
            ,rec.hydroseq
            ,rec.fmeasure
            ,rec.tmeasure
            ,rec.lengthkm
            ,rec.flowtimeday
            ,rec.base_pathlength  + rec.lengthkm
            ,rec.base_pathtime    + rec.flowtimeday
            ,rec.levelpathi
            ,rec.terminalpa
            ,rec.uphydroseq
            ,rec.dnhydroseq
            ,rec.base_pathlength 
            ,rec.base_pathtime
            ,rec.fromnode
            ,rec.tonode
            ,100::FLOAT8 AS cost
            UNION
            SELECT
             mq.nhdplusid
            ,mq.hydroseq
            ,mq.fmeasure
            ,mq.tmeasure
            ,mq.lengthkm
            ,mq.totma
            ,dm.network_distancekm  + mq.lengthkm
            ,dm.network_flowtimeday + mq.totma
            ,mq.levelpathi
            ,mq.terminalpa
            ,mq.uphydroseq
            ,mq.dnhydroseq
            ,dm.base_pathlength
            ,dm.base_pathtime
            ,mq.fromnode
            ,mq.tonode
            ,100::FLOAT8 AS cost
            FROM
            cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
            CROSS JOIN
            dm
            WHERE
                mq.hydroseq   = dm.dnhydroseq
            AND mq.terminalpa = dm.terminalpa
            AND mq.hydroseq   >= obj_stop_flowline.hydroseq
            AND NOT EXISTS (
               SELECT
               1
               FROM
               tmp_network_working30d cc
               WHERE
               cc.hydroseq = mq.hydroseq
            )
         )
         INSERT INTO tmp_network_working30d(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi 
            ,terminalpa
            ,uphydroseq 
            ,dnhydroseq
            ,fromnode
            ,tonode
            ,cost
         )
         SELECT
          a.nhdplusid
         ,a.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,a.flowtimeday
         ,a.network_distancekm
         ,a.network_flowtimeday
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.fromnode
         ,a.tonode
         ,a.cost
         FROM
         dm a
         ON CONFLICT DO NOTHING;         

      END LOOP;
   
      EXIT WHEN NOT FOUND;
   
   END LOOP;
      
   ----------------------------------------------------------------------------
   -- Step 40
   -- If downstream location not found, then exit
   ----------------------------------------------------------------------------
   SELECT
   COUNT(*)
   INTO int_count
   FROM 
   tmp_network_working30d a
   WHERE
   a.nhdplusid = obj_stop_flowline.nhdplusid;
   
   IF int_count = 0
   THEN
      RETURN 0;
      
   END IF;
      
   ----------------------------------------------------------------------------
   -- Step 50
   -- Turn around and go upstream
   ----------------------------------------------------------------------------
   WITH RECURSIVE ut(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,base_pathlength
      ,base_pathtime
      ,nav_order
   )
   AS (
      SELECT
       obj_stop_flowline.nhdplusid
      ,obj_stop_flowline.hydroseq
      ,obj_stop_flowline.out_measure
      ,obj_stop_flowline.tmeasure
      ,obj_stop_flowline.out_lengthkm
      ,obj_stop_flowline.out_flowtimeday
      ,obj_stop_flowline.out_lengthkm
      ,obj_stop_flowline.out_flowtimeday
      ,obj_stop_flowline.levelpathi
      ,obj_stop_flowline.terminalpa
      ,obj_stop_flowline.uphydroseq
      ,obj_stop_flowline.dnhydroseq
      ,obj_stop_flowline.pathlengthkm    + (obj_stop_flowline.lengthkm    - obj_stop_flowline.out_lengthkm)
      ,obj_stop_flowline.pathflowtimeday + (obj_stop_flowline.flowtimeday - obj_stop_flowline.out_flowtimeday)
      ,0 AS nav_order
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm
      ,mq.totma
      ,mq.pathlength - ut.base_pathlength + mq.lengthkm
      ,mq.pathtimema - ut.base_pathtime   + mq.totma
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,ut.base_pathlength
      ,ut.base_pathtime
      ,ut.nav_order + 1 
      FROM 
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      ut
      WHERE
      mq.ary_downstream_hydroseq @> ARRAY[ut.hydroseq]
      AND mq.hydroseq IN (
         SELECT
         b.hydroseq
         FROM
         tmp_network_working30d b
      )
   )
   INSERT INTO tmp_navigation_working30(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,nav_order
      ,selected
   )
   SELECT
    a.nhdplusid
   ,a.hydroseq
   ,a.fmeasure
   ,a.tmeasure
   ,a.lengthkm
   ,a.flowtimeday
   ,a.network_distancekm
   ,a.network_flowtimeday
   ,a.levelpathi
   ,a.terminalpa
   ,a.uphydroseq
   ,a.dnhydroseq
   ,a.nav_order
   ,TRUE
   FROM
   ut a
   ON CONFLICT DO NOTHING;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Trim the top of the run
   ----------------------------------------------------------------------------
   UPDATE tmp_navigation_working30 a
   SET 
    fmeasure            = obj_start_flowline.fmeasure
   ,tmeasure            = obj_start_flowline.out_measure
   ,lengthkm            = obj_start_flowline.out_lengthkm
   ,flowtimeday         = obj_start_flowline.out_flowtimeday
   ,network_distancekm  = a.network_distancekm  - (a.lengthkm    - obj_start_flowline.out_lengthkm)
   ,network_flowtimeday = a.network_flowtimeday - (a.flowtimeday - obj_start_flowline.out_flowtimeday)
   ,navtermination_flag = 2
   WHERE
   a.hydroseq = obj_start_flowline.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Return total count of results
   ----------------------------------------------------------------------------
   SELECT 
   COUNT(*) 
   INTO int_count 
   FROM 
   tmp_navigation_working30 a
   WHERE 
   a.selected IS TRUE;
   
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_ppall(
    cipsrv_nhdplus_h.flowline
   ,cipsrv_nhdplus_h.flowline  
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_ppall(
    cipsrv_nhdplus_h.flowline
   ,cipsrv_nhdplus_h.flowline
)  TO PUBLIC;

--******************************--
----- functions/nav_single.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_single(
    IN  str_search_type           VARCHAR
   ,IN  obj_start_flowline        cipsrv_nhdplus_h.flowline
   ,IN  obj_stop_flowline         cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distancekm    NUMERIC
   ,IN  num_maximum_flowtimeday   NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   
   num_init_meas_total      NUMERIC;
   num_init_fmeasure        NUMERIC;
   num_init_tmeasure        NUMERIC;
   num_init_lengthkm        NUMERIC;
   num_init_flowtimeday     NUMERIC;
   int_navtermination_flag  INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Calculate the single flowline navigation
   ----------------------------------------------------------------------------
   IF obj_start_flowline.nhdplusid = obj_stop_flowline.nhdplusid
   THEN
      num_init_meas_total  := ABS(obj_stop_flowline.out_measure - obj_start_flowline.out_measure);
      num_init_lengthkm    := num_init_meas_total * obj_start_flowline.lengthkm_ratio;
      num_init_flowtimeday := num_init_meas_total * obj_start_flowline.flowtimeday_ratio;

      IF obj_start_flowline.out_measure < obj_stop_flowline.out_measure
      THEN
         num_init_fmeasure := obj_start_flowline.out_measure;
         num_init_tmeasure := obj_stop_flowline.out_measure;

      ELSE
         num_init_fmeasure := obj_stop_flowline.out_measure;
         num_init_tmeasure := obj_start_flowline.out_measure;

      END IF;
      
   ELSIF num_maximum_distancekm < obj_start_flowline.out_lengthkm
   THEN
      IF str_search_type IN ('UM','UT')
      THEN
         num_init_fmeasure := obj_start_flowline.out_measure;
         num_init_tmeasure := obj_start_flowline.out_measure + ROUND(num_maximum_distancekm / obj_start_flowline.lengthkm_ratio,5);
         
      ELSE
         num_init_fmeasure := obj_start_flowline.out_measure - ROUND(num_maximum_distancekm / obj_start_flowline.lengthkm_ratio,5);
         num_init_tmeasure := obj_start_flowline.out_measure;

      END IF;

      num_init_lengthkm    := num_maximum_distancekm;
      num_init_flowtimeday := (num_init_tmeasure - num_init_fmeasure) * obj_start_flowline.flowtimeday_ratio;

   ELSIF num_maximum_flowtimeday < obj_start_flowline.out_flowtimeday
   THEN
      IF str_search_type IN ('UM','UT')
      THEN
         num_init_fmeasure := obj_start_flowline.out_measure;
         num_init_tmeasure := obj_start_flowline.out_measure + ROUND(num_maximum_flowtimeday / obj_start_flowline.flowtimeday_ratio,5);
         
      ELSE
         num_init_fmeasure := obj_start_flowline.out_measure - ROUND(num_maximum_flowtimeday / obj_start_flowline.flowtimeday_ratio,5);
         num_init_tmeasure := obj_start_flowline.out_measure;

      END IF;

      num_init_lengthkm    := (num_init_tmeasure - num_init_fmeasure) * obj_start_flowline.lengthkm_ratio;
      num_init_flowtimeday := num_maximum_flowtimeday;

   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Test just in case the results are exactly the same as original flowline
   ----------------------------------------------------------------------------
   IF obj_start_flowline.tmeasure - obj_start_flowline.fmeasure = num_init_tmeasure - num_init_fmeasure
   THEN
      int_navtermination_flag := 1;
      
   ELSE
      int_navtermination_flag := 2;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Insert the results
   ----------------------------------------------------------------------------
   INSERT INTO tmp_navigation_working30(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,navtermination_flag
      ,nav_order
      ,selected
   ) VALUES (
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,num_init_fmeasure
      ,num_init_tmeasure
      ,num_init_lengthkm
      ,num_init_flowtimeday
      ,num_init_lengthkm
      ,num_init_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,int_navtermination_flag
      ,0
      ,TRUE
   );

   ----------------------------------------------------------------------------
   -- Step 40
   -- Insert the initial flowline and tag the running counts
   ----------------------------------------------------------------------------
   RETURN 1;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_single(
    VARCHAR
   ,cipsrv_nhdplus_h.flowline
   ,cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_single(
    VARCHAR
   ,cipsrv_nhdplus_h.flowline
   ,cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
)  TO PUBLIC;

--******************************--
----- functions/nav_trim_temp.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_trim_temp(
    IN  p_search_type          VARCHAR
   ,IN  p_fmeasure             NUMERIC
   ,IN  p_tmeasure             NUMERIC
   ,IN  p_lengthkm             NUMERIC
   ,IN  p_flowtimeday          NUMERIC
   ,IN  p_network_distancekm   NUMERIC
   ,IN  p_network_flowtimeday  NUMERIC
   ,IN  p_maximum_distancekm   NUMERIC
   ,IN  p_maximum_flowtimeday  NUMERIC
) RETURNS TABLE(
    fmeasure            NUMERIC
   ,tmeasure            NUMERIC
   ,lengthkm            NUMERIC
   ,flowtimeday         NUMERIC
   ,network_distancekm  NUMERIC
   ,network_flowtimeday NUMERIC
)
IMMUTABLE
AS $BODY$ 
DECLARE
   num_fmeasure            NUMERIC := p_fmeasure;
   num_tmeasure            NUMERIC := p_tmeasure;
   num_lengthkm            NUMERIC := p_lengthkm;
   num_flowtimeday         NUMERIC := p_flowtimeday;
   num_network_distancekm  NUMERIC := p_network_distancekm;
   num_network_flowtimeday NUMERIC := p_network_flowtimeday;
   
   num_ratio               NUMERIC;
   num_ratio_other         NUMERIC;
   num_trim                NUMERIC;
   num_trim_meas           NUMERIC;
   
BEGIN

   IF p_maximum_distancekm IS NOT NULL
   THEN
      num_ratio       := p_lengthkm / (p_tmeasure - p_fmeasure);
      num_ratio_other := p_flowtimeday / (p_tmeasure - p_fmeasure);  
      
      num_trim        := p_network_distancekm - p_maximum_distancekm;
      
   ELSIF p_maximum_flowtimeday IS NOT NULL
   THEN
      num_ratio       := p_flowtimeday / (p_tmeasure - p_fmeasure);
      num_ratio_other := p_lengthkm / (p_tmeasure - p_fmeasure);
      
      num_trim        := p_network_flowtimeday - p_maximum_flowtimeday;
      
   END IF;

   IF  num_trim > 0
   AND num_ratio > 0
   THEN
      num_trim_meas := num_trim / num_ratio;
   
      IF p_search_type IN ('UT','UM')
      THEN
         num_tmeasure := ROUND(p_tmeasure - num_trim_meas,5);
      
      ELSE
         num_fmeasure := ROUND(p_fmeasure + num_trim_meas,5);
      
      END IF;
      
      IF p_maximum_distancekm IS NOT NULL
      THEN
         num_lengthkm            := num_lengthkm - num_trim;
         num_network_distancekm  := num_network_distancekm - num_trim;
         
         num_flowtimeday         := num_flowtimeday - (num_trim_meas * num_ratio_other);
         num_network_flowtimeday := num_network_flowtimeday - (num_trim_meas * num_ratio_other);
      
      ELSIF p_maximum_flowtimeday IS NOT NULL
      THEN
         num_lengthkm            := num_lengthkm - (num_trim_meas * num_ratio_other);
         num_network_distancekm  := num_network_distancekm - (num_trim_meas * num_ratio_other);
         
         num_flowtimeday         := num_flowtimeday - num_trim;
         num_network_flowtimeday := num_network_flowtimeday - num_trim;
         
      END IF;
      
   END IF;

   RETURN QUERY SELECT
    num_fmeasure
   ,num_tmeasure
   ,num_lengthkm
   ,num_flowtimeday
   ,num_network_distancekm
   ,num_network_flowtimeday;            
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_trim_temp(
    VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_trim_temp(
    VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/nav_um.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_um(
    IN  obj_start_flowline       cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distancekm   NUMERIC
   ,IN  num_maximum_flowtimeday  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE  
   int_count INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Return total count of results
   ----------------------------------------------------------------------------
   WITH RECURSIVE um(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,divergence
      ,base_pathlength
      ,base_pathtime
      ,nav_order
   )
   AS (
      SELECT
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,obj_start_flowline.out_measure
      ,obj_start_flowline.tmeasure
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,obj_start_flowline.divergence
      ,obj_start_flowline.pathlengthkm    + (obj_start_flowline.lengthkm    - obj_start_flowline.out_lengthkm)
      ,obj_start_flowline.pathflowtimeday + (obj_start_flowline.flowtimeday - obj_start_flowline.out_flowtimeday)
      ,0 AS nav_order
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm  -- segment lengthkm
      ,mq.totma
      ,mq.pathlength - um.base_pathlength + mq.lengthkm
      ,mq.pathtimema - um.base_pathtime   + mq.totma
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,mq.divergence
      ,um.base_pathlength -- base pathlength
      ,um.base_pathtime
      ,um.nav_order + 1 
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      um
      WHERE
      (
         (
                mq.hydroseq   = um.uphydroseq
            AND mq.levelpathi = um.levelpathi
         )
         OR (
                mq.hydroseq   = um.uphydroseq
            AND um.divergence = 2
         )
      )
      AND(
            num_maximum_distancekm IS NULL
         OR mq.pathlength - um.base_pathlength <= num_maximum_distancekm
      )
      AND (
            num_maximum_flowtimeday IS NULL
         OR mq.pathtimema - um.base_pathtime   <= num_maximum_flowtimeday
      )
   )
   INSERT INTO tmp_navigation_working30(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,nav_order
      ,selected
   )
   SELECT
    a.nhdplusid
   ,a.hydroseq
   ,a.fmeasure
   ,a.tmeasure
   ,a.lengthkm
   ,a.flowtimeday
   ,a.network_distancekm
   ,a.network_flowtimeday
   ,a.levelpathi
   ,a.terminalpa
   ,a.uphydroseq
   ,a.dnhydroseq
   ,a.nav_order
   ,TRUE
   FROM
   um a;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Tag the nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,b.headwater
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
          a.selected = TRUE   
      AND a.navtermination_flag IS NULL
   )
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN a.nav_order = (SELECT MAX(c.nav_order) FROM tmp_navigation_working30 c LIMIT 1)
   THEN
      CASE
      WHEN cte.headwater
      THEN
         4
      ELSE
         1
      END
   ELSE
      0    
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return total count of results
   ----------------------------------------------------------------------------
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_um(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC   
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_um(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
)  TO PUBLIC;

--******************************--
----- functions/nav_ut_concise.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_ut_concise(
    IN  obj_start_flowline       cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distancekm   NUMERIC
   ,IN  num_maximum_flowtimeday  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   int_count INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Return total count of results
   ----------------------------------------------------------------------------
   WITH RECURSIVE ut(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,base_pathlength
      ,base_pathtime
      ,nav_order
   )
   AS (
      SELECT
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,obj_start_flowline.out_measure
      ,obj_start_flowline.tmeasure
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,obj_start_flowline.pathlengthkm    + (obj_start_flowline.lengthkm    - obj_start_flowline.out_lengthkm)
      ,obj_start_flowline.pathflowtimeday + (obj_start_flowline.flowtimeday - obj_start_flowline.out_flowtimeday)
      ,0 AS nav_order
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm
      ,mq.totma
      ,mq.pathlength - ut.base_pathlength + mq.lengthkm
      ,mq.pathtimema - ut.base_pathtime   + mq.totma
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,ut.base_pathlength
      ,ut.base_pathtime
      ,ut.nav_order + 1
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      ut
      WHERE
      mq.ary_downstream_hydroseq @> ARRAY[ut.hydroseq]
      AND (
            num_maximum_distancekm IS NULL
         OR mq.pathlength - ut.base_pathlength <= num_maximum_distancekm
      )
      AND (
            num_maximum_flowtimeday IS NULL
         OR mq.pathtimema - ut.base_pathtime   <= num_maximum_flowtimeday
      )
   )
   INSERT INTO tmp_navigation_working30(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,nav_order
      ,selected
   )
   SELECT
    a.nhdplusid
   ,a.hydroseq
   ,a.fmeasure
   ,a.tmeasure
   ,a.lengthkm
   ,a.flowtimeday
   ,a.network_distancekm
   ,a.network_flowtimeday
   ,a.levelpathi
   ,a.terminalpa
   ,a.uphydroseq
   ,a.dnhydroseq
   ,a.nav_order
   ,TRUE
   FROM
   ut a
   ON CONFLICT DO NOTHING;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Tag the upstream mainline nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,b.ary_upstream_hydroseq
      ,b.headwater
      ,b.coastal_connection
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
          a.selected = TRUE   
      AND a.navtermination_flag IS NULL
   )
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN EXISTS ( SELECT 1 FROM tmp_navigation_working30 d WHERE d.hydroseq = ANY(cte.ary_upstream_hydroseq) )
   THEN
      0
   ELSE
      CASE
      WHEN cte.headwater
      THEN
         4
      ELSE
         1
      END
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return total count of results
   ----------------------------------------------------------------------------
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_ut_concise(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC   
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_ut_concise(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
)  TO PUBLIC;

--******************************--
----- functions/nav_ut_extended.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_ut_extended(
    IN  obj_start_flowline       cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distancekm   NUMERIC
   ,IN  num_maximum_flowtimeday  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   rec                      RECORD;
   int_count                INTEGER;
   int_check                INTEGER;
   num_init_baselengthkm    NUMERIC;
   num_init_baseflowtimeday NUMERIC;
   
BEGIN

   num_init_baselengthkm    := obj_start_flowline.pathlengthkm    + (obj_start_flowline.lengthkm    - obj_start_flowline.out_lengthkm);
   num_init_baseflowtimeday := obj_start_flowline.pathflowtimeday + (obj_start_flowline.flowtimeday - obj_start_flowline.out_flowtimeday);
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- First run upstream mainline
   ----------------------------------------------------------------------------
   WITH RECURSIVE um(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,divergence
      ,base_pathlength
      ,base_pathtime
      ,nav_order
   )
   AS (
      SELECT
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,obj_start_flowline.out_measure
      ,obj_start_flowline.tmeasure
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.out_lengthkm
      ,obj_start_flowline.out_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,obj_start_flowline.divergence
      ,num_init_baselengthkm
      ,num_init_baseflowtimeday
      ,0 AS nav_order
      UNION
      SELECT
       mq.nhdplusid
      ,mq.hydroseq
      ,mq.fmeasure
      ,mq.tmeasure
      ,mq.lengthkm  -- segment lengthkm
      ,mq.totma
      ,mq.pathlength - um.base_pathlength + mq.lengthkm
      ,mq.pathtimema - um.base_pathtime   + mq.totma
      ,mq.levelpathi
      ,mq.terminalpa
      ,mq.uphydroseq
      ,mq.dnhydroseq
      ,mq.divergence
      ,um.base_pathlength -- base pathlength
      ,um.base_pathtime
      ,um.nav_order + 1000              
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
      CROSS JOIN
      um
      WHERE 
      (
         (
                mq.hydroseq   = um.uphydroseq
            AND mq.levelpathi = um.levelpathi
         )
         OR (
                mq.hydroseq    = um.uphydroseq
            AND um.divergence  = 2
         )
         OR (
                mq.force_main_line IS TRUE
            AND mq.dnhydroseq  = um.hydroseq
         )
      )
      AND (
            num_maximum_distancekm IS NULL
         OR mq.pathlength - um.base_pathlength <= num_maximum_distancekm
      )
      AND (
            num_maximum_flowtimeday IS NULL
         OR mq.pathtimema - um.base_pathtime   <= num_maximum_flowtimeday
      )
   )
   INSERT INTO tmp_navigation_working30(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,nav_order
      ,selected
   )
   SELECT
    a.nhdplusid
   ,a.hydroseq
   ,a.fmeasure
   ,a.tmeasure
   ,a.lengthkm
   ,a.flowtimeday
   ,a.network_distancekm
   ,a.network_flowtimeday
   ,a.levelpathi
   ,a.terminalpa
   ,a.uphydroseq
   ,a.dnhydroseq
   ,a.nav_order
   ,TRUE
   FROM
   um a
   ON CONFLICT DO NOTHING;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Tag the upstream mainline nav termination flags
   ----------------------------------------------------------------------------
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN a.nav_order = (SELECT MAX(b.nav_order) FROM tmp_navigation_working30 b LIMIT 1)
   THEN
      1
   ELSE
      0
   END
   WHERE 
   selected = TRUE;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Extract the divs off the mainline
   ----------------------------------------------------------------------------
   FOR rec IN 
      SELECT 
       a.nhdplusid
      ,a.hydroseq
      ,a.fmeasure
      ,a.tmeasure
      ,a.lengthkm
      ,a.totma
      ,b.network_distancekm  + a.lengthkm AS network_distancekm
      ,b.network_flowtimeday + a.totma    AS network_flowtimeday 
      ,a.levelpathi
      ,a.terminalpa
      ,a.uphydroseq
      ,a.dnhydroseq
      ,b.nav_order
      FROM 
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav a
      JOIN
      tmp_navigation_working30 b
      ON
      a.ary_downstream_hydroseq @> ARRAY[b.hydroseq]
      WHERE NOT EXISTS (
         SELECT
         1
         FROM
         tmp_navigation_working30 cc
         WHERE
         cc.hydroseq = a.hydroseq
      )
   
   LOOP
      
      BEGIN
         INSERT INTO tmp_navigation_working30(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,nav_order
            ,selected
         ) VALUES (
             rec.nhdplusid
            ,rec.hydroseq
            ,rec.fmeasure
            ,rec.tmeasure
            ,rec.lengthkm
            ,rec.totma
            ,rec.network_distancekm
            ,rec.network_flowtimeday
            ,rec.levelpathi
            ,rec.terminalpa
            ,rec.uphydroseq
            ,rec.dnhydroseq
            ,rec.nav_order
            ,TRUE
         );
   
         WITH RECURSIVE ut(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,base_pathlength
            ,base_pathtime
            ,nav_order
         )
         AS (
            SELECT
             rec.nhdplusid
            ,rec.hydroseq
            ,rec.fmeasure
            ,rec.tmeasure
            ,rec.lengthkm
            ,rec.totma
            ,rec.lengthkm
            ,rec.totma
            ,rec.levelpathi
            ,rec.terminalpa
            ,rec.uphydroseq
            ,rec.dnhydroseq
            ,num_init_baselengthkm 
            ,num_init_baseflowtimeday
            ,rec.nav_order
            UNION
            SELECT
             mq.nhdplusid
            ,mq.hydroseq
            ,mq.fmeasure
            ,mq.tmeasure
            ,mq.lengthkm
            ,mq.totma
            ,mq.pathlength - ut.base_pathlength + mq.lengthkm
            ,mq.pathtimema - ut.base_pathtime   + mq.totma
            ,mq.levelpathi
            ,mq.terminalpa
            ,mq.uphydroseq
            ,mq.dnhydroseq
            ,ut.base_pathlength
            ,ut.base_pathtime
            ,ut.nav_order + 1 
            FROM
            cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
            CROSS JOIN
            ut
            WHERE
            mq.ary_downstream_hydroseq @> ARRAY[ut.hydroseq]
            AND (
                  num_maximum_distancekm IS NULL
               OR mq.pathlength - ut.base_pathlength <= num_maximum_distancekm
            )
            AND (
                  num_maximum_flowtimeday IS NULL
               OR mq.pathtimema - ut.base_pathtime   <= num_maximum_flowtimeday
            )
            AND NOT EXISTS (
               SELECT
               1
               FROM
               tmp_navigation_working30 cc
               WHERE
               cc.hydroseq = mq.hydroseq
            )
         )
         INSERT INTO tmp_navigation_working30(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,network_distancekm
            ,network_flowtimeday
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,nav_order
            ,selected
         )
         SELECT
          a.nhdplusid
         ,a.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,a.flowtimeday
         ,a.network_distancekm
         ,a.network_flowtimeday
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.nav_order
         ,TRUE
         FROM
         ut a
         WHERE
         a.nhdplusid <> rec.nhdplusid
         ON CONFLICT DO NOTHING;
         
         -- At some point this should be removed
         GET DIAGNOSTICS int_check = row_count;
         IF int_check > 10000
         THEN
            RAISE WARNING '% %',rec.nhdplusid,int_check;
            
         END IF;              
         
         int_count := int_count + int_check;
         
      EXCEPTION
         WHEN UNIQUE_VIOLATION 
         THEN
            NULL;

         WHEN OTHERS
         THEN               
            RAISE;
            
      END;

   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Tag the upstream mainline nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,b.ary_upstream_hydroseq
      ,b.headwater
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
          a.selected = TRUE   
      AND a.navtermination_flag IS NULL
   )
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN EXISTS ( SELECT 1 FROM tmp_navigation_working30 d WHERE d.hydroseq = ANY(cte.ary_upstream_hydroseq) )
   THEN
      0
   ELSE
      CASE
      WHEN cte.headwater
      THEN
         4
      ELSE
         1
      END
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Return total count of results
   ----------------------------------------------------------------------------
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_ut_extended(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC   
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_ut_extended(
    cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
)  TO PUBLIC;

--******************************--
----- functions/navigate.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.navigate';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.navigate(
    IN  p_search_type                  VARCHAR
   ,IN  p_start_nhdplusid              BIGINT
   ,IN  p_start_permanent_identifier   VARCHAR
   ,IN  p_start_reachcode              VARCHAR
   ,IN  p_start_hydroseq               BIGINT
   ,IN  p_start_measure                NUMERIC
   ,IN  p_stop_nhdplusid               BIGINT
   ,IN  p_stop_permanent_identifier    VARCHAR
   ,IN  p_stop_reachcode               VARCHAR
   ,IN  p_stop_hydroseq                BIGINT
   ,IN  p_stop_measure                 NUMERIC
   ,IN  p_max_distancekm               NUMERIC
   ,IN  p_max_flowtimeday              NUMERIC
   ,IN  p_return_flowline_details      BOOLEAN
   ,IN  p_return_flowline_geometry     BOOLEAN
   ,IN  p_known_region                 VARCHAR DEFAULT NULL
   ,OUT out_start_nhdplusid            BIGINT
   ,OUT out_start_permanent_identifier VARCHAR
   ,OUT out_start_measure              NUMERIC
   ,OUT out_grid_srid                  INTEGER
   ,OUT out_stop_nhdplusid             BIGINT
   ,OUT out_stop_measure               NUMERIC
   ,OUT out_flowline_count             INTEGER
   ,OUT out_return_code                NUMERIC
   ,OUT out_status_message             VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                            RECORD;
   str_search_type                VARCHAR(16) := UPPER(p_search_type); 
   obj_start_flowline             cipsrv_nhdplus_h.flowline;
   obj_stop_flowline              cipsrv_nhdplus_h.flowline;
   num_maximum_distancekm         NUMERIC     := p_max_distancekm;
   num_maximum_flowtimeday        NUMERIC     := p_max_flowtimeday;
   int_counter                    INTEGER;
   int_check                      INTEGER;
   boo_return_flowline_details    BOOLEAN;
   boo_return_flowline_geometry   BOOLEAN;

BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   out_return_code    := 0;
   obj_start_flowline := NULL::cipsrv_nhdplus_h.flowline;
   obj_stop_flowline  := NULL::cipsrv_nhdplus_h.flowline;
   
   IF str_search_type IN ('PP','POINT TO POINT','POINT-TO-POINT')
   THEN
      str_search_type := 'PP';
      
   ELSIF str_search_type IN ('PPALL')
   THEN
      str_search_type := 'PPALL';
   
   ELSIF str_search_type IN ('UT','UPSTREAM WITH TRIBUTARIES')
   THEN
      str_search_type := 'UT';
   
   ELSIF str_search_type IN ('UM','UPSTREAM MAIN PATH ONLY')
   THEN
      str_search_type := 'UM';
   
   ELSIF str_search_type IN ('DD','DOWNSTREAM WITH DIVERGENCES')
   THEN
      str_search_type := 'DD';
   
   ELSIF str_search_type IN ('DM','DOWNSTREAM MAIN PATH ONLY')
   THEN
      str_search_type := 'DM';
      
   ELSE
      out_return_code    := -1;
      out_status_message := 'Valid SearchType codes are UM, UT, DM, DD and PP.';

   END IF;

   IF str_search_type IN ('PP','PPALL')
   THEN
      num_maximum_distancekm  := NULL;
      num_maximum_flowtimeday := NULL;

   END IF;

   IF  num_maximum_distancekm  IS NOT NULL
   AND num_maximum_flowtimeday IS NOT NULL
   THEN
      num_maximum_flowtimeday := NULL;

   END IF;
   
   IF num_maximum_distancekm = 0
   OR num_maximum_flowtimeday = 0
   THEN
      out_return_code    := -3;
      out_status_message := 'navigation for zero distance or flowtime is not valid.';
   
   END IF;
   
   boo_return_flowline_details := p_return_flowline_details;
   IF boo_return_flowline_details IS NULL
   THEN
      boo_return_flowline_details := TRUE;
      
   END IF;
   
   boo_return_flowline_geometry := p_return_flowline_geometry;
   IF boo_return_flowline_geometry IS NULL
   THEN
      boo_return_flowline_geometry := TRUE;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Flush or create the temp tables
   ----------------------------------------------------------------------------
   int_check := cipsrv_engine.create_navigation_temp_tables();

   ----------------------------------------------------------------------------
   -- Step 40
   -- Get the start flowline
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.get_flowline(
       p_direction            := str_search_type
      ,p_nhdplusid            := p_start_nhdplusid
      ,p_permanent_identifier := p_start_permanent_identifier
      ,p_reachcode            := p_start_reachcode
      ,p_hydroseq             := p_start_hydroseq
      ,p_measure              := p_start_measure
      ,p_known_region         := p_known_region
   );
   out_return_code        := rec.out_return_code;
   out_status_message     := rec.out_status_message;
   
   IF out_return_code <> 0
   THEN
      IF out_return_code = -10
      THEN
         out_status_message := 'Flowline ' || COALESCE(
             p_start_nhdplusid::VARCHAR
            ,p_start_permanent_identifier
            ,p_start_reachcode
            ,p_start_hydroseq::VARCHAR
            ,'err'
         );
         
         IF p_start_measure IS NOT NULL
         THEN
            out_status_message := out_status_message || ' at measure ' || p_start_measure::VARCHAR;
            
         END IF;
         
         out_status_message := out_status_message || ' not found in NHDPlus stream network.';
         
      END IF;
      
      RETURN;
      
   END IF;

   IF rec.out_flowline IS NULL
   THEN
      RAISE EXCEPTION 'start get flowline returned no results';
   
   END IF;
   
   obj_start_flowline := rec.out_flowline;
   out_grid_srid := obj_start_flowline.out_grid_srid;
   
   IF obj_start_flowline.hydroseq IS NULL
   THEN
      out_return_code    := -22;
      out_status_message := 'Start flowline is not part of the NHDPlus network.';
      
      RETURN;
   
   ELSIF num_maximum_flowtimeday IS NOT NULL
   AND   obj_start_flowline.flowtimeday IS NULL
   THEN
      out_return_code    := -23;
      out_status_message := 'Start flowline is tidal without flow time information.';
      
      RETURN;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Get the stop flowline
   ----------------------------------------------------------------------------
   IF str_search_type IN ('PP','PPALL')
   THEN
      rec := cipsrv_nhdplus_h.get_flowline(
          p_direction            := 'U'
         ,p_nhdplusid            := p_stop_nhdplusid
         ,p_permanent_identifier := p_stop_permanent_identifier
         ,p_reachcode            := p_stop_reachcode
         ,p_hydroseq             := p_stop_hydroseq
         ,p_measure              := p_stop_measure
         ,p_known_region         := p_known_region
      );
      out_return_code       := rec.out_return_code;
      out_status_message    := rec.out_status_message;

      IF out_return_code <> 0
      THEN
         RETURN;
         
      END IF;
   
      IF rec.out_flowline IS NULL
      THEN
         RAISE EXCEPTION 'stop get flowline returned no results';
      
      END IF;
      
      obj_stop_flowline := rec.out_flowline;

      IF obj_stop_flowline.hydroseq IS NULL
      THEN
         out_return_code    := -22;
         out_status_message := 'Stop flowline is not part of the NHDPlus network.';
      
         RETURN;
      
      ELSIF num_maximum_flowtimeday IS NOT NULL
      AND   obj_stop_flowline.flowtimeday IS NULL
      THEN
         out_return_code    := -23;
         out_status_message := 'Stop flowline is tidal without flow time information.';
         
         RETURN;
         
      END IF;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Turn PP around if stop above start
   ----------------------------------------------------------------------------
   IF obj_stop_flowline.hydroseq > obj_start_flowline.hydroseq
   THEN
      rec := cipsrv_nhdplus_h.get_flowline(
          p_direction            := str_search_type
         ,p_nhdplusid            := p_stop_nhdplusid
         ,p_permanent_identifier := p_stop_permanent_identifier
         ,p_reachcode            := p_stop_reachcode
         ,p_hydroseq             := p_stop_hydroseq
         ,p_measure              := p_stop_measure
         ,p_known_region         := p_known_region
      );
      out_return_code        := rec.out_return_code;
      out_status_message     := rec.out_status_message;
      obj_start_flowline     := rec.out_flowline;
      
      rec := cipsrv_nhdplus_h.get_flowline(
          p_direction            := 'U'
         ,p_nhdplusid            := p_start_nhdplusid
         ,p_permanent_identifier := p_start_permanent_identifier
         ,p_reachcode            := p_start_reachcode
         ,p_hydroseq             := p_start_hydroseq
         ,p_measure              := p_start_measure
         ,p_known_region         := p_known_region
      );
      out_return_code       := rec.out_return_code;
      out_status_message    := rec.out_status_message;
      obj_stop_flowline     := rec.out_flowline;

   END IF;
   
   out_start_nhdplusid            := obj_start_flowline.nhdplusid;
   out_start_permanent_identifier := obj_start_flowline.permanent_identifier;
   out_start_measure              := obj_start_flowline.out_measure;
   out_stop_nhdplusid             := obj_stop_flowline.nhdplusid;
   out_stop_measure               := obj_stop_flowline.out_measure;

   ----------------------------------------------------------------------------
   -- Step 70
   -- Abend if start or stop is coastal
   ----------------------------------------------------------------------------
   IF obj_start_flowline.fcode = 56600
   OR obj_stop_flowline.fcode  = 56600
   THEN
      out_return_code      := -56600;
      out_status_message   := 'Navigation from or to coastal flowlines is not valid.';
      
      RETURN;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Create the initial flowline and deal with single flowline search
   ----------------------------------------------------------------------------
   IF obj_start_flowline.nhdplusid = obj_stop_flowline.nhdplusid
   OR num_maximum_distancekm < obj_start_flowline.out_lengthkm
   OR num_maximum_flowtimeday < obj_start_flowline.out_flowtimeday
   THEN
      int_counter := cipsrv_nhdplus_h.nav_single(
          str_search_type          := str_search_type
         ,obj_start_flowline       := obj_start_flowline
         ,obj_stop_flowline        := obj_stop_flowline
         ,num_maximum_distancekm   := num_maximum_distancekm
         ,num_maximum_flowtimeday  := num_maximum_flowtimeday
      );

   ELSE
   
   ----------------------------------------------------------------------------
   -- Step 90
   -- Do Point to Point
   ----------------------------------------------------------------------------
      IF str_search_type = 'PP'
      THEN
         int_counter := cipsrv_nhdplus_h.nav_pp(
             obj_start_flowline       := obj_start_flowline
            ,obj_stop_flowline        := obj_stop_flowline
         );
         
      ELSIF str_search_type = 'PPALL'
      THEN
         int_counter := cipsrv_nhdplus_h.nav_ppall(
             obj_start_flowline       := obj_start_flowline
            ,obj_stop_flowline        := obj_stop_flowline
         );
         
      ELSE
   ----------------------------------------------------------------------------
   -- Step 100
   -- Do upstream search with tributaries
   ----------------------------------------------------------------------------
         IF str_search_type = 'UT'
         THEN 
            IF (
                   num_maximum_distancekm  IS NULL
               AND num_maximum_flowtimeday IS NULL
               AND obj_start_flowline.arbolatesu > 500
            ) OR (
                   num_maximum_distancekm  IS NOT NULL
               AND num_maximum_distancekm > 200
               AND obj_start_flowline.arbolatesu > 200
            ) OR (
                   num_maximum_flowtimeday  IS NOT NULL
               AND num_maximum_flowtimeday > 3
               AND obj_start_flowline.arbolatesu > 200
            )
            THEN
               int_counter := cipsrv_nhdplus_h.nav_ut_extended(
                   obj_start_flowline      := obj_start_flowline
                  ,num_maximum_distancekm  := num_maximum_distancekm
                  ,num_maximum_flowtimeday := num_maximum_flowtimeday
               );

            ELSE   
               int_counter := cipsrv_nhdplus_h.nav_ut_concise(
                   obj_start_flowline      := obj_start_flowline
                  ,num_maximum_distancekm  := num_maximum_distancekm
                  ,num_maximum_flowtimeday := num_maximum_flowtimeday
               );

            END IF;
                 
   ----------------------------------------------------------------------------
   -- Step 110
   -- Do upstream search main line
   ----------------------------------------------------------------------------
         ELSIF str_search_type = 'UM'
         THEN
            int_counter := cipsrv_nhdplus_h.nav_um(
                obj_start_flowline      := obj_start_flowline
               ,num_maximum_distancekm  := num_maximum_distancekm
               ,num_maximum_flowtimeday := num_maximum_flowtimeday
            );

   ----------------------------------------------------------------------------
   -- Step 120
   -- Do downstream search main line
   ----------------------------------------------------------------------------
         ELSIF str_search_type = 'DM'
         THEN
            int_counter := cipsrv_nhdplus_h.nav_dm(
                obj_start_flowline      := obj_start_flowline
               ,num_maximum_distancekm  := num_maximum_distancekm
               ,num_maximum_flowtimeday := num_maximum_flowtimeday
            );

   ----------------------------------------------------------------------------
   -- Step 130
   -- Do downstream with divergences 
   -------------------------------------------------------------------
         ELSIF str_search_type = 'DD'
         THEN
            int_counter := cipsrv_nhdplus_h.nav_dd(
                obj_start_flowline      := obj_start_flowline
               ,num_maximum_distancekm  := num_maximum_distancekm
               ,num_maximum_flowtimeday := num_maximum_flowtimeday
            );

         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;

   ----------------------------------------------------------------------------
   -- Step 140
   -- Trim endings and mark partial flowline termination flags
   ----------------------------------------------------------------------------
         IF num_maximum_distancekm IS NOT NULL
         THEN
            UPDATE tmp_navigation_working30 a
            SET (
                fmeasure
               ,tmeasure
               ,lengthkm
               ,flowtimeday
               ,network_distancekm
               ,network_flowtimeday
               ,navtermination_flag
            ) = (
               SELECT
                aa.fmeasure
               ,aa.tmeasure
               ,aa.lengthkm
               ,aa.flowtimeday
               ,aa.network_distancekm
               ,aa.network_flowtimeday
               ,2
               FROM
               cipsrv_nhdplus_h.nav_trim_temp(
                   p_search_type          := str_search_type
                  ,p_fmeasure             := a.fmeasure
                  ,p_tmeasure             := a.tmeasure
                  ,p_lengthkm             := a.lengthkm
                  ,p_flowtimeday          := a.flowtimeday
                  ,p_network_distancekm   := a.network_distancekm
                  ,p_network_flowtimeday  := a.network_flowtimeday
                  ,p_maximum_distancekm   := num_maximum_distancekm
                  ,p_maximum_flowtimeday  := num_maximum_flowtimeday
               ) aa
            )
            WHERE
                a.selected IS TRUE
            AND a.network_distancekm > num_maximum_distancekm;

         ELSIF num_maximum_flowtimeday IS NOT NULL
         THEN
            UPDATE tmp_navigation_working30 a
            SET (
                fmeasure
               ,tmeasure
               ,lengthkm
               ,flowtimeday
               ,network_distancekm
               ,network_flowtimeday
               ,navtermination_flag
            ) = (
               SELECT
                aa.fmeasure
               ,aa.tmeasure
               ,aa.lengthkm
               ,aa.flowtimeday
               ,aa.network_distancekm
               ,aa.network_flowtimeday
               ,2
               FROM
               cipsrv_nhdplus_h.nav_trim_temp(
                   p_search_type          := str_search_type
                  ,p_fmeasure             := a.fmeasure
                  ,p_tmeasure             := a.tmeasure
                  ,p_lengthkm             := a.lengthkm
                  ,p_flowtimeday          := a.flowtimeday
                  ,p_network_distancekm   := a.network_distancekm
                  ,p_network_flowtimeday  := a.network_flowtimeday
                  ,p_maximum_distancekm   := num_maximum_distancekm
                  ,p_maximum_flowtimeday  := num_maximum_flowtimeday
               ) aa
            )
            WHERE
                a.selected IS TRUE
            AND a.network_flowtimeday > num_maximum_flowtimeday;

         END IF;
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 160
   -- Load the final table
   ----------------------------------------------------------------------------
   IF boo_return_flowline_details
   THEN
      INSERT INTO tmp_navigation_results(
          nhdplusid
         ,hydroseq
         ,fmeasure
         ,tmeasure
         ,levelpathi
         ,terminalpa
         ,uphydroseq
         ,dnhydroseq
         ,lengthkm
         ,flowtimeday
         /* ++++++++++ */
         ,network_distancekm
         ,network_flowtimeday
         /* ++++++++++ */
         ,permanent_identifier
         ,reachcode
         ,fcode
         ,gnis_id
         ,gnis_name
         ,wbarea_permanent_identifier
         /* ++++++++++ */
         ,navtermination_flag
         ,shape
         ,nav_order
      )
      SELECT
       a.nhdplusid
      ,a.hydroseq
      ,a.fmeasure
      ,a.tmeasure
      ,a.levelpathi
      ,a.terminalpa
      ,a.uphydroseq
      ,a.dnhydroseq
      ,a.lengthkm
      ,a.flowtimeday
      /* ++++++++++ */
      ,a.network_distancekm
      ,a.network_flowtimeday
      /* ++++++++++ */
      ,a.permanent_identifier
      ,a.reachcode
      ,a.fcode
      ,a.gnis_id
      ,a.gnis_name
      ,a.wbarea_permanent_identifier
      /* ++++++++++ */
      ,a.navtermination_flag
      ,a.shape
      ,a.nav_order
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.hydroseq
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.levelpathi
         ,aa.terminalpa
         ,aa.uphydroseq
         ,aa.dnhydroseq
         ,aa.lengthkm
         ,aa.flowtimeday
         /* ++++++++++ */
         ,aa.network_distancekm
         ,aa.network_flowtimeday
         /* ++++++++++ */
         ,bb.permanent_identifier
         ,bb.reachcode
         ,bb.fcode
         ,bb.gnis_id
         ,bb.gnis_name
         ,bb.wbarea_permanent_identifier
         /* ++++++++++ */
         ,aa.navtermination_flag
         ,CASE
          WHEN boo_return_flowline_geometry
          THEN
            CASE
            WHEN aa.fmeasure <> bb.fmeasure
            OR   aa.tmeasure <> bb.fmeasure
            THEN
               ST_GeometryN(
                   ST_LocateBetween(bb.shape,aa.fmeasure,aa.tmeasure)
                  ,1
               )
            ELSE
               bb.shape
            END
          ELSE
            NULL::GEOMETRY
          END AS shape
         ,aa.nav_order
         FROM
         tmp_navigation_working30 aa
         JOIN
         cipsrv_nhdplus_h.nhdflowline bb
         ON
         aa.nhdplusid = bb.nhdplusid
         WHERE
             aa.selected IS TRUE
         AND aa.fmeasure <> aa.tmeasure
         AND aa.fmeasure >= 0 AND aa.fmeasure <= 100
         AND aa.tmeasure >= 0 AND aa.tmeasure <= 100
         AND aa.lengthkm > 0
      ) a
      ORDER BY
       a.nav_order
      ,a.network_distancekm;
      
   ELSE
      INSERT INTO tmp_navigation_results(
          nhdplusid
         ,hydroseq
         ,fmeasure
         ,tmeasure
         ,levelpathi
         ,terminalpa
         ,uphydroseq
         ,dnhydroseq
         ,lengthkm
         ,flowtimeday
         ,network_distancekm
         ,network_flowtimeday
         ,navtermination_flag
         ,nav_order
      )
      SELECT
       a.nhdplusid
      ,a.hydroseq
      ,a.fmeasure
      ,a.tmeasure
      ,a.levelpathi
      ,a.terminalpa
      ,a.uphydroseq
      ,a.dnhydroseq
      ,a.lengthkm
      ,a.flowtimeday
      ,a.network_distancekm
      ,a.network_flowtimeday
      ,a.navtermination_flag
      ,a.nav_order
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.hydroseq
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.levelpathi
         ,aa.terminalpa
         ,aa.uphydroseq
         ,aa.dnhydroseq
         ,aa.lengthkm
         ,aa.flowtimeday
         ,aa.network_distancekm
         ,aa.network_flowtimeday
         ,aa.navtermination_flag
         ,aa.nav_order
         FROM
         tmp_navigation_working30 aa
         WHERE
             aa.selected IS TRUE
         AND aa.fmeasure <> aa.tmeasure
         AND aa.fmeasure >= 0 AND aa.fmeasure <= 100
         AND aa.tmeasure >= 0 AND aa.tmeasure <= 100
         AND aa.lengthkm > 0
      ) a
      ORDER BY
       a.nav_order
      ,a.network_distancekm;
   
   END IF;
   
   GET DIAGNOSTICS out_flowline_count = ROW_COUNT;
   
   IF out_flowline_count = 0
   THEN
      out_return_code    := -1;
      out_status_message := 'No results found.';
   
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.navigate(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.navigate(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
)  TO PUBLIC;

--******************************--
----- functions/point_at_measure.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.point_at_measure';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.point_at_measure(
    IN  p_nhdplusid               BIGINT
   ,IN  p_permanent_identifier    VARCHAR
   ,IN  p_reachcode               VARCHAR 
   ,IN  p_measure                 NUMERIC
   ,IN  p_2d_flag                 BOOLEAN DEFAULT TRUE
) RETURNS GEOMETRY
STABLE
AS
$BODY$ 
DECLARE
   sdo_output  GEOMETRY;
   boo_2d_flag BOOLEAN := p_2d_flag;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_measure IS NULL
   OR p_measure < -1
   OR p_measure > 101
   THEN
      RAISE EXCEPTION 'measure cannot be null and must be between -1 and 101';
      
   END IF;
   
   IF boo_2d_flag IS NULL
   THEN
      boo_2d_flag := TRUE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Grab the point if a normal measure
   ----------------------------------------------------------------------------
   IF p_measure >= 0 AND p_measure <= 100
   THEN
      IF p_nhdplusid IS NOT NULL
      THEN
         SELECT
         ST_GeometryN(ST_LocateAlong(a.shape,p_measure,0),1) AS shape
         INTO
         sdo_output
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
         a.nhdplusid = p_nhdplusid
         AND (
            p_measure = a.fmeasure
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
         );
      
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         SELECT
         ST_GeometryN(ST_LocateAlong(a.shape,p_measure,0),1) AS shape
         INTO
         sdo_output
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
         a.permanent_identifier = p_permanent_identifier
         AND (
            p_measure = a.fmeasure
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
         );
      
      ELSIF p_reachcode IS NOT NULL
      THEN
         SELECT
         ST_GeometryN(ST_LocateAlong(a.shape,p_measure,0),1) AS shape
         INTO
         sdo_output
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
         a.reachcode = p_reachcode
         AND (
            p_measure = a.fmeasure
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
         );
         
      ELSE
         RAISE EXCEPTION 'nhdplusid, permanent_identifier or reachcode required';
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Grab the start point if measure = 101
   ----------------------------------------------------------------------------
   IF p_measure = 101
   THEN
      IF p_nhdplusid IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,1)
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
         a.nhdplusid = p_nhdplusid;
      
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,1)
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
         a.permanent_identifier = p_permanent_identifier;
      
      ELSIF p_reachcode IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,1)
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
             a.reachcode = p_reachcode
         AND a.tmeasure = 100;
         
      ELSE
         RAISE EXCEPTION 'nhdplusid, permanent_identifier or reachcode required';
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Grab the end point if measure = -1
   ----------------------------------------------------------------------------
   IF p_measure = -1
   THEN
      IF p_nhdplusid IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,ST_NPoints(a.shape))
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
         a.nhdplusid = p_nhdplusid;
      
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,ST_NPoints(a.shape))
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
         a.permanent_identifier = p_permanent_identifier;
      
      ELSIF p_reachcode IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,ST_NPoints(a.shape))
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
             a.reachcode = p_reachcode
         AND a.fmeasure = 0;
         
      ELSE
         RAISE EXCEPTION 'nhdplusid, permanent_identifier or reachcode required';
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Return what we got
   ----------------------------------------------------------------------------
   IF boo_2d_flag
   THEN
      RETURN ST_Force2D(sdo_output);
      
   ELSE
      RETURN sdo_output;
   
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.point_at_measure(
    BIGINT
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.point_at_measure(
    BIGINT
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,BOOLEAN
) TO PUBLIC;

--******************************--
----- functions/pointindexing.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.pointindexing';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.pointindexing(
    IN  p_point                        GEOMETRY
   ,IN  p_indexing_engine              VARCHAR
   ,IN  p_fcode_allow                  INTEGER[]
   ,IN  p_fcode_deny                   INTEGER[]
   ,IN  p_distance_max_distkm          NUMERIC
   ,IN  p_raindrop_snap_max_distkm     NUMERIC
   ,IN  p_raindrop_path_max_distkm     NUMERIC
   ,IN  p_limit_innetwork              BOOLEAN
   ,IN  p_limit_navigable              BOOLEAN
   ,IN  p_fallback_fcode_allow         INTEGER[]
   ,IN  p_fallback_fcode_deny          INTEGER[]
   ,IN  p_fallback_distance_max_distkm NUMERIC
   ,IN  p_fallback_limit_innetwork     BOOLEAN
   ,IN  p_fallback_limit_navigable     BOOLEAN
   ,IN  p_return_link_path             BOOLEAN
   ,IN  p_known_region                 VARCHAR
   ,IN  p_known_catchment_nhdplusid    BIGINT   DEFAULT NULL
   ,OUT out_flowlines                  cipsrv_nhdplus_h.snapflowline[]
   ,OUT out_path_distance_km           NUMERIC
   ,OUT out_end_point                  GEOMETRY
   ,OUT out_indexing_line              GEOMETRY
   ,OUT out_region                     VARCHAR
   ,OUT out_nhdplusid                  BIGINT
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                 RECORD;
   str_indexing_engine VARCHAR(4000) := UPPER(p_indexing_engine);
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   -------------------------------------------------------------------------
   -- Step 20
   -- Check distance engines first
   -------------------------------------------------------------------------
   IF str_indexing_engine IN ('DISTANCE')
   THEN
      rec := cipsrv_nhdplus_h.distance_index(
          p_point                     => p_point
         ,p_fcode_allow               => p_fcode_allow
         ,p_fcode_deny                => p_fcode_deny
         ,p_distance_max_distkm       => p_distance_max_distkm
         ,p_limit_innetwork           => p_limit_innetwork
         ,p_limit_navigable           => p_limit_navigable
         ,p_return_link_path          => p_return_link_path
         ,p_known_region              => p_known_region
      );
     
   -------------------------------------------------------------------------
   -- Step 30
   -- Check cat constrained 
   -------------------------------------------------------------------------
   ELSIF str_indexing_engine IN ('CATCONSTRAINED')
   THEN
      rec := cipsrv_nhdplus_h.catconstrained_index(
          p_point                     => p_point
         ,p_return_link_path          => p_return_link_path
         ,p_known_region              => p_known_region
         ,p_known_catchment_nhdplusid => p_known_catchment_nhdplusid
      );

   -------------------------------------------------------------------------
   -- Step 40
   -- Check raindrop
   -------------------------------------------------------------------------
   ELSIF str_indexing_engine = 'RAINDROP'
   THEN
      rec := cipsrv_nhdplus_h.raindrop_index(
          p_point                     => p_point
         ,p_fcode_allow               => p_fcode_allow
         ,p_fcode_deny                => p_fcode_deny
         ,p_raindrop_snap_max_distkm  => p_raindrop_snap_max_distkm
         ,p_raindrop_path_max_distkm  => p_raindrop_path_max_distkm
         ,p_limit_innetwork           => p_limit_innetwork
         ,p_limit_navigable           => p_limit_navigable
         ,p_return_link_path          => p_return_link_path
         ,p_known_region              => p_known_region
      );
      
   ELSE
      out_return_code    := -1;
      out_status_message := 'Unknown indexing engine type.';
      RETURN;
      
   END IF;
   
   -------------------------------------------------------------------------
   -- Step 50
   -- Return results
   -------------------------------------------------------------------------
   out_return_code      := rec.out_return_code;
   out_status_message   := rec.out_status_message;
   
   out_flowlines        := rec.out_flowlines;
   out_path_distance_km := rec.out_path_distance_km;
   out_end_point        := rec.out_end_point;
   out_indexing_line    := rec.out_indexing_line;
   out_region           := rec.out_region;
   out_nhdplusid        := rec.out_nhdplusid;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.pointindexing(
    GEOMETRY
   ,VARCHAR
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,BIGINT
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.pointindexing(
    GEOMETRY
   ,VARCHAR
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,BIGINT
) TO PUBLIC;
--******************************--
----- functions/query_generic_common_mbr.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.query_generic_common_mbr(
   IN  p_input  GEOMETRY
) RETURNS VARCHAR
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_point GEOGRAPHY;
   
BEGIN

   IF p_input IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   sdo_point := ST_Transform(
       ST_PointOnSurface(p_input)
      ,4326
   )::GEOGRAPHY;
   
   IF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('CONUS')
   )
   THEN
      RETURN 'CONUS';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('HI')
   )
   THEN
      RETURN 'HI';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('PRVI')
   )
   THEN
      RETURN 'PRVI';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('AK')
   )
   THEN
      RETURN 'AK';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('GUMP')
   )
   THEN
      RETURN 'GUMP';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_h.generic_common_mbr('SAMOA')
   )
   THEN
      RETURN 'SAMOA';
      
   END IF;
   
   RETURN NULL;  
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.query_generic_common_mbr(
   GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.query_generic_common_mbr(
   GEOMETRY
) TO PUBLIC;

--******************************--
----- functions/raindrop_coord_to_raster.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.raindrop_coord_to_raster';
   IF b IS NOT NULL THEN
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.raindrop_coord_to_raster(
    IN    p_column_x              INTEGER
   ,IN    p_row_y                 INTEGER
   ,IN    p_region                VARCHAR
   ,OUT   out_rid                 INTEGER
   ,OUT   out_raster              RASTER
   ,OUT   out_offset_x            INTEGER
   ,OUT   out_offset_y            INTEGER
   ,OUT   out_return_code         INTEGER
   ,OUT   out_status_message      VARCHAR
) 
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF p_column_x IS NULL
   OR p_row_y IS NULL
   THEN
      RETURN;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 20
   -- Query the grid as needed
   --------------------------------------------------------------------------
   IF UPPER(p_region) IN ('CONUS','5070')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_5070_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSIF UPPER(p_region) IN ('AK','3338')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_3338_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSIF UPPER(p_region) IN ('HI','26904')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_26904_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSIF UPPER(p_region) IN ('PRVI','32161')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_32161_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSIF UPPER(p_region) IN ('GUMP','32655')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_32655_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSIF UPPER(p_region) IN ('SAMOA','32702')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_32702_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 30
   -- Check output is okay
   --------------------------------------------------------------------------
   IF p_column_x IS NULL
   OR p_row_y IS NULL
   THEN
      out_return_code    := -201;
      out_status_message := 'Unable to obtain X,Y location from input point on ' 
                     || p_region || ' raster grid';
      RETURN;
      
   END IF;

EXCEPTION
   WHEN NO_DATA_FOUND
   THEN
      out_return_code    := -201;
      out_status_message := 'Unable to obtain X,Y location from input point on ' 
                     || p_region || ' raster grid';
      RETURN;
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.raindrop_coord_to_raster(
    INTEGER
   ,INTEGER
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.raindrop_coord_to_raster(
    INTEGER
   ,INTEGER
   ,VARCHAR
) TO PUBLIC;
--******************************--
----- functions/raindrop_index.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.raindrop_index';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.raindrop_index(
    IN  p_point                     GEOMETRY
   ,IN  p_fcode_allow               INTEGER[]
   ,IN  p_fcode_deny                INTEGER[]
   ,IN  p_raindrop_snap_max_distkm  NUMERIC
   ,IN  p_raindrop_path_max_distkm  NUMERIC
   ,IN  p_limit_innetwork           BOOLEAN
   ,IN  p_limit_navigable           BOOLEAN
   ,IN  p_return_link_path          BOOLEAN
   ,IN  p_known_region              VARCHAR
   ,OUT out_flowlines               cipsrv_nhdplus_h.snapflowline[]
   ,OUT out_path_distance_km        NUMERIC
   ,OUT out_end_point               GEOMETRY
   ,OUT out_indexing_line           GEOMETRY
   ,OUT out_region                  VARCHAR
   ,OUT out_nhdplusid               BIGINT
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                          RECORD;
   num_raindrop_snap_max_distkm NUMERIC  := p_raindrop_snap_max_distkm;
   num_raindrop_path_max_distkm NUMERIC  := p_raindrop_path_max_distkm;
   boo_limit_innetwork          BOOLEAN  := p_limit_innetwork;
   boo_limit_navigable          BOOLEAN  := p_limit_navigable;
   boo_return_link_path         BOOLEAN  := p_return_link_path;
   num_cell_width_km            NUMERIC;
   num_cell_diagonal            NUMERIC;
   int_raster_srid              INTEGER;
   rec_flowline                 RECORD;
   sdo_temporary                GEOMETRY;
   l_point                      GEOMETRY;
   l_nearest_flowline_dist_km   NUMERIC := 0;
   l_traveled_distance_km       NUMERIC := 0;
   l_distance_tmp_km            NUMERIC := 0;
   l_permanent_identifier       BIGINT;
   l_raster                     RASTER;
   l_raster_rid                 INTEGER;
   l_columnX                    INTEGER;
   l_rowY                       INTEGER;
   l_offsetX                    INTEGER;
   l_offsetY                    INTEGER;
   l_current_direction          INTEGER := -1;
   l_last_direction             INTEGER := -2;
   l_before_last_direction      INTEGER := -3;
   l_last_point                 GEOMETRY;
   l_temp_point                 GEOMETRY;
   obj_flowline                 cipsrv_nhdplus_h.snapflowline;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF p_point IS NULL OR ST_IsEmpty(p_point) 
   OR ST_GeometryType(p_point) <> 'ST_Point'
   THEN
      out_return_code    := -99;
      out_status_message := 'Input must be point geometry.';
      RETURN;
      
   END IF;
   
   IF num_raindrop_snap_max_distkm IS NULL
   OR num_raindrop_snap_max_distkm <= 0
   THEN
      num_raindrop_snap_max_distkm := 2;
      
   END IF;
   
   IF boo_limit_innetwork IS NULL
   THEN
      boo_limit_innetwork := FALSE;
      
   END IF;
   
   IF boo_limit_navigable IS NULL
   THEN
      boo_limit_navigable := FALSE;
      
   END IF;
   
   IF boo_return_link_path IS NULL
   THEN
      boo_return_link_path := FALSE;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Bail if rasters are not installed
   --------------------------------------------------------------------------
   IF NOT cipsrv_engine.table_exists(
       p_schema_name := 'cipsrv_nhdplusgrid_h'
      ,p_table_name  := 'fdr_5070_rdt'
   )
   THEN
      out_return_code    := -2;
      out_status_message := 'NHDPlus h raster data not installed.';
      RETURN;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   num_cell_width_km  := rec.out_grid_size / 1000;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   out_region         := rec.out_srid::VARCHAR;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
     
   --------------------------------------------------------------------------
   -- Step 40
   -- Project input point if required
   --------------------------------------------------------------------------
   IF ST_SRID(p_point) = int_raster_srid
   THEN
      l_point := p_point;
      
   ELSE
      l_point := ST_Transform(p_point,int_raster_srid);
      
   END IF;
   
   num_cell_diagonal := num_cell_width_km * SQRT(2);
   
   IF num_raindrop_path_max_distkm IS NULL
   THEN
      num_raindrop_path_max_distkm := num_cell_diagonal;
      
   END IF;
   
   IF boo_return_link_path
   THEN
      out_indexing_line := l_point;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 50
   -- Check if we are not already on top of the result
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.distance_index(
       p_point               := l_point
      ,p_fcode_allow         := p_fcode_allow
      ,p_fcode_deny          := p_fcode_deny
      ,p_distance_max_distkm := num_cell_diagonal
      ,p_limit_innetwork     := boo_limit_innetwork
      ,p_limit_navigable     := boo_limit_navigable
      ,p_return_link_path    := boo_return_link_path
      ,p_known_region        := int_raster_srid::VARCHAR
   );

   IF  rec.out_return_code = 0
   AND rec.out_path_distance_km < num_cell_width_km
   THEN
      out_flowlines        := rec.out_flowlines;
      out_path_distance_km := rec.out_path_distance_km;
      out_end_point        := rec.out_end_point;
      out_indexing_line    := rec.out_indexing_line;
      out_return_code      := rec.out_return_code;
      out_status_message   := rec.out_status_message;
      RETURN;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Keeps track of total distance
   --------------------------------------------------------------------------
   sdo_temporary := l_point;
   
   rec := cipsrv_nhdplus_h.raindrop_world_to_raster(
       p_point        := l_point
      ,p_known_region := int_raster_srid::VARCHAR
      ,p_preprojected := TRUE
   );
   
   IF rec.out_return_code <> 0
   THEN
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      RETURN;
   
   END IF;
   
   l_raster     := rec.out_raster;
   l_raster_rid := rec.out_rid;
   l_columnX    := rec.out_column_x;
   l_rowY       := rec.out_row_y;
   l_offsetX    := rec.out_offset_x;
   l_offsetY    := rec.out_offset_y;
   
   l_point := cipsrv_nhdplus_h.raindrop_st_pixelascentroid(
       p_raster    := l_raster
      ,p_column_x  := l_columnX
      ,p_row_y     := l_rowY 
      ,p_offset_x  := l_offsetX
      ,p_offset_y  := l_offsetY
   );
   
   out_indexing_line := ST_MakeLine(
       geom1  := out_indexing_line
      ,geom2  := l_point
   );
   
   -- Be certain the grid srid is meter based!
   out_path_distance_km := ST_Distance(
       sdo_temporary
      ,l_point
   ) / 1000;

   --------------------------------------------------------------------------
   -- Step 70
   -- Primary Loop to traverse the flow direction grid
   --------------------------------------------------------------------------
   LOOP

      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 70.10
      -- Determine distance to nearest qualifying NHDPlus flowline
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      rec := cipsrv_nhdplus_h.distance_index_simple(
          p_point               := l_point
         ,p_fcode_allow         := p_fcode_allow
         ,p_fcode_deny          := p_fcode_deny
         ,p_distance_max_distkm := num_raindrop_snap_max_distkm + num_raindrop_path_max_distkm
         ,p_limit_innetwork     := boo_limit_innetwork
         ,p_limit_navigable     := boo_limit_navigable
         ,p_known_region        := int_raster_srid::VARCHAR
      );

      IF rec.out_return_code <> 0
      THEN
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         
         IF out_indexing_line IS NOT NULL
         THEN
            out_indexing_line := ST_Transform(out_indexing_line,4269);
         
         END IF;
               
         RETURN;
      
      END IF;

      l_nearest_flowline_dist_km := rec.out_path_distance_km;
      l_permanent_identifier     := rec.out_nhdplusid;
      --RAISE WARNING '% %', l_nearest_flowline_dist_km, l_permanent_identifier;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.20
      -- Continue checking and traversing until these conditions are met:
      --   * within the proximity value specified
      --   * traversed the maximum number of meters specified
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      --RAISE WARNING '% % % % %', l_nearest_flowline_dist_km, num_cell_diagonal, out_path_distance_km, num_raindrop_path_max_distkm, l_permanent_identifier;
      
      EXIT WHEN l_nearest_flowline_dist_km IS NULL
      OR l_nearest_flowline_dist_km  <= num_cell_diagonal
      OR out_path_distance_km        >  num_raindrop_path_max_distkm;

      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.30
      -- If no raster current then fetch one
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      IF l_raster IS NULL
      THEN
         rec := cipsrv_nhdplus_h.raindrop_world_to_raster(
             p_point        := l_point
            ,p_known_region := int_raster_srid::VARCHAR
            ,p_preprojected := TRUE
         );
         --RAISE WARNING '% %', r.pReturnCode, r.pStatusMessage;
         
         IF rec.out_return_code <> 0
         THEN
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
            IF out_indexing_line IS NOT NULL
            THEN
               out_indexing_line := ST_Transform(out_indexing_line,4269);
            
            END IF;
               
            RETURN;
         
         END IF;
         
         l_raster     := rec.out_raster;
         l_raster_rid := rec.out_rid;
         l_columnX    := rec.out_column_x;
         l_rowY       := rec.out_row_y;
         l_offsetX    := rec.out_offset_x;
         l_offsetY    := rec.out_offset_y;
      
      END IF;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.40
      -- Keeps track of single cell travel distance
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      l_traveled_distance_km := 0;

      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.50
      -- Keeps track of distance traveled this loop only so we know when to
      -- stop and check for the nearest flowline again.
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      l_distance_tmp_km := 0;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.60
      -- Walk the grid
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      WHILE l_distance_tmp_km * 1.7 <= l_nearest_flowline_dist_km
      LOOP

         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         -- Step 60.60.10
         -- Take a step on the raster
         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         rec := cipsrv_nhdplus_h.raindrop_next_coordinate(
             p_raster        := l_raster
            ,p_grid_size_km  := num_cell_width_km
            ,p_offset_x      := l_offsetX
            ,p_offset_y      := l_offsetY
            ,inout_column_x  := l_columnX
            ,inout_row_y     := l_rowY
         );
         
         IF rec.out_return_code <> 0
         THEN
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
            IF out_indexing_line IS NOT NULL
            THEN
               out_indexing_line  := ST_Transform(out_indexing_line,4269);
            
            END IF;
               
            RETURN;
         
         END IF;

         l_columnX              := rec.inout_column_x;
         l_rowY                 := rec.inout_row_y;
         l_traveled_distance_km := rec.out_distance_km;
         l_current_direction    := rec.out_direction;
         l_distance_tmp_km      := l_distance_tmp_km  + l_traveled_distance_km;
         out_path_distance_km   := out_path_distance_km + l_traveled_distance_km;

         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         -- Step 60.60.20
         -- Check if we have left the current raster
         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         IF l_columnX <  l_offsetX
         OR l_rowY    <  l_offsetY
         OR l_columnX >= l_offsetX + ST_Width(l_raster)
         OR l_rowY    >= l_offsetY + ST_Height(l_raster)
         THEN
            rec := cipsrv_nhdplus_h.raindrop_coord_to_raster(
                p_column_x := l_columnX
               ,p_row_y    := l_rowY
               ,p_region   := int_raster_srid::VARCHAR
            );
            
            IF rec.out_return_code <> 0
            THEN
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
               IF out_indexing_line IS NOT NULL
               THEN
                  out_indexing_line  := ST_Transform(out_indexing_line,4269);
               
               END IF;
               
               RETURN;
            
            END IF;
            
            l_raster_rid := rec.out_rid;
            l_raster     := rec.out_raster;
            l_offsetX    := rec.out_offset_x;
            l_offsetY    := rec.out_offset_y;
         
         END IF;

         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         -- Step 60.60.30
         -- Capture the cell point if so required
         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         IF boo_return_link_path
         THEN
            l_point := cipsrv_nhdplus_h.raindrop_st_pixelascentroid(
                p_raster     := l_raster
               ,p_column_x   := l_columnX
               ,p_row_y      := l_rowY 
               ,p_offset_x   := l_offsetX
               ,p_offset_y   := l_offsetY
            );
            
            IF l_current_direction <> l_last_direction
            THEN
               IF l_last_direction = l_before_last_direction
               THEN
                  out_indexing_line := ST_MakeLine(
                      geom1  := out_indexing_line
                     ,geom2  := l_last_point
                  );
                  --RAISE WARNING '%', ST_AsText(ST_Transform(out_indexing_line,4269));
                  
               END IF;
               
               out_indexing_line := ST_MakeLine(
                   geom1  := out_indexing_line
                  ,geom2  := l_point
               );
               
            END IF;
            
            l_before_last_direction := l_last_direction;
            l_last_direction        := l_current_direction;
            l_last_point            := l_point;
         
         END IF;
         
      END LOOP;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.70
      -- Convert location back to point 
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      l_point := cipsrv_nhdplus_h.raindrop_st_pixelascentroid(
          p_raster     := l_raster
         ,p_column_x   := l_columnX
         ,p_row_y      := l_rowY 
         ,p_offset_x   := l_offsetX
         ,p_offset_y   := l_offsetY
      );

   END LOOP;
   --RAISE WARNING '%',l_nearest_flowline_dist_km;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Check if the path has wandered away from eligible flowlines
   --------------------------------------------------------------------------
   IF l_nearest_flowline_dist_km IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'No results found';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 80
   -- Get the closest eligible flowline that matches snap distance
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.distance_index(
       p_point               := l_point
      ,p_fcode_allow         := p_fcode_allow
      ,p_fcode_deny          := p_fcode_deny
      ,p_distance_max_distkm := num_raindrop_snap_max_distkm
      ,p_limit_innetwork     := boo_limit_innetwork
      ,p_limit_navigable     := boo_limit_navigable
      ,p_return_link_path    := boo_return_link_path
      ,p_known_region        := int_raster_srid::VARCHAR
   );

   IF rec.out_return_code <> 0
   THEN
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      RETURN;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 90
   -- To avoid overshoot, recheck the distance using center of last segment
   --------------------------------------------------------------------------
   
   --------------------------------------------------------------------------
   -- Step 100
   -- Load the output array
   --------------------------------------------------------------------------
   IF boo_return_link_path
   THEN
      out_indexing_line := ST_MakeLine(
          geom1  := out_indexing_line
         ,geom2  := l_point
      );
      --RAISE WARNING '%', ST_AsText(ST_Transform(out_indexing_line,4269));
      
   END IF;
   
   out_flowlines := rec.out_flowlines;

   FOR i IN 1 .. array_length(out_flowlines,1)
   LOOP
      obj_flowline := out_flowlines[i];
      obj_flowline.snap_distancekm := obj_flowline.snap_distancekm + out_path_distance_km;
      out_flowlines[i] := obj_flowline;

   END LOOP;
   
   --------------------------------------------------------------------------
   -- Step 110
   -- Clean up the output products
   --------------------------------------------------------------------------
   out_end_point        := rec.out_end_point;
   sdo_temporary        := ST_Transform(out_end_point,int_raster_srid);
   out_path_distance_km := out_path_distance_km + ST_Distance(
       l_point
      ,sdo_temporary
   ) / 1000;
   out_nhdplusid        := out_flowlines[1].nhdplusid;
   
   IF boo_return_link_path
   AND out_path_distance_km > 0.00005
   THEN
      out_indexing_line := ST_MakeLine(
          geom1  := out_indexing_line
         ,geom2  := sdo_temporary
      );
      
      out_indexing_line := ST_Transform(out_indexing_line,4269);

   ELSE
      out_indexing_line := NULL;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.raindrop_index(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.raindrop_index(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) TO PUBLIC;
--******************************--
----- functions/raindrop_next_coordinate.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.raindrop_next_coordinate';
   IF b IS NOT NULL THEN
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.raindrop_next_coordinate(
    IN    p_raster                 RASTER
   ,IN    p_grid_size_km           NUMERIC
   ,IN    p_offset_x               INTEGER
   ,IN    p_offset_y               INTEGER
   ,INOUT inout_column_x           INTEGER
   ,INOUT inout_row_y              INTEGER
   ,OUT   out_distance_km          NUMERIC
   ,OUT   out_direction            INTEGER
   ,OUT   out_return_code          INTEGER
   ,OUT   out_status_message       VARCHAR
) 
IMMUTABLE
AS $BODY$ 
DECLARE
   num_corner_km NUMERIC;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   num_corner_km := p_grid_size_km * SQRT(2);

   --------------------------------------------------------------------------
   -- Step 20
   -- Get the raster value
   --------------------------------------------------------------------------
   out_direction := cipsrv_nhdplus_h.raindrop_st_value(
       p_raster    := p_raster
      ,p_column_x  := inout_column_x
      ,p_row_y     := inout_row_y
      ,p_offset_x  := p_offset_x
      ,p_offset_y  := p_offset_y
   );

   --------------------------------------------------------------------------
   -- Step 30
   -- Walk the grid
   --------------------------------------------------------------------------
   CASE out_direction
   WHEN 1
   THEN
      inout_column_x  := inout_column_x + 1;
      out_distance_km := p_grid_size_km;
      
   WHEN 2
   THEN
      inout_column_x  := inout_column_x + 1;
      inout_row_y     := inout_row_y    + 1;
      out_distance_km := num_corner_km;
      
   WHEN 4
   THEN
      inout_row_y     := inout_row_y    + 1;
      out_distance_km := p_grid_size_km;
      
   WHEN 8
   THEN
      inout_column_x  := inout_column_x - 1;
      inout_row_y     := inout_row_y    + 1;
      out_distance_km := num_corner_km;
      
   WHEN 16
   THEN
      inout_column_x  := inout_column_x - 1;
      out_distance_km := p_grid_size_km;
      
   WHEN 32
   THEN
      inout_column_x  := inout_column_x - 1;
      inout_row_y     := inout_row_y    - 1;
      out_distance_km := num_corner_km;
      
   WHEN 64
   THEN
      inout_row_y     := inout_row_y    - 1;
      out_distance_km := p_grid_size_km;
      
   WHEN 128
   THEN
      inout_column_x  := inout_column_x + 1;
      inout_row_y     := inout_row_y    - 1;
      out_distance_km := num_corner_km;
      
   WHEN 0
   THEN
      out_return_code    := -20011;
      out_status_message := 'Flow ends at sink';
      RETURN;
      
   WHEN 255
   THEN
      out_return_code    := -20010;
      out_status_message := 'Flow Direction Grid Out Of Bounds';
      RETURN;
      
   ELSE
      out_return_code    := -20010;
      out_status_message := 'Flow Direction Grid Out Of Bounds';
      RETURN;
      
   END CASE;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.raindrop_next_coordinate(
    RASTER
   ,NUMERIC
   ,INTEGER
   ,INTEGER
   ,INTEGER
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.raindrop_next_coordinate(
    RASTER
   ,NUMERIC
   ,INTEGER
   ,INTEGER
   ,INTEGER
   ,INTEGER
) TO PUBLIC;
--******************************--
----- functions/raindrop_st_pixelascentroid.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.raindrop_st_pixelascentroid';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.raindrop_st_pixelascentroid(
    IN  p_raster                 RASTER
   ,IN  p_column_x               INTEGER
   ,IN  p_row_y                  INTEGER
   ,IN  p_offset_x               INTEGER
   ,IN  p_offset_y               INTEGER
) RETURNS GEOMETRY
IMMUTABLE
AS $BODY$ 
DECLARE 
BEGIN

   RETURN ST_PixelAsCentroid(
       rast    := p_raster
      ,x       := p_column_x - p_offset_x + 1
      ,y       := p_row_y    - p_offset_y + 1
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.raindrop_st_pixelascentroid(
    RASTER
   ,INTEGER
   ,INTEGER
   ,INTEGER
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.raindrop_st_pixelascentroid(
    RASTER
   ,INTEGER
   ,INTEGER
   ,INTEGER
   ,INTEGER
) TO PUBLIC;
--******************************--
----- functions/raindrop_st_value.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.raindrop_st_value';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.raindrop_st_value(
    IN  p_raster                 RASTER
   ,IN  p_column_x               INTEGER
   ,IN  p_row_y                  INTEGER
   ,IN  p_offset_x               INTEGER
   ,IN  p_offset_y               INTEGER
) RETURNS INTEGER
IMMUTABLE
AS $BODY$ 
DECLARE 
BEGIN

   RETURN ST_Value(
       rast     := p_raster
      ,band     := 1
      ,x        := p_column_x - p_offset_x + 1
      ,y        := p_row_y    - p_offset_y + 1
      ,exclude_nodata_value := true
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.raindrop_st_value(
    RASTER
   ,INTEGER
   ,INTEGER
   ,INTEGER
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.raindrop_st_value(
    RASTER
   ,INTEGER
   ,INTEGER
   ,INTEGER
   ,INTEGER
) TO PUBLIC;
--******************************--
----- functions/raindrop_world_to_raster.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.raindrop_world_to_raster';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.raindrop_world_to_raster(
    IN  p_point                  GEOMETRY
   ,IN  p_known_region           VARCHAR
   ,IN  p_preprojected           BOOLEAN DEFAULT FALSE
   ,OUT out_column_x             INTEGER
   ,OUT out_row_y                INTEGER
   ,OUT out_rid                  INTEGER
   ,OUT out_raster               RASTER
   ,OUT out_offset_x             INTEGER
   ,OUT out_offset_y             INTEGER
   ,OUT out_return_code          INTEGER
   ,OUT out_status_message       VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                     RECORD;
   int_raster_srid         INTEGER;
   sdo_input               GEOMETRY;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF p_point IS NULL OR ST_IsEmpty(p_point) 
   OR ST_GeometryType(p_point) <> 'ST_Point'
   THEN
      out_return_code    := -99;
      out_status_message := 'Input must be point geometry.';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
     
   --------------------------------------------------------------------------
   -- Step 30
   -- Project input point if required
   --------------------------------------------------------------------------
   IF p_preprojected
   OR ST_SRID(p_point) = int_raster_srid
   THEN
      sdo_input := p_point;
      
   ELSE
      sdo_input := ST_Transform(p_point,int_raster_srid);
      
   END IF;

   -------------------------------------------------------------------------
   -- Step 40
   -- Query the grid as needed
   --------------------------------------------------------------------------
   IF int_raster_srid = 5070
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_5070_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   ELSIF int_raster_srid = 3338
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_3338_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   ELSIF int_raster_srid = 26904
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_26904_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   ELSIF int_raster_srid = 32161
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y 
      FROM
      cipsrv_nhdplusgrid_h.fdr_32161_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   ELSIF int_raster_srid = 32655
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y 
      FROM
      cipsrv_nhdplusgrid_h.fdr_32655_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   ELSIF int_raster_srid = 32702
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_32702_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Check output is okay
   --------------------------------------------------------------------------
   IF out_column_x IS NULL
   OR out_row_y IS NULL
   THEN
      out_return_code    := -201;
      out_status_message := 'Unable to obtain X,Y location from input point on ' 
                     || int_raster_srid || ' raster grid';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Add the offsets
   --------------------------------------------------------------------------
   out_column_x := out_column_x + out_offset_x - 1;
   out_row_y    := out_row_y    + out_offset_y - 1;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.raindrop_world_to_raster(
    GEOMETRY
   ,VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.raindrop_world_to_raster(
    GEOMETRY
   ,VARCHAR
   ,BOOLEAN
) TO PUBLIC;
--******************************--
----- functions/randomcatchment.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.randomcatchment';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.randomcatchment(
    IN  p_region                VARCHAR DEFAULT NULL
   ,IN  p_include_extended      BOOLEAN DEFAULT FALSE
   ,IN  p_return_geometry       BOOLEAN DEFAULT FALSE
   ,OUT out_nhdplusid           BIGINT
   ,OUT out_areasqkm            NUMERIC
   ,OUT out_catchmentstatecodes VARCHAR[]
   ,OUT out_shape               GEOMETRY
   ,OUT out_centroid            GEOMETRY
   ,OUT out_return_code         INTEGER
   ,OUT out_status_message      VARCHAR
)
STABLE
AS $BODY$
DECLARE
   boo_search      BOOLEAN;
   int_sanity      INTEGER;
   num_big_samp    NUMERIC := 0.001;
   str_statecode   VARCHAR;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Select a random navigable flowline
   --------------------------------------------------------------------------
   boo_search := TRUE;
   int_sanity := 0;
   
   WHILE boo_search
   LOOP
      IF p_region IN ('CONUS','5070')
      THEN
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isocean
            FROM
            cipsrv_nhdplus_h.catchment_5070 aa
            TABLESAMPLE SYSTEM(num_big_samp)
         ) a
         WHERE 
         p_include_extended OR NOT a.isocean
         ORDER BY RANDOM()
         LIMIT 1;
               
      ELSIF p_region IN ('ALASKA','AK','3338')
      THEN     
         IF NOT p_include_extended
         THEN
            out_status_message := 'Alaska is entirely extended H3 catchments.';
            RETURN;
            
         END IF;
         
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isalaskan
            FROM
            cipsrv_nhdplus_h.catchment_3338 aa
            TABLESAMPLE SYSTEM(num_big_samp)
         ) a
         WHERE 
         p_include_extended OR NOT a.isalaskan
         ORDER BY RANDOM()
         LIMIT 1;
               
      ELSIF p_region IN ('HAWAII','HI','26904')
      THEN
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isocean
            FROM
            cipsrv_nhdplus_h.catchment_26904 aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         WHERE 
         p_include_extended OR NOT a.isocean
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('PRVI','32161')
      THEN
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isocean
            FROM
            cipsrv_nhdplus_h.catchment_32161 aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         WHERE 
         p_include_extended OR NOT a.isocean
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('GUAMMAR','GUMP','32655')
      THEN         
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isocean
            FROM
            cipsrv_nhdplus_h.catchment_32655 aa
            TABLESAMPLE SYSTEM(1)
         ) a
         WHERE 
         p_include_extended OR NOT a.isocean
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('AMSAMOA','AS','32702')
      THEN
         SELECT
          a.nhdplusid
         ,a.catchmentstatecodes[1]
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecodes
            ,aa.isocean
            FROM
            cipsrv_nhdplus_h.catchment_32702 aa
            TABLESAMPLE SYSTEM(1)
         ) a
         WHERE 
         p_include_extended OR NOT a.isocean
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSE
         SELECT
          a.nhdplusid
         ,a.catchmentstatecode
         INTO
          out_nhdplusid
         ,str_statecode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.catchmentstatecode
            ,aa.isocean
            ,aa.isalaskan
            FROM
            cipsrv_nhdplus_h.catchment_fabric aa
            TABLESAMPLE SYSTEM(num_big_samp)
         ) a
         WHERE 
         p_include_extended OR (a.isocean = 'N' AND a.isalaskan = 'N')
         ORDER BY RANDOM()
         LIMIT 1;
      
      END IF;
      
      IF out_nhdplusid IS NOT NULL
      THEN
         boo_search := FALSE;
         
      END IF;
      
      int_sanity := int_sanity + 1;
      IF int_sanity > 10
      THEN
         num_big_samp := num_big_samp * 2;
         
      END IF;
      
      IF int_sanity > 25
      THEN
         out_return_code := -9;
         out_status_message := 'Unable to sample ' || p_region || ' via ' || num_big_samp::VARCHAR;
         RETURN;
         
      END IF;
      
   END LOOP;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Get the catchment details
   --------------------------------------------------------------------------
   IF str_statecode NOT IN ('AK','HI','PR','VI','GU','MP','AS')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN p_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN p_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_5070_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSIF str_statecode IN ('AK')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN p_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN p_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_3338_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSIF str_statecode IN ('HI')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN p_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN p_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_26904_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSIF str_statecode IN ('PR','VI')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN p_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN p_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_32161_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSIF str_statecode IN ('GU','MP')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN p_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN p_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_32655_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSIF str_statecode IN ('AS')
   THEN
      SELECT
       a.areasqkm
      ,a.catchmentstatecodes
      ,CASE WHEN p_return_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
      ,CASE WHEN p_return_geometry THEN ST_PointOnSurface(a.shape) ELSE NULL::GEOMETRY END AS shape_centroid
      INTO
       out_areasqkm
      ,out_catchmentstatecodes
      ,out_shape
      ,out_centroid
      FROM
      cipsrv_nhdplus_h.catchment_32702_full a
      WHERE
      a.nhdplusid = out_nhdplusid;
      
   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.randomcatchment(
    VARCHAR
   ,BOOLEAN
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.randomcatchment(
    VARCHAR
   ,BOOLEAN
   ,BOOLEAN
) TO PUBLIC;

--******************************--
----- functions/randomnav.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.randomnav';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.randomnav(
    IN  p_region               VARCHAR DEFAULT NULL
   ,IN  p_return_geometry      BOOLEAN DEFAULT FALSE
   ,OUT out_nhdplusid          BIGINT
   ,OUT out_reachcode          VARCHAR
   ,OUT out_measure            NUMERIC
   ,OUT out_shape              GEOMETRY
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   num_fmeasure    NUMERIC;
   num_tmeasure    NUMERIC;
   num_random      NUMERIC;
   boo_search      BOOLEAN;
   int_sanity      INTEGER;
   num_big_samp    NUMERIC := 0.001;
   sdo_flowline    GEOMETRY;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Select a random navigable flowline
   --------------------------------------------------------------------------
   boo_search := TRUE;
   int_sanity := 0;
   
   WHILE boo_search
   LOOP
      IF p_region IN ('CONUS','5070')
      THEN
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.isnavigable
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline_5070 aa
            TABLESAMPLE SYSTEM(num_big_samp)
         ) a
         WHERE 
         a.isnavigable
         ORDER BY RANDOM()
         LIMIT 1;
            
      ELSIF p_region IN ('HAWAII','HI','26904')
      THEN
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.isnavigable
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline_26904 aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         WHERE 
         a.isnavigable
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('PRVI','32161')
      THEN
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.isnavigable
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline_32161 aa
            TABLESAMPLE SYSTEM(0.1)
         ) a
         WHERE 
         a.isnavigable
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('GUAMMAR','GUMP','32655')
      THEN
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.isnavigable
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline_32655 aa
            TABLESAMPLE SYSTEM(1)
         ) a
         WHERE 
         a.isnavigable
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSIF p_region IN ('AMSAMOA','AS','32702')
      THEN
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.isnavigable
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline_32702 aa
            TABLESAMPLE SYSTEM(1)
         ) a
         WHERE 
         a.isnavigable = 'Y'
         ORDER BY RANDOM()
         LIMIT 1;
         
      ELSE
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.shape
         INTO
          out_nhdplusid
         ,out_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,sdo_flowline
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.reachcode
            ,aa.fmeasure
            ,aa.tmeasure
            ,aa.fcode
            ,CASE WHEN p_return_geometry THEN aa.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.nhdflowline aa
            TABLESAMPLE SYSTEM(num_big_samp)
         ) a
         JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE 
         a.fcode NOT IN (56600)
         ORDER BY RANDOM()
         LIMIT 1;
      
      END IF;
      
      IF out_nhdplusid IS NOT NULL
      THEN
         boo_search := FALSE;
         
      END IF;
      
      int_sanity := int_sanity + 1;
      IF int_sanity > 10
      THEN
         num_big_samp := num_big_samp * 2;
         
      END IF;
      
      IF int_sanity > 25
      THEN
         out_return_code := -9;
         out_status_message := 'Unable to sample ' || p_region || ' via ' || num_big_samp::VARCHAR;
         RETURN;
         
      END IF;
      
   END LOOP;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Determine random measure on flowline
   --------------------------------------------------------------------------
   out_measure := RANDOM() * (num_tmeasure - num_fmeasure) + num_fmeasure;
   out_measure := ROUND(out_measure,5);
   
   IF p_return_geometry
   THEN
      sdo_flowline := ST_TRANSFORM(sdo_flowline,4326);   
      out_shape    := ST_Force2D(ST_GeometryN(ST_LocateAlong(sdo_flowline,out_measure),1));
      
   END IF;
   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.randomnav(
    VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.randomnav(
    VARCHAR
   ,BOOLEAN
) TO PUBLIC;

--******************************--
----- functions/randompoint.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.randompoint';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.randompoint(
    IN  p_region               VARCHAR DEFAULT NULL
   ,IN  p_include_extended     BOOLEAN DEFAULT FALSE
   ,OUT out_shape              GEOMETRY
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec             RECORD;
   sdo_box         GEOMETRY;
   int_count       INTEGER;
   num_min_x       NUMERIC;
   num_min_y       NUMERIC;
   num_max_x       NUMERIC;
   num_max_y       NUMERIC;
   num_rand_x      NUMERIC;
   num_rand_y      NUMERIC;
   boo_search      BOOLEAN;
   int_sanity      INTEGER;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Select a random catchment
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := p_region
      ,p_include_extended := p_include_extended
      ,p_return_geometry  := TRUE
   );
   
   IF rec.out_return_code != 0
   THEN
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      RETURN;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Box the catchment and get bounds
   --------------------------------------------------------------------------
   sdo_box   := ST_Extent(rec.out_shape);
   num_min_x := ST_XMin(sdo_box);
   num_min_y := ST_YMin(sdo_box);
   num_max_x := ST_XMax(sdo_box);
   num_max_y := ST_YMax(sdo_box);
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Limit the point to being within the catchment
   --------------------------------------------------------------------------
   boo_search := TRUE;
   int_sanity := 0;
   WHILE boo_search
   LOOP
      num_rand_x := RANDOM() * (num_max_x - num_min_x) + num_min_x;
      num_rand_y := RANDOM() * (num_max_y - num_min_y) + num_min_y;
      out_shape  := ST_SetSRID(ST_Point(num_rand_x,num_rand_y),ST_SRID(rec.out_shape));
   
      IF ST_Within(out_shape,rec.out_shape)
      THEN
         boo_search := FALSE;
      
      END IF;
      
      int_sanity := int_sanity + 1;
      IF int_sanity > 25
      THEN
         out_return_code    := -9;
         out_status_message := 'Unable to process ' || int_featureid::VARCHAR || '.';
         RETURN;
         
      END IF;
   
   END LOOP;
   
   out_shape := ST_Transform(out_shape,4269);
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.randompoint(
    VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.randompoint(
    VARCHAR
   ,BOOLEAN
) TO PUBLIC;

--******************************--
----- functions/randomppnav.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.randomppnav';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.randomppnav(
    IN  p_region               VARCHAR DEFAULT NULL
   ,IN  p_return_geometry      BOOLEAN DEFAULT FALSE
   ,OUT out_nhdplusid1         BIGINT
   ,OUT out_reachcode1         VARCHAR
   ,OUT out_measure1           NUMERIC
   ,OUT out_shape1             GEOMETRY
   ,OUT out_nhdplusid2         BIGINT
   ,OUT out_reachcode2         VARCHAR
   ,OUT out_measure2           NUMERIC
   ,OUT out_shape2             GEOMETRY
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                RECORD;
   num_fmeasure       NUMERIC;
   num_tmeasure       NUMERIC;
   int_hydroseq       BIGINT;
   int_dnhydroseq     BIGINT;
   int_terminalpathid BIGINT;
   boo_check          BOOLEAN;
   int_sanity         INTEGER;
   sdo_flowline       GEOMETRY;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   boo_check := FALSE;
   int_sanity := 1;
   
   WHILE NOT boo_check
   LOOP
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Select a random navigable flowline
   --------------------------------------------------------------------------
      rec := cipsrv_nhdplus_h.randomnav(
          p_region          := p_region
         ,p_return_geometry := p_return_geometry
      );
      
      IF rec.out_return_code != 0
      THEN
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         RETURN;
      
      END IF;
      
      out_nhdplusid1  := rec.out_nhdplusid;
      out_reachcode1  := rec.out_reachcode;
      out_measure1    := rec.out_measure;
      out_shape1      := rec.out_shape;
   
      SELECT
       a.hydroseq
      ,a.dnhydroseq
      ,a.terminalpa
      INTO
       int_hydroseq
      ,int_dnhydroseq
      ,int_terminalpathid
      FROM
      cipsrv_nhdplus_h.nhdplusflowlinevaa a 
      WHERE 
      a.nhdplusid = out_nhdplusid1;
      
   --------------------------------------------------------------------------
   -- Step 30
   -- Select a random downstream location from the start point
   --------------------------------------------------------------------------
      WITH RECURSIVE pp(
         nhdplusid
        ,hydroseq
        ,dnhydroseq
        ,terminalpa
        ,fmeasure
        ,tmeasure
      )
      AS (
         SELECT
          out_nhdplusid1
         ,int_hydroseq
         ,int_dnhydroseq
         ,int_terminalpathid
         ,0::NUMERIC
         ,0::NUMERIC
         UNION
         SELECT
          mq.nhdplusid
         ,mq.hydroseq
         ,mq.dnhydroseq
         ,mq.terminalpa
         ,mq.fmeasure
         ,mq.tmeasure
         FROM
         cipsrv_nhdplus_h.nhdplusflowlinevaa_nav mq
         CROSS JOIN
         pp
         WHERE
             mq.ary_upstream_hydroseq @> ARRAY[pp.hydroseq]
         AND mq.terminalpa =  pp.terminalpa
      )
      SELECT
       a.nhdplusid
      ,b.reachcode
      ,a.fmeasure
      ,a.tmeasure
      ,CASE WHEN p_return_geometry THEN b.shape ELSE NULL::GEOMETRY END AS shape
      INTO
       out_nhdplusid2
      ,out_reachcode2
      ,num_fmeasure
      ,num_tmeasure
      ,sdo_flowline
      FROM
      pp a
      JOIN
      cipsrv_nhdplus_h.nhdflowline b
      ON
      b.nhdplusid = a.nhdplusid
      WHERE
          a.nhdplusid <> out_nhdplusid1
      AND RANDOM() < 0.01 
      LIMIT 1;
      
      out_measure2 := RANDOM() * (num_tmeasure - num_fmeasure) + num_fmeasure;
      out_measure2 := ROUND(out_measure2,5);
      
      IF out_nhdplusid2 IS NOT NULL
      THEN
         boo_check := TRUE;
         
      END IF;
      
      int_sanity := int_sanity + 1;
      
      IF int_sanity > 100
      THEN
         out_return_code := -9;
         out_status_message := 'Sanity check failed';
         RETURN;
      
      END IF;
      
   END LOOP;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Build shape2 point if requested
   --------------------------------------------------------------------------
   IF p_return_geometry
   THEN
      sdo_flowline := ST_TRANSFORM(sdo_flowline,4326);   
      out_shape2   := ST_Force2D(ST_GeometryN(ST_LocateAlong(sdo_flowline,out_measure2),1));
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.randomppnav(
    VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.randomppnav(
    VARCHAR
   ,BOOLEAN
) TO PUBLIC;

--******************************--
----- functions/snapflowline2geojson.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.snapflowline2geojson';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.snapflowline2geojson(
    IN  p_input                        cipsrv_nhdplus_h.snapflowline
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   RETURN JSONB_BUILD_OBJECT(
       'type'                ,'Feature'
      ,'geometry'            ,ST_AsGeoJSON(ST_Transform(p_input.shape,4326))::JSONB
      ,'properties'          ,JSONB_BUILD_OBJECT(
          'permanent_identifier'       ,p_input.permanent_identifier
         ,'fdate'                      ,p_input.fdate
         ,'resolution'                 ,p_input.resolution
         ,'gnis_id'                    ,p_input.gnis_id
         ,'gnis_name'                  ,p_input.gnis_name
         ,'lengthkm'                   ,p_input.lengthkm
         ,'reachcode'                  ,p_input.reachcode
         ,'flowdir'                    ,p_input.flowdir
         ,'wbarea_permanent_identifier',p_input.wbarea_permanent_identifier
         ,'ftype'                      ,p_input.ftype
         ,'fcode'                      ,p_input.fcode
         ,'mainpath'                   ,p_input.mainpath
         ,'innetwork'                  ,p_input.innetwork
         ,'visibilityfilter'           ,p_input.visibilityfilter
         ,'nhdplusid'                  ,p_input.nhdplusid
         ,'vpuid'                      ,p_input.vpuid
         ,'enabled'                    ,p_input.enabled
         ,'fmeasure'                   ,p_input.fmeasure
         ,'tmeasure'                   ,p_input.tmeasure
         ,'hydroseq'                   ,p_input.hydroseq
         ,'snap_measure'               ,p_input.snap_measure
         ,'snap_distancekm'            ,p_input.snap_distancekm
      )
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.snapflowline2geojson(
    cipsrv_nhdplus_h.snapflowline
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.snapflowline2geojson(
    cipsrv_nhdplus_h.snapflowline
) TO PUBLIC;
--******************************--
----- functions/snapflowlines2geojson.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.snapflowlines2geojson';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.snapflowlines2geojson(
    IN  p_input                        cipsrv_nhdplus_h.snapflowline[]
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   RETURN JSONB_BUILD_OBJECT(
       'type'                ,'FeatureCollection'
      ,'features'            ,(
         SELECT
         array_agg(cipsrv_nhdplus_h.snapflowline2geojson(x))
         FROM
         UNNEST(p_input) AS x
      )
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.snapflowlines2geojson(
    cipsrv_nhdplus_h.snapflowline[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.snapflowlines2geojson(
    cipsrv_nhdplus_h.snapflowline[]
) TO PUBLIC;
--******************************--
----- functions/split_catchment.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.split_catchment';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.split_catchment(
    IN  p_split_point  GEOMETRY
   ,IN  p_nhdplusid    BIGINT   DEFAULT NULL
   ,IN  p_known_region VARCHAR  DEFAULT NULL
) RETURNS GEOMETRY
STABLE
AS $BODY$
DECLARE 
   rec               RECORD;
   int_nhdplusid     BIGINT       := p_nhdplusid;
   int_original_srid INTEGER;
   int_raster_srid   INTEGER;
   sdo_results       GEOMETRY;
   sdo_results2      GEOMETRY;
   sdo_point         GEOMETRY;
   sdo_catchment     GEOMETRY;
   sdo_catchmentsp   GEOMETRY;
   sdo_temp          GEOMETRY;
   sdo_temp2         GEOMETRY;
   sdo_top_point     GEOMETRY;
   sdo_downstream    GEOMETRY;
   sdo_grid_point    GEOMETRY;
   sdo_catch_buffer  GEOMETRY;
   int_depth         INTEGER := 1;
   rast              RASTER;
   rast_fac          RASTER;
   int_column_x      INTEGER;
   int_row_y         INTEGER;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters and set parameters
   --------------------------------------------------------------------------
   IF p_split_point IS NULL
   THEN
      RETURN NULL;
      
   END IF;    
   
   IF ST_GeometryType(p_split_point) <> 'ST_Point'
   THEN
      RAISE EXCEPTION 'split point geometry must be single point';

   END IF;

   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection 
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_split_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   
   IF rec.out_return_code != 0
   THEN
      RAISE EXCEPTION '%: %',rec.out_return_code,rec.out_status_message;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 30
   -- Project initial point
   --------------------------------------------------------------------------
   int_original_srid := ST_Srid(p_split_point);

   IF int_original_srid <> int_raster_srid
   THEN
      sdo_point := ST_Transform(p_split_point,int_raster_srid);
      
   ELSE
      sdo_point := p_split_point;

   END IF;

   --------------------------------------------------------------------------
   -- Step 40
   -- Fetch the catchment outline to clip with
   --------------------------------------------------------------------------
   IF int_nhdplusid IS NOT NULL
   THEN
      IF int_raster_srid = 5070
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_h.catchment_5070 a
         WHERE
         a.nhdplusid = int_nhdplusid;
         
      ELSIF int_raster_srid = 3338
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_h.catchment_3338 a
         WHERE
         a.nhdplusid = int_nhdplusid;

      ELSIF int_raster_srid = 26904
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_h.catchment_26904 a
         WHERE
         a.nhdplusid = int_nhdplusid;

      ELSIF int_raster_srid = 32161
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_h.catchment_32161 a
         WHERE
         a.nhdplusid = int_nhdplusid;

      ELSIF int_raster_srid = 32655
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_h.catchment_32655 a
         WHERE
         a.nhdplusid = int_nhdplusid;

      ELSIF int_raster_srid = 32702
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_h.catchment_32702 a
         WHERE
         a.nhdplusid = int_nhdplusid;

      ELSE
         RAISE EXCEPTION 'err';
         
      END IF;
   
   ELSE
      IF int_raster_srid = 5070
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_h.catchment_np21_acu a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSIF int_raster_srid = 26904
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_h.catchment_np21_uhi a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSIF int_raster_srid = 32161
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_h.catchment_np21_lpv a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSIF int_raster_srid = 32655
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_h.catchment_np21_ugm a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSIF int_raster_srid = 32702
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_h.catchment_np21_uas a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSE
         RAISE EXCEPTION 'err';
         
      END IF;

   END IF;

   IF sdo_catchment IS NULL
   THEN
      RETURN NULL;

   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Buffer the catchment to get a slightly larger fdr raster 
   -- This size value may need tuning
   --------------------------------------------------------------------------
   sdo_catch_buffer := ST_Buffer(
       sdo_catchment
      ,50
   );
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Fetch the catchment rasters
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.fetch_grids_by_geometry(
       p_FDR_input        := sdo_catch_buffer
      ,p_FAC_input        := sdo_catchment
      ,p_known_region     := int_raster_srid::VARCHAR
      ,p_FDR_nodata       := 255
      ,p_FAC_nodata       := -2147483647
      ,p_crop             := TRUE
   );
   rast     := rec.out_FDR;
   rast_fac := rec.out_FAC;
   
   IF rast IS NULL
   THEN
      RAISE EXCEPTION 'No results for FDR raster';
      
   ELSIF rast_fac IS NULL
   THEN
      RAISE EXCEPTION 'No results for FAC raster';
   
   END IF;

   --------------------------------------------------------------------------
   -- Step 70
   -- Determine what we think is the top of the flowline
   --------------------------------------------------------------------------
   sdo_top_point := cipsrv_nhdplus_h.top_of_flow(
       p_nhdplusid            := int_nhdplusid
      ,p_permanent_identifier := NULL
      ,p_reachcode            := NULL 
      ,p_known_region         := int_raster_srid::VARCHAR
      ,p_2d_flag              := TRUE
      ,p_polygon_mask         := ST_Buffer(sdo_catchment,-1)
   );
   --RAISE WARNING '%', st_astext(st_transform(st_collect(sdo_top_point,sdo_catchment),4269));

   --------------------------------------------------------------------------
   -- Step 80
   -- Get the nearest cell on the flow grid
   --------------------------------------------------------------------------   
   rec := cipsrv_engine.deepest_cell(
       p_input               := sdo_top_point
      ,p_FDR                 := rast
      ,p_FAC                 := rast_fac 
   );
   int_column_x := rec.out_columnX;
   int_row_y    := rec.out_rowY;
   
   --------------------------------------------------------------------------
   -- Step 90
   -- Convert downstream path into multipoint and get closest point to input
   --------------------------------------------------------------------------
   sdo_downstream := cipsrv_engine.raster_raindrop(
       p_raster       := rast
      ,p_columnX      := int_column_x
      ,p_rowY         := int_row_y
   );
   --raise warning '%', st_astext(st_transform(st_collect(sdo_catchment,st_collect(null,sdo_downstream)),4269));

   --------------------------------------------------------------------------
   -- Step 100
   -- Convert downstream path into multipoint and get closest point to input
   --------------------------------------------------------------------------
   SELECT 
   ST_Collect(geom) 
   INTO 
   sdo_downstream
   FROM
   ST_DumpPoints(sdo_downstream);
   
   sdo_grid_point := ST_ClosestPoint(
       sdo_downstream
      ,sdo_point
   );
   --raise warning '%', st_astext(st_transform(st_collect(sdo_catchment,st_collect(null,sdo_grid_point)),4269));
   
   --------------------------------------------------------------------------
   -- Step 110
   -- Use new grid point to split catchment
   --------------------------------------------------------------------------
   rec := ST_WorldToRasterCoord(
       rast
      ,sdo_grid_point
   );
   IF rec IS NULL
   THEN
      RAISE EXCEPTION 'point not on raster error';
      
   END IF;
   
   int_column_x := rec.columnx;
   int_row_y    := rec.rowy;

   --------------------------------------------------------------------------
   -- Step 120
   -- Recursively delineation the upstream cells
   --------------------------------------------------------------------------
   rec := cipsrv_engine.fdr_upstream_norecursion(
       p_column_x := int_column_x
      ,p_row_y    := int_row_y
      ,iout_rast  := rast
   );

   --------------------------------------------------------------------------
   -- Step 130
   -- Blank out all but the delineated cells (make nodata 0 for ST_Polygon)
   --------------------------------------------------------------------------
   rast := ST_Reclass(
       rec.iout_rast
      ,1
      ,'[0-98]:0,99:99,[100-255]:0'
      ,'8BUI'
      ,0
   );
   --raise warning '%', st_astext(st_transform(ST_Polygon(rast),4269));
   
   --------------------------------------------------------------------------
   -- Step 140
   -- Build a polygon from the remaining cells with value 99
   -- Clip the results down the original catchment size to remove padding
   --------------------------------------------------------------------------
   sdo_results := ST_Polygon(
       ST_Clip(
           rast
          ,sdo_catchment
          ,255
          ,TRUE
       )
      ,1
   );
   --raise warning '%', st_astext(st_transform(ST_Collect(sdo_results,sdo_catchment),4269));
   
   --------------------------------------------------------------------------
   -- Step 150
   -- Reduce to single polygon if possible
   --------------------------------------------------------------------------
   IF ST_NumGeometries(sdo_results) = 1
   THEN
      sdo_results := ST_GeometryN(sdo_results,1);

   END IF;
   
   --------------------------------------------------------------------------
   -- Step 180
   -- Project to match input point
   --------------------------------------------------------------------------
   IF int_original_srid <> int_raster_srid
   THEN
      sdo_results := ST_Transform(
          sdo_results
         ,int_original_srid
      );
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 190
   -- Return what we got
   --------------------------------------------------------------------------
   RETURN sdo_results;

END;
$BODY$ LANGUAGE 'plpgsql';

ALTER FUNCTION cipsrv_nhdplus_h.split_catchment(
    GEOMETRY
   ,BIGINT
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.split_catchment(
    GEOMETRY
   ,BIGINT
   ,VARCHAR
) TO PUBLIC;
--******************************--
----- functions/top_of_flow.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.top_of_flow';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.top_of_flow(
    IN  p_nhdplusid                 BIGINT
   ,IN  p_permanent_identifier      VARCHAR
   ,IN  p_reachcode                 VARCHAR
   ,IN  p_known_region              VARCHAR
   ,IN  p_2d_flag                   BOOLEAN  DEFAULT TRUE
   ,IN  p_polygon_mask              GEOMETRY DEFAULT NULL
) RETURNS GEOMETRY
STABLE
AS
$BODY$ 
DECLARE
   rec                RECORD;
   sdo_results        GEOMETRY;
   sdo_polygon_mask   GEOMETRY := p_polygon_mask;
   sdo_flowline       GEOMETRY;
   sdo_temp           GEOMETRY;
   num_highest        NUMERIC;
   num_top_measure    NUMERIC;
   int_raster_srid    INTEGER;
   int_original_srid  INTEGER;
   boo_2d_flag        BOOLEAN := p_2d_flag;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF boo_2d_flag IS NULL
   THEN
      boo_2d_flag := TRUE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Grab the flowline if no mask
   ----------------------------------------------------------------------------
   IF sdo_polygon_mask IS NULL
   THEN
      IF p_nhdplusid IS NOT NULL
      THEN
         SELECT
          a.shape
         ,a.tmeasure
         ,ST_PointN(a.shape,1)
         INTO
          sdo_flowline
         ,num_top_measure
         ,sdo_results
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
         a.nhdplusid = p_nhdplusid;
      
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         SELECT
          a.shape
         ,a.tmeasure
         ,ST_PointN(a.shape,1)
         INTO
          sdo_flowline
         ,num_top_measure
         ,sdo_results
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
         a.permanent_identifier = p_permanent_identifier;
      
      ELSIF p_reachcode IS NOT NULL
      THEN
         SELECT
          a.shape
         ,a.tmeasure
         ,ST_PointN(a.shape,1)
         INTO
          sdo_flowline
         ,num_top_measure
         ,sdo_results
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
             a.reachcode = p_reachcode
         AND a.tmeasure = 100;
         
      ELSE
         RAISE EXCEPTION 'nhdplusid, permanent_identifier or reachcode required';
         
      END IF;
      
      rec := cipsrv_nhdplus_h.determine_grid_srid(
          p_geometry       := sdo_results
         ,p_known_region   := p_known_region
      );
      int_raster_srid    := rec.out_srid;

      sdo_results := ST_Transform(sdo_results,int_raster_srid);
         
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Determine the projection if p_polygon_mask provided
   ----------------------------------------------------------------------------
   IF sdo_polygon_mask IS NOT NULL
   THEN
      int_original_srid := ST_SRID(sdo_polygon_mask);
      
      IF int_original_srid = 5070
      THEN
         int_raster_srid := 5070;
         
      ELSIF int_original_srid = 3338
      THEN
         int_raster_srid := 3338;
         
      ELSIF int_original_srid = 32161
      THEN
         int_raster_srid := 32161;
         
      ELSIF int_original_srid = 26904
      THEN
         int_raster_srid := 26904;
         
      ELSIF int_original_srid = 32655
      THEN
         int_raster_srid := 32655;
         
      ELSIF int_original_srid = 32702
      THEN
         int_raster_srid := 32702;
         
      ELSE
         rec := cipsrv_nhdplus_h.determine_grid_srid(
             p_geometry       := ST_Centroid(sdo_polygon_mask)
            ,p_known_region   := p_known_region
         );
         int_raster_srid    := rec.out_srid;

         IF rec.out_return_code <> 0
         THEN
            RAISE EXCEPTION '%: %', rec.out_return_code,rec.out_status_message;
            
         END IF;
         
         sdo_polygon_mask := ST_Transform(sdo_polygon_mask,int_raster_srid);
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Grab flowline including polygon mask
   ----------------------------------------------------------------------------
   IF sdo_polygon_mask IS NOT NULL
   THEN
      IF p_nhdplusid IS NOT NULL
      THEN
         IF int_raster_srid = 5070
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_5070 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 3338
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_3338 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 26904
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_26904 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32161
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32161 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32655
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32655 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32702
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32702 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSE
             RAISE EXCEPTION 'err';
             
         END IF;
         
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         IF int_raster_srid = 5070
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_5070 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 3338
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_3338 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 26904
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_26904 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32161
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32161 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32655
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32655 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32702
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32702 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSE
             RAISE EXCEPTION 'err';
             
         END IF;
         
      ELSIF p_reachcode IS NOT NULL
      THEN
         IF int_raster_srid = 5070
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_5070 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSIF int_raster_srid = 3338
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_3338 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSIF int_raster_srid = 26904
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_26904 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSIF int_raster_srid = 32161
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_32161 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSIF int_raster_srid = 32655
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_32655 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSIF int_raster_srid = 32702
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_32702 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSE
             RAISE EXCEPTION 'err';
             
         END IF;

      ELSE
         RAISE EXCEPTION 'nhdplusid, permanent_identifier or reachcode required';
         
      END IF;
      --raise warning '%', st_astext(st_transform(sdo_flowline,4269));

      IF sdo_flowline IS NULL
      THEN
         RAISE EXCEPTION 'no flowline returned that intersects polygon mask';
         
      END IF;
            
      sdo_flowline := cipsrv_engine.lrs_intersection(
          p_geometry1 := sdo_flowline
         ,p_geometry2 := sdo_polygon_mask
      );
      
      IF sdo_flowline IS NULL
      THEN
         sdo_results := NULL;
         
      ELSIF ST_NumGeometries(sdo_flowline) = 1
      THEN
         sdo_results := ST_StartPoint(ST_GeometryN(sdo_flowline,1));
         
      ELSE
         num_highest := -1;
         
         FOR i IN 1 .. ST_NumGeometries(sdo_flowline)
         LOOP
            sdo_temp := ST_GeometryN(sdo_flowline,i);
            IF ST_M(ST_StartPoint(sdo_temp)) > num_highest
            THEN
               num_highest := ST_M(ST_StartPoint(sdo_temp));
               
            END IF;
            
         END LOOP;
         
         FOR i IN 1 .. ST_NumGeometries(sdo_flowline)
         LOOP
            sdo_temp := ST_GeometryN(sdo_flowline,i);
            IF ST_M(ST_StartPoint(sdo_temp)) = num_highest
            THEN
               sdo_results := ST_StartPoint(sdo_temp);
               EXIT;
               
            END IF;
         
         END LOOP;
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Bail if nothing found
   ----------------------------------------------------------------------------
   IF sdo_results IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Swap back cs if needed
   ----------------------------------------------------------------------------
   IF int_original_srid <> int_raster_srid
   THEN
      sdo_results := ST_Transform(sdo_results,int_original_srid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Return what we got
   ----------------------------------------------------------------------------
   IF boo_2d_flag
   THEN
      RETURN ST_Force2D(sdo_results);
      
   ELSE
      RETURN sdo_results;
   
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.top_of_flow(
    BIGINT
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.top_of_flow(
    BIGINT
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
   ,GEOMETRY
) TO PUBLIC;
