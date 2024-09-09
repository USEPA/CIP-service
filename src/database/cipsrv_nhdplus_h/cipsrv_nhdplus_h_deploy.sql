--******************************--
----- materialized views/nhdplusflowlinevaa_catnodes.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(
    nhdplusid
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
 a.nhdplusid
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
    levelpathi
   ,max_hydroseq
   ,min_hydroseq
   ,fromnode
   ,tonode
   ,levelpathilengthkm
)
AS
SELECT
 a.levelpathi
,a.max_hydroseq
,a.min_hydroseq
,(SELECT c.fromnode FROM cipsrv_nhdplus_h.nhdplusflowlinevaa c WHERE c.hydroseq = a.max_hydroseq) AS fromnode
,(SELECT d.tonode   FROM cipsrv_nhdplus_h.nhdplusflowlinevaa d WHERE d.hydroseq = a.min_hydroseq) AS tonode
,a.levelpathilengthkm 
FROM (
   SELECT
    aa.levelpathi
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

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_3338(
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
   ,'Y' AS isalaskan
   ,MAX(aa.h3hexagonaddr) AS h3hexagonaddr
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,3338)) AS shape
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('AK')
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

ALTER TABLE cipsrv_nhdplus_h.catchment_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_3338 TO public;

CREATE UNIQUE INDEX catchment_3338_01u
ON cipsrv_nhdplus_h.catchment_3338(nhdplusid);

CREATE UNIQUE INDEX catchment_3338_02u
ON cipsrv_nhdplus_h.catchment_3338(hydroseq);

CREATE INDEX catchment_3338_01i
ON cipsrv_nhdplus_h.catchment_3338(levelpathi);

CREATE INDEX catchment_3338_02i
ON cipsrv_nhdplus_h.catchment_3338(fcode);

CREATE INDEX catchment_3338_03i
ON cipsrv_nhdplus_h.catchment_3338(istribal);

CREATE INDEX catchment_3338_04i
ON cipsrv_nhdplus_h.catchment_3338(isnavigable);

CREATE INDEX catchment_3338_05i
ON cipsrv_nhdplus_h.catchment_3338(iscoastal);

CREATE INDEX catchment_3338_06i
ON cipsrv_nhdplus_h.catchment_3338(isocean);

CREATE INDEX catchment_3338_spx
ON cipsrv_nhdplus_h.catchment_3338 USING GIST(shape);

CREATE INDEX catchment_3338_spx2
ON cipsrv_nhdplus_h.catchment_3338 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_h.catchment_3338;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_3338;

--******************************--
----- materialized views/catchment_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_5070(
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
ON cipsrv_nhdplus_h.catchment_5070(istribal);

CREATE INDEX catchment_5070_04i
ON cipsrv_nhdplus_h.catchment_5070(isnavigable);

CREATE INDEX catchment_5070_05i
ON cipsrv_nhdplus_h.catchment_5070(iscoastal);

CREATE INDEX catchment_5070_06i
ON cipsrv_nhdplus_h.catchment_5070(isocean);

CREATE INDEX catchment_5070_spx
ON cipsrv_nhdplus_h.catchment_5070 USING GIST(shape);

CREATE INDEX catchment_5070_spx2
ON cipsrv_nhdplus_h.catchment_5070 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_h.catchment_5070;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_5070;

--******************************--
----- materialized views/catchment_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_26904(
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
   ,ST_UNION(ST_Transform(aa.shape,26904)) AS shape
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('HI')
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

ALTER TABLE cipsrv_nhdplus_h.catchment_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_26904 TO public;

CREATE UNIQUE INDEX catchment_26904_01u
ON cipsrv_nhdplus_h.catchment_26904(nhdplusid);

CREATE UNIQUE INDEX catchment_26904_02u
ON cipsrv_nhdplus_h.catchment_26904(hydroseq);

CREATE INDEX catchment_26904_01i
ON cipsrv_nhdplus_h.catchment_26904(levelpathi);

CREATE INDEX catchment_26904_02i
ON cipsrv_nhdplus_h.catchment_26904(fcode);

CREATE INDEX catchment_26904_03i
ON cipsrv_nhdplus_h.catchment_26904(istribal);

CREATE INDEX catchment_26904_04i
ON cipsrv_nhdplus_h.catchment_26904(isnavigable);

CREATE INDEX catchment_26904_05i
ON cipsrv_nhdplus_h.catchment_26904(iscoastal);

CREATE INDEX catchment_26904_06i
ON cipsrv_nhdplus_h.catchment_26904(isocean);

CREATE INDEX catchment_26904_spx
ON cipsrv_nhdplus_h.catchment_26904 USING GIST(shape);

CREATE INDEX catchment_26904_spx2
ON cipsrv_nhdplus_h.catchment_26904 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_h.catchment_26904;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_26904;

--******************************--
----- materialized views/catchment_32161.sql 

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

--******************************--
----- materialized views/catchment_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_32655(
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
   ,ST_UNION(ST_Transform(aa.shape,32655)) AS shape
   FROM
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('GU','MP')
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

ALTER TABLE cipsrv_nhdplus_h.catchment_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32655 TO public;

CREATE UNIQUE INDEX catchment_32655_01u
ON cipsrv_nhdplus_h.catchment_32655(nhdplusid);

CREATE UNIQUE INDEX catchment_32655_02u
ON cipsrv_nhdplus_h.catchment_32655(hydroseq);

CREATE INDEX catchment_32655_01i
ON cipsrv_nhdplus_h.catchment_32655(levelpathi);

CREATE INDEX catchment_32655_02i
ON cipsrv_nhdplus_h.catchment_32655(fcode);

CREATE INDEX catchment_32655_03i
ON cipsrv_nhdplus_h.catchment_32655(istribal);

CREATE INDEX catchment_32655_04i
ON cipsrv_nhdplus_h.catchment_32655(isnavigable);

CREATE INDEX catchment_32655_05i
ON cipsrv_nhdplus_h.catchment_32655(iscoastal);

CREATE INDEX catchment_32655_06i
ON cipsrv_nhdplus_h.catchment_32655(isocean);

CREATE INDEX catchment_32655_spx
ON cipsrv_nhdplus_h.catchment_32655 USING GIST(shape);

CREATE INDEX catchment_32655_spx2
ON cipsrv_nhdplus_h.catchment_32655 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_h.catchment_32655;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_32655;

--******************************--
----- materialized views/catchment_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_32702(
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
   cipsrv_nhdplus_h.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('AS')
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

ALTER TABLE cipsrv_nhdplus_h.catchment_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32702 TO public;

CREATE UNIQUE INDEX catchment_32702_01u
ON cipsrv_nhdplus_h.catchment_32702(nhdplusid);

CREATE UNIQUE INDEX catchment_32702_02u
ON cipsrv_nhdplus_h.catchment_32702(hydroseq);

CREATE INDEX catchment_32702_01i
ON cipsrv_nhdplus_h.catchment_32702(levelpathi);

CREATE INDEX catchment_32702_02i
ON cipsrv_nhdplus_h.catchment_32702(fcode);

CREATE INDEX catchment_32702_03i
ON cipsrv_nhdplus_h.catchment_32702(istribal);

CREATE INDEX catchment_32702_04i
ON cipsrv_nhdplus_h.catchment_32702(isnavigable);

CREATE INDEX catchment_32702_05i
ON cipsrv_nhdplus_h.catchment_32702(iscoastal);

CREATE INDEX catchment_32702_06i
ON cipsrv_nhdplus_h.catchment_32702(isocean);

CREATE INDEX catchment_32702_spx
ON cipsrv_nhdplus_h.catchment_32702 USING GIST(shape);

CREATE INDEX catchment_32702_spx2
ON cipsrv_nhdplus_h.catchment_32702 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_h.catchment_32702;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_32702;

--******************************--
----- materialized views/nhdflowline_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_3338(
    permanent_identifier
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
   ,shape
)
AS
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

CREATE INDEX nhdflowline_3338_02i
ON cipsrv_nhdplus_h.nhdflowline_3338(fcode);

CREATE INDEX nhdflowline_3338_03i
ON cipsrv_nhdplus_h.nhdflowline_3338(hasvaa);

CREATE INDEX nhdflowline_3338_04i
ON cipsrv_nhdplus_h.nhdflowline_3338(isnavigable);

CREATE INDEX nhdflowline_3338_spx
ON cipsrv_nhdplus_h.nhdflowline_3338 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_3338;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_3338;

--******************************--
----- materialized views/nhdflowline_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_5070(
    permanent_identifier
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
   ,shape
)
AS
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

CREATE INDEX nhdflowline_5070_02i
ON cipsrv_nhdplus_h.nhdflowline_5070(fcode);

CREATE INDEX nhdflowline_5070_03i
ON cipsrv_nhdplus_h.nhdflowline_5070(hasvaa);

CREATE INDEX nhdflowline_5070_04i
ON cipsrv_nhdplus_h.nhdflowline_5070(isnavigable);

CREATE INDEX nhdflowline_5070_spx
ON cipsrv_nhdplus_h.nhdflowline_5070 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_5070;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_5070;

--******************************--
----- materialized views/nhdflowline_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_26904(
    permanent_identifier
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
   ,shape
)
AS
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

CREATE INDEX nhdflowline_26904_02i
ON cipsrv_nhdplus_h.nhdflowline_26904(fcode);

CREATE INDEX nhdflowline_26904_03i
ON cipsrv_nhdplus_h.nhdflowline_26904(hasvaa);

CREATE INDEX nhdflowline_26904_04i
ON cipsrv_nhdplus_h.nhdflowline_26904(isnavigable);

CREATE INDEX nhdflowline_26904_spx
ON cipsrv_nhdplus_h.nhdflowline_26904 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_26904;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_26904;

--******************************--
----- materialized views/nhdflowline_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_32161(
    permanent_identifier
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
   ,shape
)
AS
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

CREATE INDEX nhdflowline_32161_02i
ON cipsrv_nhdplus_h.nhdflowline_32161(fcode);

CREATE INDEX nhdflowline_32161_03i
ON cipsrv_nhdplus_h.nhdflowline_32161(hasvaa);

CREATE INDEX nhdflowline_32161_04i
ON cipsrv_nhdplus_h.nhdflowline_32161(isnavigable);

CREATE INDEX nhdflowline_32161_spx
ON cipsrv_nhdplus_h.nhdflowline_32161 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_32161;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_32161;

--******************************--
----- materialized views/nhdflowline_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_32655(
    permanent_identifier
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
   ,shape
)
AS
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

CREATE INDEX nhdflowline_32655_02i
ON cipsrv_nhdplus_h.nhdflowline_32655(fcode);

CREATE INDEX nhdflowline_32655_03i
ON cipsrv_nhdplus_h.nhdflowline_32655(hasvaa);

CREATE INDEX nhdflowline_32655_04i
ON cipsrv_nhdplus_h.nhdflowline_32655(isnavigable);

CREATE INDEX nhdflowline_32655_spx
ON cipsrv_nhdplus_h.nhdflowline_32655 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_32655;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_32655;

--******************************--
----- materialized views/nhdflowline_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_32702(
    permanent_identifier
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
   ,shape
)
AS
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

CREATE INDEX nhdflowline_32702_02i
ON cipsrv_nhdplus_h.nhdflowline_32702(fcode);

CREATE INDEX nhdflowline_32702_03i
ON cipsrv_nhdplus_h.nhdflowline_32702(hasvaa);

CREATE INDEX nhdflowline_32702_04i
ON cipsrv_nhdplus_h.nhdflowline_32702(isnavigable);

CREATE INDEX nhdflowline_32702_spx
ON cipsrv_nhdplus_h.nhdflowline_32702 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_32702;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_32702;

--******************************--
----- materialized views/nhdplusflowlinevaa_nav.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_nav;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(
    nhdplusid
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
 CAST(a.nhdplusid  AS BIGINT) AS nhdplusid
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
,b.lengthkm
,CASE
 WHEN a.totma IN (-9999,-9998)
 THEN
   NULL
 ELSE
   a.totma
 END AS totma
,a.pathlength
,CASE
 WHEN a.pathtimema IN (-9999,-9998)
 THEN
   NULL
 ELSE
   a.pathtimema
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
----- functions/determine_grid_srid.sql 

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
      out_grid_size  := 30;
      
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
----- functions/temp_table_exists.sql 

CREATE or REPLACE FUNCTION cipsrv_nhdplus_h.temp_table_exists(
   IN p_table_name VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_table_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for temp table
   ----------------------------------------------------------------------------
   SELECT 
    n.nspname
   INTO str_table_name
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   where 
       n.nspname like 'pg_temp_%'
   AND pg_catalog.pg_table_is_visible(c.oid)
   AND UPPER(relname) = UPPER(p_table_name);

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_table_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.temp_table_exists(
   VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.temp_table_exists(
   VARCHAR
) TO PUBLIC;

--******************************--
----- functions/create_line_temp_tables.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.create_line_temp_tables()
RETURNS INT4
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_line temp table
   ----------------------------------------------------------------------------
   IF cipsrv_nhdplus_h.temp_table_exists('tmp_line')
   THEN
      TRUNCATE TABLE tmp_line;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_line(
          nhdplusid               BIGINT
         ,areasqkm                NUMERIC
         ,overlapmeasure          NUMERIC
         ,eventpercentage         NUMERIC
         ,nhdpercentage           NUMERIC
         ,hydroseq                BIGINT
         ,levelpathi              BIGINT
         ,fromnode                BIGINT
         ,tonode                  BIGINT
         ,connector_fromnode      BIGINT
         ,connector_tonode        BIGINT
         ,fcode                   INTEGER
         ,isnavigable             BOOLEAN
      );

      CREATE UNIQUE INDEX tmp_line_pk 
      ON tmp_line(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_line_pk2
      ON tmp_line(hydroseq);
      
      CREATE INDEX tmp_line_01i
      ON tmp_line(levelpathi);
      
      CREATE INDEX tmp_line_02i
      ON tmp_line(fcode);
      
      CREATE INDEX tmp_line_03i
      ON tmp_line(isnavigable);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_line_dd temp table
   ----------------------------------------------------------------------------
   IF cipsrv_nhdplus_h.temp_table_exists('tmp_line_levelpathi')
   THEN
      TRUNCATE TABLE tmp_line_levelpathi;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_line_levelpathi(
          levelpathi              BIGINT
         ,max_hydroseq            BIGINT
         ,min_hydroseq            BIGINT
         ,totaleventpercentage    NUMERIC
         ,totaloverlapmeasure     NUMERIC
         ,levelpathilengthkm      NUMERIC
         ,fromnode                BIGINT
         ,tonode                  BIGINT
         ,connector_fromnode      BIGINT
         ,connector_tonode        BIGINT
      );

      CREATE UNIQUE INDEX tmp_line_levelpathi_pk 
      ON tmp_line_levelpathi(levelpathi);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.create_line_temp_tables() OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.create_line_temp_tables() TO PUBLIC;

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

   IF str_known_region = '5070'
   THEN
      geom_input      := ST_Transform(p_geometry,5070);
      permid_geometry := ST_Transform(p_permid_geometry,5070);

      INSERT INTO tmp_cip(
          permid_joinkey
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_5070 a
      WHERE
      ST_Intersects(
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
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_3338 a
      WHERE
      ST_Intersects(
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
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_26904 a
      WHERE
      ST_Intersects(
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
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_32161 a
      WHERE
      ST_Intersects(
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
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_32655 a
      WHERE
      ST_Intersects(
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
         ,nhdplusid
         ,overlap_measure
      )
      SELECT
       p_permid_joinkey
      ,a.nhdplusid
      ,NULL
      FROM
      cipsrv_nhdplus_h.catchment_32702 a
      WHERE
      ST_Intersects(
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
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.index_point_simple(
    GEOMETRY
   ,VARCHAR
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cipsrv_nhdplus_h.catchment_5070 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cipsrv_nhdplus_h.catchment_3338 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cipsrv_nhdplus_h.catchment_26904 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cipsrv_nhdplus_h.catchment_32161 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cipsrv_nhdplus_h.catchment_32655 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
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
               FROM
               cipsrv_nhdplus_h.catchment_32702 aaaa
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
      out_return_code := cipsrv_nhdplus_h.create_line_temp_tables();
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
                  cipsrv_nhdplus_h.catchment_5070 aaaa
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
                  cipsrv_nhdplus_h.catchment_3338 aaaa
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
                  cipsrv_nhdplus_h.catchment_26904 aaaa
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
                  cipsrv_nhdplus_h.catchment_32161 aaaa
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
                  cipsrv_nhdplus_h.catchment_32655 aaaa
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
                  cipsrv_nhdplus_h.catchment_32702 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
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
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_5070 aaaa
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
         ,nhdplusid
         ,overlap_measure
      )  
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
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
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_3338 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
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
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_26904 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
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
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32161 aaaa
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
         ,nhdplusid
         ,overlap_measure
      )  
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
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
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32655 aaaa
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
         ,nhdplusid
         ,overlap_measure
      )  
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
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
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32702 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_5070 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_3338 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_26904 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32161 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32655 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,permid_geometry
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_h.catchment_32702 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
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
               cipsrv_nhdplus_h.catchment_5070 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
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
               cipsrv_nhdplus_h.catchment_3338 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
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
               cipsrv_nhdplus_h.catchment_26904 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
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
               cipsrv_nhdplus_h.catchment_32161 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
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
               cipsrv_nhdplus_h.catchment_32655 aaaa
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
         ,nhdplusid
         ,overlap_measure
      ) 
      SELECT 
       p_permid_joinkey
      ,a.nhdplusid
      ,a.overlapmeasure
      FROM (
         SELECT
          aa.nhdplusid
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
             aaa.nhdplusid
            ,aaa.areasqkm
            ,aaa.fcode
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
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
               cipsrv_nhdplus_h.catchment_32702 aaaa
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
----- functions/create_nav_temp_tables.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.create_navigation_temp_tables()
RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_navigation_working30 temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_nhdplus_h.temp_table_exists('tmp_navigation_working30')
   THEN
      TRUNCATE TABLE tmp_navigation_working30;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_navigation_working30(
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
         ,navtermination_flag         INTEGER
         ,nav_order                   INTEGER
         ,selected                    BOOLEAN
      );

      CREATE UNIQUE INDEX tmp_navigation_working30_pk
      ON tmp_navigation_working30(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_navigation_working30_1u
      ON tmp_navigation_working30(hydroseq);
      
      CREATE INDEX tmp_navigation_working30_01i
      ON tmp_navigation_working30(network_distancekm);
            
      CREATE INDEX tmp_navigation_working30_02i
      ON tmp_navigation_working30(network_flowtimeday);
      
      CREATE INDEX tmp_navigation_working30_03i
      ON tmp_navigation_working30(dnhydroseq);
      
      CREATE INDEX tmp_navigation_working30_04i
      ON tmp_navigation_working30(nav_order);
      
      CREATE INDEX tmp_navigation_working30_05i
      ON tmp_navigation_working30(selected);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_navigation_results temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_nhdplus_h.temp_table_exists('tmp_navigation_results')
   THEN
      TRUNCATE TABLE tmp_navigation_results;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_navigation_results(
          nhdplusid                   BIGINT
         ,hydroseq                    BIGINT
         ,fmeasure                    NUMERIC
         ,tmeasure                    NUMERIC
         ,levelpathi                  BIGINT
         ,terminalpa                  BIGINT
         ,uphydroseq                  BIGINT
         ,dnhydroseq                  BIGINT
         ,lengthkm                    NUMERIC
         ,flowtimeday                 NUMERIC
         /* ++++++++++ */
         ,network_distancekm          NUMERIC
         ,network_flowtimeday         NUMERIC
         /* ++++++++++ */
         ,permanent_identifier        VARCHAR(40)
         ,reachcode                   VARCHAR(14)
         ,fcode                       INTEGER
         ,gnis_id                     VARCHAR(10)
         ,gnis_name                   VARCHAR(65)
         ,wbarea_permanent_identifier VARCHAR(40)
         /* ++++++++++ */
         ,quality_marker              INTEGER
         ,navtermination_flag         INTEGER
         ,shape                       GEOMETRY
         ,nav_order                   INTEGER
      );

      CREATE UNIQUE INDEX tmp_navigation_results_pk
      ON tmp_navigation_results(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_navigation_results_1u
      ON tmp_navigation_results(hydroseq);
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.create_navigation_temp_tables() OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.create_navigation_temp_tables() TO PUBLIC;

--******************************--
----- functions/get_flowline.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.get_flowline(
    IN  p_direction            VARCHAR DEFAULT NULL
   ,IN  p_nhdplusid            BIGINT  DEFAULT NULL
   ,IN  p_permanent_identifier VARCHAR DEFAULT NULL
   ,IN  p_reachcode            VARCHAR DEFAULT NULL
   ,IN  p_hydroseq             BIGINT  DEFAULT NULL
   ,IN  p_measure              NUMERIC DEFAULT NULL
   ,OUT out_flowline           cipsrv_nhdplus_h.flowline
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   str_direction      VARCHAR(5) := UPPER(p_direction);
   num_difference     NUMERIC;
   num_end_of_line    NUMERIC := 0.0001;
   
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
   IF p_nhdplusid            IS NOT NULL
   OR p_permanent_identifier IS NOT NULL
   OR p_hydroseq             IS NOT NULL
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
            a.nhdplusid            = p_nhdplusid
         OR a.permanent_identifier = p_permanent_identifier
         OR b.hydroseq             = p_hydroseq;

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
         WHERE (
               a.nhdplusid            = p_nhdplusid
            OR a.permanent_identifier = p_permanent_identifier
            OR b.hydroseq             = p_hydroseq
         ) AND (
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
   IF out_flowline.vpuid = '20'
   OR SUBSTR(out_flowline.vpuid,1,2) = '20'
   THEN
      out_flowline.out_grid_srid := 26904;
      
   ELSIF out_flowline.vpuid = '21'
   OR SUBSTR(out_flowline.vpuid,1,2) = '21'
   THEN
      out_flowline.out_grid_srid := 32161;
      
   ELSIF out_flowline.vpuid IN ('22G','22M')
   OR SUBSTR(out_flowline.vpuid,1,4) IN ('2201','2202')
   THEN
      out_flowline.out_grid_srid := 32655;
      
   ELSIF out_flowline.vpuid = '22A'
   OR SUBSTR(out_flowline.vpuid,1,4) = '2203'
   THEN
      out_flowline.out_grid_srid := 32702;
   
   ELSE
      out_flowline.out_grid_srid := 5070;
      
   END IF;
   
EXCEPTION

   WHEN NO_DATA_FOUND
   THEN
      out_return_code    := -10;
      out_status_message := 'no results found in NHDPlus';
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
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.get_flowline(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/catconstrained_reach_index.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.catconstrained_reach_index';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.catconstrained_reach_index(
    IN  p_geometry               GEOMETRY
   ,IN  p_catchment_nhdplusid    NUMERIC
   ,IN  p_return_link_path       BOOLEAN   DEFAULT NULL
   ,IN  p_known_region           VARCHAR   DEFAULT NULL
   ,OUT out_permanent_identifier VARCHAR
   ,OUT out_nhdplusid            NUMERIC
   ,OUT out_fdate                DATE
   ,OUT out_resolution           INTEGER
   ,OUT out_reachcode            VARCHAR
   ,OUT out_flowdir              INTEGER
   ,OUT out_gnis_id              VARCHAR
   ,OUT out_gnis_name            VARCHAR
   ,OUT out_wbarea_permanent_identifier VARCHAR
   ,OUT out_ftype                INTEGER
   ,OUT out_fcode                INTEGER
   ,OUT out_vpuid                VARCHAR
   ,OUT out_snap_measure         NUMERIC
   ,OUT out_snap_distancekm      NUMERIC
   ,OUT out_snap_point           GEOMETRY
   ,OUT out_link_path            GEOMETRY
   ,OUT out_return_code          INTEGER
   ,OUT out_status_message       VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                  RECORD;
   int_raster_srid      INTEGER;
   sdo_input            GEOMETRY;
   num_nhdplusid        NUMERIC;
   boo_issink           BOOLEAN;
   boo_isocean          BOOLEAN;
   boo_isalaskan        BOOLEAN;
   
BEGIN

   out_return_code := 0;
   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters and set parameters
   --------------------------------------------------------------------------
   IF p_geometry IS NULL
   THEN
      RAISE EXCEPTION 'input p_geometry required.';
      
   END IF;
   
   IF ST_GeometryType(p_geometry) <> 'ST_Point'
   THEN
      out_return_code    := -99;
      out_status_message := 'Input must be point geometry';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_geometry
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
   IF ST_SRID(p_geometry) = int_raster_srid
   THEN
      sdo_input := p_geometry;
      
   ELSE
      sdo_input := ST_Transform(p_geometry,int_raster_srid);
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Determine the catchment if not provided
   --------------------------------------------------------------------------
   IF p_catchment_nhdplusid IS NOT NULL
   THEN
      num_nhdplusid := p_catchment_nhdplusid;
      
   ELSE
      IF int_raster_srid = 5070
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          num_nhdplusid
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
          num_nhdplusid
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
          num_nhdplusid
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
          num_nhdplusid
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
          num_nhdplusid
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
         num_nhdplusid
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
   IF num_nhdplusid IS NULL
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
      out_nhdplusid      := num_nhdplusid;
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
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
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
         ,aa.vpuid
         ,aa.fmeasure
         ,aa.tmeasure
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
         aa.nhdplusid = num_nhdplusid
      ) a;
   
   ELSIF int_raster_srid = 26904
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
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
         ,aa.vpuid
         ,aa.fmeasure
         ,aa.tmeasure
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
         aa.nhdplusid = num_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32161
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
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
         ,aa.vpuid
         ,aa.fmeasure
         ,aa.tmeasure
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
         aa.nhdplusid = num_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32655
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
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
         ,aa.vpuid
         ,aa.fmeasure
         ,aa.tmeasure
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
         aa.nhdplusid = num_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32702
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
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
         ,aa.vpuid
         ,aa.fmeasure
         ,aa.tmeasure
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
         aa.nhdplusid = num_nhdplusid
      ) a;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Generate snap path if requested
   --------------------------------------------------------------------------
   IF p_return_link_path
   THEN
      out_link_path := ST_MakeLine(
          ST_Transform(sdo_input,4269)
         ,out_snap_point
      );
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 80
   -- Check for problems and mismatches
   --------------------------------------------------------------------------
   IF out_nhdplusid IS NULL
   OR out_permanent_identifier IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'Error matching catchment to flowline <<' || num_nhdplusid::VARCHAR || '>>';
      RETURN;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.catconstrained_reach_index(
    GEOMETRY
   ,NUMERIC
   ,BOOLEAN
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.catconstrained_reach_index(
    GEOMETRY
   ,NUMERIC
   ,BOOLEAN
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/distance_reach_index.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.distance_reach_index';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.distance_reach_index(
    IN  p_geometry               GEOMETRY
   ,IN  p_fcode_allow            INTEGER[] DEFAULT NULL
   ,IN  p_fcode_deny             INTEGER[] DEFAULT NULL
   ,IN  p_distance_max_dist_km   NUMERIC   DEFAULT 15
   ,IN  p_return_link_path       BOOLEAN   DEFAULT NULL
   ,IN  p_limit_innetwork        BOOLEAN   DEFAULT FALSE
   ,IN  p_limit_navigable        BOOLEAN   DEFAULT FALSE
   ,IN  p_known_region           VARCHAR   DEFAULT NULL
   ,OUT out_permanent_identifier VARCHAR
   ,OUT out_nhdplusid            NUMERIC
   ,OUT out_fdate                DATE
   ,OUT out_resolution           INTEGER
   ,OUT out_reachcode            VARCHAR
   ,OUT out_flowdir              INTEGER
   ,OUT out_gnis_id              VARCHAR
   ,OUT out_gnis_name            VARCHAR
   ,OUT out_wbarea_permanent_identifier VARCHAR
   ,OUT out_ftype                INTEGER
   ,OUT out_fcode                INTEGER
   ,OUT out_vpuid                VARCHAR
   ,OUT out_snap_measure         NUMERIC
   ,OUT out_snap_distancekm      NUMERIC
   ,OUT out_snap_point           GEOMETRY
   ,OUT out_link_path            GEOMETRY
   ,OUT out_return_code          INTEGER
   ,OUT out_status_message       VARCHAR
)
STABLE
AS $BODY$
DECLARE
   rec                      RECORD;
   int_raster_srid          INTEGER;
   sdo_input                GEOMETRY;
   num_distance_max_dist_km NUMERIC;
   boo_check_fcode_allow    BOOLEAN;
   boo_check_fcode_deny     BOOLEAN;
   boo_check_innetwork      BOOLEAN;
   boo_check_navigable      BOOLEAN;
   int_limit                INTEGER := 16;
   
BEGIN

   out_return_code := 0;
   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters and set parameters
   --------------------------------------------------------------------------
   IF p_geometry IS NULL
   THEN
      RAISE EXCEPTION 'p_geometry point required.';
      
   END IF;
   
   IF ST_GeometryType(p_geometry) <> 'ST_Point'
   THEN
      out_return_code    := -99;
      out_status_message := 'Input must be point geometry';
      RETURN;
      
   END IF;
   
   IF p_distance_max_dist_km IS NULL
   THEN
      num_distance_max_dist_km := 99999;
      
   ELSE
      num_distance_max_dist_km := p_distance_max_dist_km;
  
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_geometry
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
   IF ST_SRID(p_geometry) = int_raster_srid
   THEN
      sdo_input := p_geometry;
      
   ELSE
      sdo_input := ST_Transform(p_geometry,int_raster_srid);
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Set booleans for optional clauses
   --------------------------------------------------------------------------
   IF p_fcode_allow IS NOT NULL
   AND array_length(p_fcode_allow,1) > 0
   THEN
      boo_check_fcode_allow := TRUE;
   
   ELSE
      boo_check_fcode_allow := FALSE;
      
   END IF;
   
   IF p_fcode_deny IS NOT NULL
   AND array_length(p_fcode_deny,1) > 0
   THEN
      boo_check_fcode_deny := TRUE;
   
   ELSE
      boo_check_fcode_deny := FALSE;
      
   END IF;
   
   IF p_limit_innetwork
   THEN
      boo_check_innetwork := TRUE;
   
   ELSE
      boo_check_innetwork := FALSE;
      
   END IF;
   
   IF p_limit_navigable
   THEN
      boo_check_navigable := TRUE;
      
   ELSE
      boo_check_navigable := FALSE;
   
   END IF;
 
   --------------------------------------------------------------------------
   -- Step 50
   -- Open the cursor required
   --------------------------------------------------------------------------
   IF int_raster_srid = 5070
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.vpuid
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
         (  boo_check_innetwork IS FALSE
            OR
            aa.hasvaa IS TRUE
         )
         AND
         (  boo_check_navigable IS FALSE
            OR
            aa.isnavigable IS TRUE
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_dist_km
      ORDER BY 
      a.snap_distancekm ASC
      LIMIT 1;

   ELSIF int_raster_srid = 3338
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.vpuid
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
         (  boo_check_innetwork IS FALSE
            OR
            aa.hasvaa IS TRUE
         )
         AND
         (  boo_check_navigable IS FALSE
            OR
            aa.isnavigable IS TRUE
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_dist_km
      ORDER BY 
      a.snap_distancekm ASC
      LIMIT 1;

   ELSIF int_raster_srid = 26904
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.vpuid
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
         (  boo_check_innetwork IS FALSE
            OR
            aa.hasvaa IS TRUE
         )
         AND
         (  boo_check_navigable IS FALSE
            OR
            aa.isnavigable IS TRUE
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_dist_km
      ORDER BY 
      a.snap_distancekm ASC
      LIMIT 1;

   ELSIF int_raster_srid = 32161
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.vpuid
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
         (  boo_check_innetwork IS FALSE
            OR
            aa.hasvaa IS TRUE
         )
         AND
         (  boo_check_navigable IS FALSE
            OR
            aa.isnavigable IS TRUE
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_dist_km
      ORDER BY 
      a.snap_distancekm ASC
      LIMIT 1;

   ELSIF int_raster_srid = 32655
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.vpuid
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
         (  boo_check_innetwork IS FALSE
            OR
            aa.hasvaa IS TRUE
         )
         AND
         (  boo_check_navigable IS FALSE
            OR
            aa.isnavigable IS TRUE
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_dist_km
      ORDER BY 
      a.snap_distancekm ASC
      LIMIT 1;

   ELSIF int_raster_srid = 32702
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.vpuid
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
         (  boo_check_innetwork IS FALSE
            OR
            aa.hasvaa IS TRUE
         )
         AND
         (  boo_check_navigable IS FALSE
            OR
            aa.isnavigable IS TRUE
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_dist_km
      ORDER BY 
      a.snap_distancekm ASC
      LIMIT 1;
      
   ELSE
      RAISE EXCEPTION 'err';
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Generate snap path if requested
   --------------------------------------------------------------------------
   IF p_return_link_path
   THEN
      out_link_path := ST_MakeLine(
          ST_Transform(sdo_input,4269)
         ,out_snap_point
      );
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 80
   -- Check for problems and mismatches
   --------------------------------------------------------------------------
   IF out_nhdplusid IS NULL
   OR out_permanent_identifier IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'no results found';
      RETURN;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.distance_reach_index(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.distance_reach_index(
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
         a.nhdplusid <> r.nhdplusid
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
   int_check := cipsrv_nhdplus_h.create_navigation_temp_tables();

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
                   ST_LocateBetween(ST_Force3DM(bb.shape),aa.fmeasure,aa.tmeasure)
                  ,1
               )
            ELSE
               ST_Force3DM(bb.shape)
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
)  TO PUBLIC;

