DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes_seq;
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
   ,totma
   ,fcode
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
    ,CAST(aa.lengthkm AS NUMERIC) AS lengthkm
    ,CAST(aa.totma    AS NUMERIC) AS totma
    ,aa.fcode
    FROM
    cipsrv_nhdplus_h.networknhdflowline aa
    WHERE 
    EXISTS (SELECT 1 FROM cipsrv_epageofab_h.catchment_fabric bb WHERE bb.nhdplusid = aa.nhdplusid)
)
,nocat AS (
   SELECT
    cc.hydroseq
   ,cc.levelpathi
   ,cc.fromnode
   ,cc.tonode
   FROM
   cipsrv_nhdplus_h.networknhdflowline cc
   WHERE 
   NOT EXISTS (SELECT 1 FROM cipsrv_epageofab_h.catchment_fabric dd WHERE dd.nhdplusid = cc.nhdplusid)
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
,a.totma
,a.fcode
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

CREATE INDEX nhdplusflowlinevaa_catnodes_06i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes(fcode);

ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes;
