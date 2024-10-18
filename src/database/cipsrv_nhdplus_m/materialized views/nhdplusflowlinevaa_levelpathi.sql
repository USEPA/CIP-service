DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(
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
,(SELECT c.fromnode FROM cipsrv_nhdplus_m.nhdplusflowlinevaa c WHERE c.hydroseq = a.max_hydroseq) AS fromnode
,(SELECT d.tonode   FROM cipsrv_nhdplus_m.nhdplusflowlinevaa d WHERE d.hydroseq = a.min_hydroseq) AS tonode
,a.levelpathilengthkm 
FROM (
   SELECT
    MAX(aa.objectid) AS objectid
   ,aa.levelpathi
   ,MAX(aa.hydroseq) AS max_hydroseq
   ,MIN(aa.hydroseq) AS min_hydroseq
   ,SUM(aa.lengthkm) AS levelpathilengthkm
   FROM
   cipsrv_nhdplus_m.nhdplusflowlinevaa aa
   GROUP BY
   aa.levelpathi
) a;

ALTER TABLE cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi TO public;

CREATE UNIQUE INDEX nhdplusflowlinevaa_levelpathi_01u
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(levelpathi);

CREATE UNIQUE INDEX nhdplusflowlinevaa_levelpathi_02u
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(objectid);

CREATE INDEX nhdplusflowlinevaa_levelpathi_01i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(max_hydroseq);

CREATE INDEX nhdplusflowlinevaa_levelpathi_02i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(min_hydroseq);

CREATE INDEX nhdplusflowlinevaa_levelpathi_03i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(fromnode);

CREATE INDEX nhdplusflowlinevaa_levelpathi_04i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(tonode);

ANALYZE cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.nhdplusflowlinevaa_nocat;
