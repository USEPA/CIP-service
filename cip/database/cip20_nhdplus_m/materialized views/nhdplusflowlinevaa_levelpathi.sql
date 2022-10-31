DROP MATERIALIZED VIEW IF EXISTS cip20_nhdplus_m.nhdplusflowlinevaa_levelpathi;

CREATE MATERIALIZED VIEW cip20_nhdplus_m.nhdplusflowlinevaa_levelpathi(
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
,(SELECT c.fromnode FROM cip20_nhdplus_m.nhdplusflowlinevaa c WHERE c.hydroseq = a.max_hydroseq) AS fromnode
,(SELECT d.tonode   FROM cip20_nhdplus_m.nhdplusflowlinevaa d WHERE d.hydroseq = a.min_hydroseq) AS tonode
,a.levelpathilengthkm 
FROM (
   SELECT
    aa.levelpathi
   ,MAX(aa.hydroseq) AS max_hydroseq
   ,MIN(aa.hydroseq) AS min_hydroseq
   ,SUM(aa.lengthkm) AS levelpathilengthkm
   FROM
   cip20_nhdplus_m.nhdplusflowlinevaa aa
   GROUP BY
   aa.levelpathi
) a;

ALTER TABLE cip20_nhdplus_m.nhdplusflowlinevaa_levelpathi OWNER TO cip20;
GRANT SELECT ON cip20_nhdplus_m.nhdplusflowlinevaa_levelpathi TO public;

CREATE UNIQUE INDEX nhdplusflowlinevaa_levelpathi_01u
ON cip20_nhdplus_m.nhdplusflowlinevaa_levelpathi(levelpathi);

CREATE INDEX nhdplusflowlinevaa_levelpathi_01i
ON cip20_nhdplus_m.nhdplusflowlinevaa_levelpathi(max_hydroseq);

CREATE INDEX nhdplusflowlinevaa_levelpathi_02i
ON cip20_nhdplus_m.nhdplusflowlinevaa_levelpathi(min_hydroseq);

CREATE INDEX nhdplusflowlinevaa_levelpathi_03i
ON cip20_nhdplus_m.nhdplusflowlinevaa_levelpathi(fromnode);

CREATE INDEX nhdplusflowlinevaa_levelpathi_04i
ON cip20_nhdplus_m.nhdplusflowlinevaa_levelpathi(tonode);

ANALYZE cip20_nhdplus_m.nhdplusflowlinevaa_levelpathi;

--VACUUM FREEZE ANALYZE cip20_nhdplus_m.nhdplusflowlinevaa_nocat;
