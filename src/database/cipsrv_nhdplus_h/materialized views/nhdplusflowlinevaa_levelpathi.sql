DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi_seq;
CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi_seq START WITH 1;

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
 NEXTVAL('cipsrv_nhdplus_h.nhdplusflowlinevaa_levelpathi_seq')::INTEGER AS objectid
,a.levelpathi
,a.max_hydroseq
,a.min_hydroseq
,c.fromnode
,d.tonode
,CAST(a.levelpathilengthkm AS NUMERIC) AS levelpathilengthkm 
FROM (
   SELECT
    MAX(aa.objectid) AS objectid
   ,aa.levelpathi
   ,MAX(aa.hydroseq) AS max_hydroseq
   ,MIN(aa.hydroseq) AS min_hydroseq
   ,SUM(aa.lengthkm) AS levelpathilengthkm
   FROM
   cipsrv_nhdplus_h.networknhdflowline aa
   GROUP BY
   aa.levelpathi
) a
LEFT JOIN
cipsrv_nhdplus_h.networknhdflowline c
ON
c.hydroseq = a.max_hydroseq
LEFT JOIN
cipsrv_nhdplus_h.networknhdflowline d
ON
d.hydroseq = a.min_hydroseq;

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

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_nocat;
