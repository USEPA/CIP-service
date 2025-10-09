DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdplusflow_upminordivs;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdplusflow_upminordivs(
    objectid
   ,nodenumber
   ,deltalevel
   ,direction
   ,gapdistkm
   ,hasgeo
   ,fromhydroseq
   ,tohydroseq
   ,pathlength_adj
   ,pathtimema_adj
)
AS
SELECT
 CAST(a.objectid     AS INTEGER)         AS objectid
,CAST(a.nodenumber   AS BIGINT)          AS nodenumber
,CAST(a.deltalevel   AS INTEGER)         AS deltalevel
,CAST(a.direction    AS INTEGER)         AS direction 
,CAST(a.gapdistkm    AS NUMERIC)         AS gapdistkm
,CAST(a.hasgeo       AS VARCHAR(1))      AS hasgeo
,CAST(a.fromhydroseq AS BIGINT)          AS fromhydroseq
,CAST(a.tohydroseq   AS BIGINT)          AS tohydroseq
,c.dnfullpathlength - b.upfullpathlength AS pathlength_adj
,c.dnfullpathtimema - b.upfullpathtimema AS pathtimema_adj
FROM
cipsrv_nhdplus_h.nhdplusflow a
JOIN (
   SELECT
    bb.hydroseq                   AS uphydro_seq
   ,bb.pathlength                 AS upfullpathlength
   ,bb.pathtimema                 AS upfullpathtimema
   ,bb.dnhydroseq                 AS uphydro_dn
   FROM
   cipsrv_nhdplus_h.networknhdflowline bb
) b
ON
b.uphydro_seq = a.fromhydroseq
JOIN (
   SELECT
    cc.hydroseq                   AS dnhydro_seq
   ,cc.pathlength   + cc.lengthkm AS dnfullpathlength
   ,cc.pathtimema   + cc.totma    AS dnfullpathtimema
   ,cc.uphydroseq                 AS dnhydro_up
   FROM
   cipsrv_nhdplus_h.networknhdflowline cc
) c
ON
c.dnhydro_seq = a.tohydroseq
WHERE
b.uphydro_dn != c.dnhydro_seq;

ALTER TABLE cipsrv_nhdplus_h.nhdplusflow_upminordivs OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdplusflow_upminordivs TO public;

CREATE UNIQUE INDEX nhdplusflow_upminordivs_01u
ON cipsrv_nhdplus_h.nhdplusflow_upminordivs(tohydroseq,fromhydroseq);

CREATE UNIQUE INDEX nhdplusflow_upminordivs_02u
ON cipsrv_nhdplus_h.nhdplusflow_upminordivs(objectid);

CREATE INDEX nhdplusflow_upminordivs_01i
ON cipsrv_nhdplus_h.nhdplusflow_upminordivs(nodenumber);

ANALYZE cipsrv_nhdplus_h.nhdplusflow_upminordivs;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdplusflow_upminordivs;

