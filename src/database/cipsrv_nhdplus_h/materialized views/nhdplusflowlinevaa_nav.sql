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

