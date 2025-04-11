DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.nhdflowline_32161 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_m.nhdflowline_32161_seq;
CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_m.nhdflowline_32161_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.nhdflowline_32161(
    objectid
   ,permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,totma
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
   ,fmeasure
   ,tmeasure
   ,hasvaa
   ,isnavigable
   ,hydroseq
   ,shape
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_m.nhdflowline_32161_seq') AS objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,CASE
 WHEN a.totma IN (-9998,-9999)
 THEN
   CAST(NULL AS NUMERIC)
 ELSE
   a.totma
 END AS totma
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
,a.frommeas AS fmeasure
,a.tomeas   AS tmeasure
,TRUE AS hasvaa
,CASE
 WHEN a.fcode NOT IN (56600)
 THEN
   TRUE
 ELSE
   FALSE
 END AS isnavigable
,a.hydroseq
,ST_Transform(a.shape,32161) AS shape
FROM
cipsrv_nhdplus_m.networknhdflowline a
WHERE
a.vpuid IN ('21')
UNION ALL
SELECT
 NEXTVAL('cipsrv_nhdplus_m.nhdflowline_32161_seq') AS objectid
,b.permanent_identifier
,b.fdate
,b.resolution
,b.gnis_id
,b.gnis_name
,b.lengthkm
,NULL AS totma
,b.reachcode
,b.flowdir
,b.wbarea_permanent_identifier
,b.ftype
,b.fcode
,b.mainpath
,b.innetwork
,b.visibilityfilter
,b.nhdplusid
,b.vpuid
,ROUND(ST_M(ST_EndPoint(b.shape))::NUMERIC,5)   AS fmeasure
,ROUND(ST_M(ST_StartPoint(b.shape))::NUMERIC,5) AS tmeasure
,FALSE AS hasvaa
,FALSE AS isnavigable
,NULL AS hydroseq
,ST_Transform(b.shape,32161) AS shape
FROM
cipsrv_nhdplus_m.nonnetworknhdflowline b
WHERE
b.vpuid IN ('21');

ALTER TABLE cipsrv_nhdplus_m.nhdflowline_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.nhdflowline_32161 TO public;

CREATE UNIQUE INDEX nhdflowline_32161_01u
ON cipsrv_nhdplus_m.nhdflowline_32161(nhdplusid);

CREATE UNIQUE INDEX nhdflowline_32161_02u
ON cipsrv_nhdplus_m.nhdflowline_32161(objectid);

CREATE INDEX nhdflowline_32161_02i
ON cipsrv_nhdplus_m.nhdflowline_32161(fcode);

CREATE INDEX nhdflowline_32161_03i
ON cipsrv_nhdplus_m.nhdflowline_32161(hasvaa);

CREATE INDEX nhdflowline_32161_04i
ON cipsrv_nhdplus_m.nhdflowline_32161(isnavigable);

CREATE INDEX nhdflowline_32161_05i
ON cipsrv_nhdplus_m.nhdflowline_32161(hydroseq);

CREATE INDEX nhdflowline_32161_spx
ON cipsrv_nhdplus_m.nhdflowline_32161 USING GIST(shape);

ANALYZE cipsrv_nhdplus_m.nhdflowline_32161;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.nhdflowline_32161;

