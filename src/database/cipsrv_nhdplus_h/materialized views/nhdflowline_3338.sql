DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_3338 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_h.nhdflowline_3338_seq;
CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.nhdflowline_3338_seq START WITH 1;

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
   ,fmeasure
   ,tmeasure
   ,hasvaa
   ,isnavigable
   ,hydroseq
   ,shape
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_h.nhdflowline_3338_seq') AS objectid
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
,ST_Transform(a.shape,3338) AS shape
FROM
cipsrv_nhdplus_h.networknhdflowline a
WHERE
SUBSTR(a.vpuid,1,2) IN ('19')
UNION ALL
SELECT
 NEXTVAL('cipsrv_nhdplus_h.nhdflowline_3338_seq') AS objectid
,b.permanent_identifier
,b.fdate
,b.resolution
,b.gnis_id
,b.gnis_name
,b.lengthkm
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
,ST_Transform(b.shape,3338) AS shape
FROM
cipsrv_nhdplus_h.nonnetworknhdflowline b
WHERE
SUBSTR(b.vpuid,1,2) IN ('19');

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

