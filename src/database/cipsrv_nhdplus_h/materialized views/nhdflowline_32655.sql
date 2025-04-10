DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_32655 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_h.nhdflowline_32655_seq;
CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.nhdflowline_32655_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_32655(
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
 NEXTVAL('cipsrv_nhdplus_h.nhdflowline_32655_seq') AS objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.totma
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
,ST_Transform(a.shape,32655) AS shape
FROM
cipsrv_nhdplus_h.networknhdflowline a
WHERE
SUBSTR(a.vpuid,1,4) IN ('2201','2202')
UNION ALL
SELECT
 NEXTVAL('cipsrv_nhdplus_h.nhdflowline_32655_seq') AS objectid
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
,ST_Transform(b.shape,32655) AS shape
FROM
cipsrv_nhdplus_h.nonnetworknhdflowline b
WHERE
SUBSTR(b.vpuid,1,4) IN ('2201','2202');

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

