DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_26904(
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
   ,enabled
   ,fmeasure
   ,tmeasure
   ,hasvaa
   ,isnavigable
   ,shape
)
AS
SELECT
 CAST(a.objectid AS INTEGER) AS objectid
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

CREATE UNIQUE INDEX nhdflowline_26904_02u
ON cipsrv_nhdplus_h.nhdflowline_26904(objectid);

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

