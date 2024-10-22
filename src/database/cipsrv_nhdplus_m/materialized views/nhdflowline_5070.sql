DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.nhdflowline_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.nhdflowline_5070(
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
,ST_Transform(a.shape,5070) AS shape
FROM
cipsrv_nhdplus_m.nhdflowline a
LEFT JOIN
cipsrv_nhdplus_m.nhdplusflowlinevaa b
ON
a.nhdplusid = b.nhdplusid
WHERE
a.vpuid NOT IN ('19','20','21','22A','22G','22M');

ALTER TABLE cipsrv_nhdplus_m.nhdflowline_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.nhdflowline_5070 TO public;

CREATE UNIQUE INDEX nhdflowline_5070_01u
ON cipsrv_nhdplus_m.nhdflowline_5070(nhdplusid);

CREATE UNIQUE INDEX nhdflowline_5070_02u
ON cipsrv_nhdplus_m.nhdflowline_5070(objectid);

CREATE INDEX nhdflowline_5070_02i
ON cipsrv_nhdplus_m.nhdflowline_5070(fcode);

CREATE INDEX nhdflowline_5070_03i
ON cipsrv_nhdplus_m.nhdflowline_5070(hasvaa);

CREATE INDEX nhdflowline_5070_04i
ON cipsrv_nhdplus_m.nhdflowline_5070(isnavigable);

CREATE INDEX nhdflowline_5070_spx
ON cipsrv_nhdplus_m.nhdflowline_5070 USING GIST(shape);

ANALYZE cipsrv_nhdplus_m.nhdflowline_5070;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.nhdflowline_5070;

