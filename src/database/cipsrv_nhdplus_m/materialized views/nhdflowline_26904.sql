DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.nhdflowline_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.nhdflowline_26904(
    permanent_identifier
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
   ,shape
)
AS
SELECT
 a.permanent_identifier
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
,ST_Transform(a.shape,26904) AS shape
FROM
cipsrv_nhdplus_m.nhdflowline a
WHERE
a.vpuid IN ('20');

ALTER TABLE cipsrv_nhdplus_m.nhdflowline_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.nhdflowline_26904 TO public;

CREATE UNIQUE INDEX nhdflowline_26904_01u
ON cipsrv_nhdplus_m.nhdflowline_26904(nhdplusid);

CREATE INDEX nhdflowline_26904_02i
ON cipsrv_nhdplus_m.nhdflowline_26904(fcode);

CREATE INDEX nhdflowline_26904_spx
ON cipsrv_nhdplus_m.nhdflowline_26904 USING GIST(shape);

ANALYZE cipsrv_nhdplus_m.nhdflowline_26904;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.nhdflowline_26904;

