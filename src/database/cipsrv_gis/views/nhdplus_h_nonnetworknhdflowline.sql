DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nonnetworknhdflowline;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nonnetworknhdflowline
AS
SELECT
 a.objectid
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
,a.globalid
,ST_Force2D(a.shape) AS shape
FROM
cipsrv_nhdplus_h.nonnetworknhdflowline a;

ALTER TABLE cipsrv_gis.nhdplus_h_nonnetworknhdflowline OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nonnetworknhdflowline TO public;
