DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nonnetworknhdflowline_esri;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nonnetworknhdflowline_esri
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
,CAST(a.ftype      AS INTEGER)  AS ftype
,CAST(a.fcode      AS INTEGER)  AS fcode
,a.mainpath
,CAST(a.innetwork  AS SMALLINT) AS innetwork
,a.visibilityfilter
,CAST(a.nhdplusid  AS NUMERIC) AS nhdplusid
,a.vpuid
,a.globalid
,ST_Force2D(a.shape) AS shape
FROM
cipsrv_nhdplus_m.nonnetworknhdflowline a;

ALTER TABLE cipsrv_gis.nhdplus_m_nonnetworknhdflowline_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_m_nonnetworknhdflowline_esri TO public;
