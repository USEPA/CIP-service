CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdpoint
AS
SELECT
 a.objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.reachcode
,a.ftype
,a.fcode
,a.nhdplusid
,a.vpuid
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_nhdplus_m.nhdpoint a;

ALTER TABLE cipsrv_gis.nhdplus_m_nhdpoint OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdpoint TO public;
