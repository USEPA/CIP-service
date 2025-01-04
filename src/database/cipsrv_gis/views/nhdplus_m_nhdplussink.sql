CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdplussink
AS
SELECT
 a.objectid
,a.nhdplusid
,a.gridcode
,a.purpcode
,a.featureid
,a.sourcefc
,a.rpuid
,a.statusflag
,a.catchment
,a.burn
,a.vpuid
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_nhdplus_m.nhdplussink a;

ALTER TABLE cipsrv_gis.nhdplus_m_nhdplussink OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdplussink TO public;
