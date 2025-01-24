DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdarea;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdarea
AS
SELECT
 a.objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.areasqkm
,a.elevation
,a.ftype
,a.fcode
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.onoffnet
,a.purpcode
,a.burn
,a.globalid
,a.shape
FROM
cipsrv_nhdplus_h.nhdarea a;

ALTER TABLE cipsrv_gis.nhdplus_h_nhdarea OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdarea TO public;