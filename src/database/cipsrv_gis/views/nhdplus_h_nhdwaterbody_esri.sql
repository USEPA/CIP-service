CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdwaterbody_esri
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
,a.reachcode
,CAST(a.ftype AS INTEGER)  AS ftype
,CAST(a.fcode AS INTEGER)  AS fcode
,a.visibilityfilter
,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
,a.vpuid
,a.onoffnet
,a.purpcode
,a.burn
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_nhdplus_h.nhdwaterbody a;

ALTER TABLE cipsrv_gis.nhdplus_h_nhdwaterbody_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdwaterbody_esri TO public;
