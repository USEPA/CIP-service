CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdpluscatchment_esri
AS
SELECT
 a.objectid
,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
,a.sourcefc
,a.gridcode
,a.areasqkm
,a.vpuid
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_nhdplus_h.nhdpluscatchment a;

ALTER TABLE cipsrv_gis.nhdplus_h_nhdpluscatchment_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdpluscatchment_esri TO public;
