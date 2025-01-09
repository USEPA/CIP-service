DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdpluscatchment_esri;

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
,a.shape
FROM
cipsrv_nhdplus_h.nhdpluscatchment a;

ALTER TABLE cipsrv_gis.nhdplus_h_nhdpluscatchment_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdpluscatchment_esri TO public;
