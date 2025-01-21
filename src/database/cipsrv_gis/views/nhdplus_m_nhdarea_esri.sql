DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdarea_esri;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdarea_esri
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
,CAST(a.ftype AS INTEGER)  AS ftype
,CAST(a.fcode AS INTEGER)  AS fcode
,a.visibilityfilter
,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
,a.vpuid
,CAST(a.onoffnet AS SMALLINT) AS onoffnet 
,a.purpcode
,CAST(a.burn     AS SMALLINT) AS burn
,a.globalid
,a.shape
FROM
cipsrv_nhdplus_m.nhdarea a;

ALTER TABLE cipsrv_gis.nhdplus_m_nhdarea_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdarea_esri TO public;
