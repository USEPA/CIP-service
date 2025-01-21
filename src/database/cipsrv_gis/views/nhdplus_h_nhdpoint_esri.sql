DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdpoint_esri;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdpoint_esri
AS
SELECT
 a.objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.reachcode
,CAST(a.ftype AS INTEGER)  AS ftype
,CAST(a.fcode AS INTEGER)  AS fcode
,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
,a.vpuid
,a.globalid
,a.shape
FROM
cipsrv_nhdplus_h.nhdpoint a;

ALTER TABLE cipsrv_gis.nhdplus_h_nhdpoint_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdpoint_esri TO public;
