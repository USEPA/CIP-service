DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_wbdhu12_esri;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_wbdhu12_esri
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.sourcedatadesc
,a.sourceoriginator
,a.sourcefeatureid
,a.loaddate
,a.referencegnis_ids
,a.areaacres
,a.areasqkm
,a.states
,a.huc12
,a.name
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
,a.vpuid
,a.shape
FROM
cipsrv_nhdplus_m.wbdhu12 a;

ALTER TABLE cipsrv_gis.nhdplus_m_wbdhu12_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_m_wbdhu12_esri TO public;
