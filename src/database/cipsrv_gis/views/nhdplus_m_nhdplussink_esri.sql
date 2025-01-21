DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdplussink_esri;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdplussink_esri
AS
SELECT
 a.objectid
,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
,a.gridcode
,a.purpcode
,CAST(a.featureid AS NUMERIC) AS featureid
,a.sourcefc
,a.rpuid
,a.statusflag
,CAST(a.catchment AS SMALLINT) AS catchment
,CAST(a.burn      AS SMALLINT) AS burn
,a.vpuid
,a.globalid
,a.shape
FROM
cipsrv_nhdplus_m.nhdplussink a;

ALTER TABLE cipsrv_gis.nhdplus_m_nhdplussink_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdplussink_esri TO public;
