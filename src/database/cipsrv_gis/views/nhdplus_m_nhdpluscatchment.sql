DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdpluscatchment;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdpluscatchment
AS
SELECT
 a.objectid
,a.nhdplusid
,a.sourcefc
,a.gridcode
,a.areasqkm
,a.vpuid
,a.globalid
,a.shape
FROM
cipsrv_nhdplus_m.nhdpluscatchment a;

ALTER TABLE cipsrv_gis.nhdplus_m_nhdpluscatchment OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdpluscatchment TO public;
