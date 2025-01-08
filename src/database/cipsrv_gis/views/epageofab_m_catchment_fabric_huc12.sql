DROP VIEW IF EXISTS cipsrv_gis.epageofab_m_catchment_fabric_huc12;

CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric_huc12
AS
SELECT
 a.objectid
,a.xwalk_huc12
,a.xwalk_huc12_version
,a.areasqkm
,a.globalid
,a.shape
FROM
cipsrv_epageofab_m.catchment_fabric_huc12 a;

ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric_huc12 OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric_huc12 TO public;
