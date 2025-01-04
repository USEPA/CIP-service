CREATE OR REPLACE VIEW cipsrv_gis.epageofab_h_catchment_fabric_huc12_esri
AS
SELECT
 a.objectid
,a.xwalk_huc12
,a.xwalk_huc12_version
,a.areasqkm
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_epageofab_h.catchment_fabric_huc12 a;

ALTER TABLE cipsrv_gis.epageofab_h_catchment_fabric_huc12_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.epageofab_h_catchment_fabric_huc12_esri TO public;
