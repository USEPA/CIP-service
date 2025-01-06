CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_huc12_geo
AS
SELECT
 a.objectid
,a.xwalk_huc12
,a.xwalk_catresolution
,a.xwalk_huc12_version
,a.xwalk_huc12_areasqkm
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_owld.wqp_huc12_geo a;

ALTER TABLE cipsrv_gis.owld_wqp_huc12_geo OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.owld_wqp_huc12_geo TO public;
