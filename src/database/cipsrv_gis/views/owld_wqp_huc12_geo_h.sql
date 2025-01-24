DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_huc12_geo_h;

CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_huc12_geo_h
AS
SELECT
 a.objectid
,a.xwalk_huc12
,a.xwalk_catresolution
,a.xwalk_huc12_version
,a.xwalk_huc12_areasqkm
,a.globalid
,a.shape
FROM
cipsrv_owld.wqp_huc12_geo a
WHERE
a.xwalk_catresolution = 'HR';

ALTER TABLE cipsrv_gis.owld_wqp_huc12_geo_h OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.owld_wqp_huc12_geo_h TO public;
