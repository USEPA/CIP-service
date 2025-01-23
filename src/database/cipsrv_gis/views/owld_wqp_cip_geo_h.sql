DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_cip_geo_h;

CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_cip_geo_h
AS
SELECT
 a.objectid
,a.cat_joinkey
,a.catchmentstatecode
,a.nhdplusid
,a.catchmentresolution
,a.catchmentareasqkm
,a.xwalk_huc12
,a.xwalk_method
,a.xwalk_huc12_version
,a.globalid
,a.shape
FROM
cipsrv_owld.wqp_cip_geo a
WHERE
a.catchmentresolution = 'HR';

ALTER TABLE cipsrv_gis.owld_wqp_cip_geo_h OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.owld_wqp_cip_geo_h TO public;
