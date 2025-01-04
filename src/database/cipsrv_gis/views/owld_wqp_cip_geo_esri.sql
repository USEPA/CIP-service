CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_cip_geo_esri
AS
SELECT
 a.objectid
,a.cat_joinkey
,a.catchmentstatecode
,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
,a.catchmentresolution
,a.catchmentareasqkm
,a.xwalk_huc12
,a.xwalk_method
,a.xwalk_huc12_version
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_owld.wqp_cip_geo a;

ALTER TABLE cipsrv_gis.owld_wqp_cip_geo_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.owld_wqp_cip_geo_esri TO public;
