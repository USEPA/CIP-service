CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric
AS
SELECT
 a.objectid
,a.catchmentstatecode
,a.nhdplusid
,a.istribal
,a.istribal_areasqkm
,a.xwalk_huc12
,a.xwalk_method
,a.xwalk_huc12_version
,a.sourcefc
,a.gridcode
,a.areasqkm
,a.areasqkm_geo
,a.isnavigable
,a.hasvaa
,a.issink
,a.isheadwater
,a.iscoastal
,a.isocean
,a.isalaskan
,a.h3hexagonaddr
,a.state_count
,a.vpuid
,a.sourcedataset
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_epageofab_m.catchment_fabric a;

ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric TO public;
