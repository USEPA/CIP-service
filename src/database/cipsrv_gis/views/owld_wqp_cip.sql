CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_cip
AS
SELECT
 a.objectid
,a.cip_joinkey
,a.permid_joinkey
,a.source_originator
,a.source_featureid
,a.source_featureid2
,a.source_series
,a.source_subdivision
,a.source_joinkey
,a.start_date
,a.end_date
,a.cat_joinkey
,a.catchmentstatecode
,a.nhdplusid
,a.istribal
,a.istribal_areasqkm
,a.catchmentresolution
,a.catchmentareasqkm
,a.xwalk_huc12
,a.xwalk_method
,a.xwalk_huc12_version
,a.isnavigable
,a.hasvaa
,a.issink
,a.isheadwater
,a.iscoastal
,a.isocean
,a.isalaskan
,a.h3hexagonaddr
,a.globalid
FROM
cipsrv_owld.wqp_cip a;

ALTER TABLE cipsrv_gis.owld_wqp_cip OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.owld_wqp_cip TO public;
