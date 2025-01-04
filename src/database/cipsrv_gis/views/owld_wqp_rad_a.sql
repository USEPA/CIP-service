CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_a
AS
SELECT
 a.objectid
,a.permanent_identifier
,a.eventdate
,a.reachcode
,a.reachsmdate
,a.reachresolution
,a.feature_permanent_identifier
,a.featureclassref
,a.source_originator
,a.source_featureid
,a.source_featureid2
,a.source_datadesc
,a.source_series
,a.source_subdivision
,a.source_joinkey
,a.permid_joinkey
,a.start_date
,a.end_date
,a.featuredetailurl
,a.eventtype
,a.event_areasqkm
,a.geogstate
,a.xwalk_huc12
,a.xwalk_method
,a.xwalk_huc12_version
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_owld.wqp_rad_a a;

ALTER TABLE cipsrv_gis.owld_wqp_rad_a OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.owld_wqp_rad_a TO public;
