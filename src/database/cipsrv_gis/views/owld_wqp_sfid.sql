CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_sfid
AS
SELECT
 a.objectid
,a.source_originator
,a.source_featureid
,a.source_featureid2
,a.source_series
,a.source_subdivision
,a.source_joinkey
,a.start_date
,a.end_date
,a.sfiddetailurl
,a.load_id
,a.load_date
,a.src_event_count
,a.src_point_count
,a.src_line_count
,a.src_area_count
,a.cat_mr_count
,a.cat_hr_count
,a.xwalk_huc12_mr_count
,a.rad_mr_event_count
,a.rad_hr_event_count
,a.rad_mr_point_count
,a.rad_hr_point_count
,a.rad_mr_line_count
,a.rad_hr_line_count
,a.rad_mr_area_count
,a.rad_hr_area_count
,a.globalid
FROM
cipsrv_owld.wqp_sfid a;

ALTER TABLE cipsrv_gis.owld_wqp_sfid OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.owld_wqp_sfid TO public;
