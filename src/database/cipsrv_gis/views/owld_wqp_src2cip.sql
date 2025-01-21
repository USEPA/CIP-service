DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src2cip;

CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src2cip
AS
SELECT
 a.objectid
,a.src2cip_joinkey
,a.cip_joinkey
,a.source_joinkey
,a.permid_joinkey
,a.cat_joinkey
,a.catchmentstatecode
,a.nhdplusid
,a.catchmentresolution
,a.cip_action
,a.overlap_measure
,a.cip_method
,a.cip_parms
,a.cip_date
,a.cip_version
,a.globalid
FROM
cipsrv_owld.wqp_src2cip a;

ALTER TABLE cipsrv_gis.owld_wqp_src2cip OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.owld_wqp_src2cip TO public;
