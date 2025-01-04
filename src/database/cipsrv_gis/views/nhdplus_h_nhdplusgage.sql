CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplusgage
AS
SELECT
 a.objectid
,a.hydroaddressid
,a.addressdate
,a.featuretype
,a.onnetwork
,a.sourceid
,a.sourceagency
,a.sourcedataset
,a.sourcefeatureurl
,a.catchment
,a.hu
,a.reachcode
,a.measure
,a.reachsmdate
,a.nhdplusid
,a.vpuid
,a.station_nm
,a.state_cd
,a.state
,a.latsite
,a.lonsite
,a.dasqmi
,a.referencegage
,a.class
,a.classmod
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_nhdplus_h.nhdplusgage a;

ALTER TABLE cipsrv_gis.nhdplus_h_nhdplusgage OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplusgage TO public;
