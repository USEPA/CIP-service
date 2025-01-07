CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplusgage_esri
AS
SELECT
 a.objectid
,a.hydroaddressid
,a.addressdate
,a.featuretype
,CAST(a.onnetwork AS SMALLINT) AS onnetwork
,a.sourceid
,a.sourceagency
,a.sourcedataset
,a.sourcefeatureurl
,CAST(a.catchment AS NUMERIC) AS catchment
,a.hu
,a.reachcode
,a.measure
,a.reachsmdate
,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
,a.vpuid
,a.station_nm
,a.state_cd
,a.state
,a.latsite
,a.lonsite
,a.dasqmi
,CAST(a.referencegage AS SMALLINT) AS referencegage
,a.class
,a.classmod
,a.globalid
,ST_Transform(a.shape,3857) AS shape
FROM
cipsrv_nhdplus_h.nhdplusgage a;

ALTER TABLE cipsrv_gis.nhdplus_h_nhdplusgage_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplusgage_esri TO public;
