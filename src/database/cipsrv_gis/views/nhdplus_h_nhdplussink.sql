DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplussink;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplussink
AS
SELECT
 a.objectid
,a.nhdplusid
,a.gridcode
,a.purpcode
,a.featureid
,a.sourcefc
,a.rpuid
,a.statusflag
,a.catchment
,a.burn
,a.vpuid
,a.globalid
,a.shape
FROM
cipsrv_nhdplus_h.nhdplussink a;

ALTER TABLE cipsrv_gis.nhdplus_h_nhdplussink OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplussink TO public;
