DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_wbdhu12;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_wbdhu12
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.sourcedatadesc
,a.sourceoriginator
,a.sourcefeatureid
,a.loaddate
,a.referencegnis_ids
,a.areaacres
,a.areasqkm
,a.states
,a.huc12
,a.name
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.nhdplusid
,a.vpuid
,a.shape
FROM
cipsrv_nhdplus_h.wbdhu12 a;

ALTER TABLE cipsrv_gis.nhdplus_h_wbdhu12 OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_wbdhu12 TO public;
