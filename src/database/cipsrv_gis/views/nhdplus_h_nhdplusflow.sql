DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplusflow;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplusflow
AS
SELECT
 a.objectid
,a.fromnhdpid
,a.tonhdpid
,a.nodenumber
,a.deltalevel
,a.direction
,a.gapdistkm
,a.hasgeo
,a.fromvpuid
,a.tovpuid
,a.frompermid
,a.topermid
,a.fromhydroseq
,a.tohydroseq
,a.globalid
FROM
cipsrv_nhdplus_h.nhdplusflow a;

ALTER TABLE cipsrv_gis.nhdplus_h_nhdplusflow OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplusflow TO public;
