DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdpluscatchment;

CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdpluscatchment
AS
SELECT
 a.objectid
,a.nhdplusid
,a.sourcefc
,a.gridcode
,a.areasqkm
,a.vpuid
,a.globalid
,a.shape
FROM
cipsrv_nhdplus_h.nhdpluscatchment a;

ALTER TABLE cipsrv_gis.nhdplus_h_nhdpluscatchment OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdpluscatchment TO public;
