DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_evt2meta_m;

CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_evt2meta_m
AS
SELECT
 a.objectid
,a.permanent_identifier
,a.meta_processid
,a.globalid
FROM
cipsrv_owld.wqp_rad_evt2meta a
WHERE
a.reachresolution = 'MR';

ALTER TABLE cipsrv_gis.owld_wqp_rad_evt2meta_m OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.owld_wqp_rad_evt2meta_m TO public;
