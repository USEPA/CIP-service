DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32161)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_nphr a
WHERE
SUBSTR(a.huc12,1,2) IN ('21');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_pk
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_u01
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_u02
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_f01
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_f02
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_f03
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_f04
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_f05
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_spx
ON cipsrv_wbd.wbd_hu12sp_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_32161;

