DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_f3_3338(
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
,ST_TRANSFORM(a.shape,3338)       AS shape
FROM
cipsrv_wbd.wbd_hu12_f3 a
WHERE
SUBSTR(a.huc12,1,2) IN ('19');

ALTER TABLE cipsrv_wbd.wbd_hu12_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_3338_pk
ON cipsrv_wbd.wbd_hu12_f3_3338(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_3338_u01
ON cipsrv_wbd.wbd_hu12_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_3338_u02
ON cipsrv_wbd.wbd_hu12_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_f01
ON cipsrv_wbd.wbd_hu12_f3_3338(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_f02
ON cipsrv_wbd.wbd_hu12_f3_3338(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_f03
ON cipsrv_wbd.wbd_hu12_f3_3338(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_f04
ON cipsrv_wbd.wbd_hu12_f3_3338(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_f05
ON cipsrv_wbd.wbd_hu12_f3_3338(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_spx
ON cipsrv_wbd.wbd_hu12_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_f3_3338;

