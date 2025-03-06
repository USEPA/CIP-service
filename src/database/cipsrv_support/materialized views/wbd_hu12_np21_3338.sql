DROP MATERIALIZED VIEW IF EXISTS cipsrv_support.wbd_hu12_np21_3338 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_support.wbd_hu12_np21_3338_seq;
CREATE SEQUENCE IF NOT EXISTS cipsrv_support.wbd_hu12_np21_3338_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_support.wbd_hu12_np21_3338(
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
 NEXTVAL('cipsrv_support.wbd_hu12_np21_3338_seq') AS objectid
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
,a.globalid
,ST_TRANSFORM(a.shape,3338) AS shape
FROM
cipsrv_support.wbd_hu12_np21 a
WHERE
SUBSTR(a.huc12,1,2) IN ('19');

ALTER TABLE cipsrv_support.wbd_hu12_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_support.wbd_hu12_np21_3338 TO public;

CREATE UNIQUE INDEX wbd_hu12_np21_3338_pk
ON cipsrv_support.wbd_hu12_np21_3338(huc12);

CREATE UNIQUE INDEX wbd_hu12_np21_3338_01u
ON cipsrv_support.wbd_hu12_np21_3338(objectid);

CREATE UNIQUE INDEX wbd_hu12_np21_3338_02u
ON cipsrv_support.wbd_hu12_np21_3338(globalid);

CREATE INDEX wbd_hu12_np21_3338_01f
ON cipsrv_support.wbd_hu12_np21_3338(substr(huc12,1,2));

CREATE INDEX wbd_hu12_np21_3338_02f
ON cipsrv_support.wbd_hu12_np21_3338(substr(huc12,1,4));

CREATE INDEX wbd_hu12_np21_3338_03f
ON cipsrv_support.wbd_hu12_np21_3338(substr(huc12,1,6));

CREATE INDEX wbd_hu12_np21_3338_04f
ON cipsrv_support.wbd_hu12_np21_3338(substr(huc12,1,8));

CREATE INDEX wbd_hu12_np21_3338_05f
ON cipsrv_support.wbd_hu12_np21_3338(substr(huc12,1,10));

CREATE INDEX wbd_hu12_np21_3338_spx
ON cipsrv_support.wbd_hu12_np21_3338 USING GIST(shape);

ANALYZE cipsrv_support.wbd_hu12_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_support.wbd_hu12_np21_3338;

