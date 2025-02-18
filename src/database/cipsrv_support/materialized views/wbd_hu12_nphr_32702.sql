DROP MATERIALIZED VIEW IF EXISTS cipsrv_support.wbd_hu12_nphr_32702 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_support.wbd_hu12_nphr_32702_seq;
CREATE SEQUENCE IF NOT EXISTS cipsrv_support.wbd_hu12_nphr_32702_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_support.wbd_hu12_nphr_32702(
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
 NEXTVAL('cipsrv_support.wbd_hu12_nphr_32702_seq') AS objectid
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
,ST_TRANSFORM(a.shape,32702) AS shape
FROM
cipsrv_support.wbd_hu12_nphr a
WHERE
SUBSTR(a.huc12,1,4) IN ('2203');

ALTER TABLE cipsrv_support.wbd_hu12_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_support.wbd_hu12_nphr_32702 TO public;

CREATE UNIQUE INDEX wbd_hu12_nphr_32702_pk
ON cipsrv_support.wbd_hu12_nphr_32702(huc12);

CREATE UNIQUE INDEX wbd_hu12_nphr_32702_01u
ON cipsrv_support.wbd_hu12_nphr_32702(objectid);

CREATE UNIQUE INDEX wbd_hu12_nphr_32702_02u
ON cipsrv_support.wbd_hu12_nphr_32702(globalid);

CREATE INDEX wbd_hu12_nphr_32702_01f
ON cipsrv_support.wbd_hu12_nphr_32702(substr(huc12,1,2));

CREATE INDEX wbd_hu12_nphr_32702_02f
ON cipsrv_support.wbd_hu12_nphr_32702(substr(huc12,1,4));

CREATE INDEX wbd_hu12_nphr_32702_03f
ON cipsrv_support.wbd_hu12_nphr_32702(substr(huc12,1,6));

CREATE INDEX wbd_hu12_nphr_32702_04f
ON cipsrv_support.wbd_hu12_nphr_32702(substr(huc12,1,8));

CREATE INDEX wbd_hu12_nphr_32702_05f
ON cipsrv_support.wbd_hu12_nphr_32702(substr(huc12,1,10));

CREATE INDEX wbd_hu12_nphr_32702_spx
ON cipsrv_support.wbd_hu12_nphr_32702 USING GIST(shape);

ANALYZE cipsrv_support.wbd_hu12_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_support.wbd_hu12_nphr_32702;

