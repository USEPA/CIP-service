DROP MATERIALIZED VIEW IF EXISTS cipsrv_support.wbd_hu12_nphr_32655 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_support.wbd_hu12_nphr_32655_seq;
CREATE SEQUENCE IF NOT EXISTS cipsrv_support.wbd_hu12_nphr_32655_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_support.wbd_hu12_nphr_32655(
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
 NEXTVAL('cipsrv_support.wbd_hu12_nphr_32655_seq') AS objectid
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
,ST_TRANSFORM(a.shape,32655) AS shape
FROM
cipsrv_support.wbd_hu12_nphr a
WHERE
SUBSTR(a.huc12,1,4) IN ('2201','2202');

ALTER TABLE cipsrv_support.wbd_hu12_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_support.wbd_hu12_nphr_32655 TO public;

CREATE UNIQUE INDEX wbd_hu12_nphr_32655_pk
ON cipsrv_support.wbd_hu12_nphr_32655(huc12);

CREATE UNIQUE INDEX wbd_hu12_nphr_32655_01u
ON cipsrv_support.wbd_hu12_nphr_32655(objectid);

CREATE UNIQUE INDEX wbd_hu12_nphr_32655_02u
ON cipsrv_support.wbd_hu12_nphr_32655(globalid);

CREATE INDEX wbd_hu12_nphr_32655_01f
ON cipsrv_support.wbd_hu12_nphr_32655(substr(huc12,1,2));

CREATE INDEX wbd_hu12_nphr_32655_02f
ON cipsrv_support.wbd_hu12_nphr_32655(substr(huc12,1,4));

CREATE INDEX wbd_hu12_nphr_32655_03f
ON cipsrv_support.wbd_hu12_nphr_32655(substr(huc12,1,6));

CREATE INDEX wbd_hu12_nphr_32655_04f
ON cipsrv_support.wbd_hu12_nphr_32655(substr(huc12,1,8));

CREATE INDEX wbd_hu12_nphr_32655_05f
ON cipsrv_support.wbd_hu12_nphr_32655(substr(huc12,1,10));

CREATE INDEX wbd_hu12_nphr_32655_spx
ON cipsrv_support.wbd_hu12_nphr_32655 USING GIST(shape);

ANALYZE cipsrv_support.wbd_hu12_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_support.wbd_hu12_nphr_32655;

