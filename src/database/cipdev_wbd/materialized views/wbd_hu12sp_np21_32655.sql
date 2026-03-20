DROP MATERIALIZED VIEW IF EXISTS cipdev_wbd.wbd_hu12sp_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipdev_wbd.wbd_hu12sp_np21_32655(
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
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}'   AS globalid
,public.ST_TRANSFORM(a.shape,32655) AS shape
FROM
cipsrv_wbd.wbd_hu12sp_np21 a
JOIN
cipsrv_wbd.wbd_hu12_np21 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,4) IN ('2201','2202');

ALTER TABLE cipdev_wbd.wbd_hu12sp_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipdev_wbd.wbd_hu12sp_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_pk
ON cipdev_wbd.wbd_hu12sp_np21_32655(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_u01
ON cipdev_wbd.wbd_hu12sp_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_u02
ON cipdev_wbd.wbd_hu12sp_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_f01
ON cipdev_wbd.wbd_hu12sp_np21_32655(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_f02
ON cipdev_wbd.wbd_hu12sp_np21_32655(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_f03
ON cipdev_wbd.wbd_hu12sp_np21_32655(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_f04
ON cipdev_wbd.wbd_hu12sp_np21_32655(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_f05
ON cipdev_wbd.wbd_hu12sp_np21_32655(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_spx
ON cipdev_wbd.wbd_hu12sp_np21_32655 USING gist(shape);

ANALYZE cipdev_wbd.wbd_hu12sp_np21_32655;

--VACUUM FREEZE ANALYZE cipdev_wbd.wbd_hu12sp_np21_32655;

