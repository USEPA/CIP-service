DROP MATERIALIZED VIEW IF EXISTS cipdev_wbd.wbd_hu12sp_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipdev_wbd.wbd_hu12sp_f3_26904(
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
,'{' || uuid_generate_v1() || '}' AS globalid
,public.ST_TRANSFORM(a.shape,26904)      AS shape
FROM
cipdev_wbd.wbd_hu12sp_f3 a
JOIN
cipdev_wbd.wbd_hu12_f3 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) IN ('20');

ALTER TABLE cipdev_wbd.wbd_hu12sp_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipdev_wbd.wbd_hu12sp_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_pk
ON cipdev_wbd.wbd_hu12sp_f3_26904(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_u01
ON cipdev_wbd.wbd_hu12sp_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_u02
ON cipdev_wbd.wbd_hu12sp_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_f01
ON cipdev_wbd.wbd_hu12sp_f3_26904(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_f02
ON cipdev_wbd.wbd_hu12sp_f3_26904(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_f03
ON cipdev_wbd.wbd_hu12sp_f3_26904(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_f04
ON cipdev_wbd.wbd_hu12sp_f3_26904(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_f05
ON cipdev_wbd.wbd_hu12sp_f3_26904(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_spx
ON cipdev_wbd.wbd_hu12sp_f3_26904 USING gist(shape);

ANALYZE cipdev_wbd.wbd_hu12sp_f3_26904;

--VACUUM FREEZE ANALYZE cipdev_wbd.wbd_hu12sp_f3_26904;

