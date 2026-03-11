DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_f3_5070(
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
,ST_TRANSFORM(a.shape,5070)       AS shape
FROM
cipsrv_wbd.wbd_hu12sp_f3 a
JOIN
cipsrv_wbd.wbd_hu12_f3 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) NOT IN ('19','20','21','22');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_pk
ON cipsrv_wbd.wbd_hu12sp_f3_5070(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_u01
ON cipsrv_wbd.wbd_hu12sp_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_u02
ON cipsrv_wbd.wbd_hu12sp_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_f01
ON cipsrv_wbd.wbd_hu12sp_f3_5070(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_f02
ON cipsrv_wbd.wbd_hu12sp_f3_5070(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_f03
ON cipsrv_wbd.wbd_hu12sp_f3_5070(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_f04
ON cipsrv_wbd.wbd_hu12sp_f3_5070(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_f05
ON cipsrv_wbd.wbd_hu12sp_f3_5070(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_spx
ON cipsrv_wbd.wbd_hu12sp_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_f3_5070;

