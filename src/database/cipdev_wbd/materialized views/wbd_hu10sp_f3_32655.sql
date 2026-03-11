DROP MATERIALIZED VIEW IF EXISTS cipdev_wbd.wbd_hu10sp_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipdev_wbd.wbd_hu10sp_f3_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER ()                 AS objectid
,b.tnmid
,CAST(NULL AS VARCHAR(40))            AS metasourceid
,CAST(NULL AS DATE)                   AS loaddate
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_f3_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipdev_wbd.wbd_hu10_f3 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipdev_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipdev_wbd.wbd_hu10sp_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipdev_wbd.wbd_hu10sp_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_pk
ON cipdev_wbd.wbd_hu10sp_f3_32655(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_u01
ON cipdev_wbd.wbd_hu10sp_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_u02
ON cipdev_wbd.wbd_hu10sp_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_f01
ON cipdev_wbd.wbd_hu10sp_f3_32655(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_f02
ON cipdev_wbd.wbd_hu10sp_f3_32655(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_f03
ON cipdev_wbd.wbd_hu10sp_f3_32655(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_f04
ON cipdev_wbd.wbd_hu10sp_f3_32655(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_spx
ON cipdev_wbd.wbd_hu10sp_f3_32655 USING gist(shape);

ANALYZE cipdev_wbd.wbd_hu10sp_f3_32655;

--VACUUM FREEZE ANALYZE cipdev_wbd.wbd_hu10sp_f3_32655;

