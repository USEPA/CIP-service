DROP MATERIALIZED VIEW IF EXISTS cipdev_wbd.wbd_hu8sp_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipdev_wbd.wbd_hu8sp_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipdev_wbd.wbd_hu10sp_f3_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipdev_wbd.wbd_hu8_f3 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipdev_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipdev_wbd.wbd_hu8sp_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipdev_wbd.wbd_hu8sp_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_pk
ON cipdev_wbd.wbd_hu8sp_f3_5070(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_u01
ON cipdev_wbd.wbd_hu8sp_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_u02
ON cipdev_wbd.wbd_hu8sp_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_f01
ON cipdev_wbd.wbd_hu8sp_f3_5070(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_f02
ON cipdev_wbd.wbd_hu8sp_f3_5070(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_f03
ON cipdev_wbd.wbd_hu8sp_f3_5070(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_spx
ON cipdev_wbd.wbd_hu8sp_f3_5070 USING gist(shape);

ANALYZE cipdev_wbd.wbd_hu8sp_f3_5070;

--VACUUM FREEZE ANALYZE cipdev_wbd.wbd_hu8sp_f3_5070;

