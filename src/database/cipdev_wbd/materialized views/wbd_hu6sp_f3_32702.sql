DROP MATERIALIZED VIEW IF EXISTS cipdev_wbd.wbd_hu6sp_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipdev_wbd.wbd_hu6sp_f3_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,public.ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,public.ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipdev_wbd.wbd_hu8sp_f3_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipdev_wbd.wbd_hu6_f3 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipdev_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipdev_wbd.wbd_hu6sp_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipdev_wbd.wbd_hu6sp_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_pk
ON cipdev_wbd.wbd_hu6sp_f3_32702(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_u01
ON cipdev_wbd.wbd_hu6sp_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_u02
ON cipdev_wbd.wbd_hu6sp_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_f01
ON cipdev_wbd.wbd_hu6sp_f3_32702(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_f02
ON cipdev_wbd.wbd_hu6sp_f3_32702(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_spx
ON cipdev_wbd.wbd_hu6sp_f3_32702 USING gist(shape);

ANALYZE cipdev_wbd.wbd_hu6sp_f3_32702;

--VACUUM FREEZE ANALYZE cipdev_wbd.wbd_hu6sp_f3_32702;

