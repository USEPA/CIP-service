DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_f3_3338(
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
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_f3_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_f3 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_pk
ON cipsrv_wbd.wbd_hu6sp_f3_3338(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_u01
ON cipsrv_wbd.wbd_hu6sp_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_u02
ON cipsrv_wbd.wbd_hu6sp_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_f01
ON cipsrv_wbd.wbd_hu6sp_f3_3338(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_f02
ON cipsrv_wbd.wbd_hu6sp_f3_3338(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_spx
ON cipsrv_wbd.wbd_hu6sp_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_f3_3338;

