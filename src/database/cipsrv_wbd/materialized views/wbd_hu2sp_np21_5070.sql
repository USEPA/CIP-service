DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_np21_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_np21 c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_5070_pk
ON cipsrv_wbd.wbd_hu2sp_np21_5070(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_5070_u01
ON cipsrv_wbd.wbd_hu2sp_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_5070_u02
ON cipsrv_wbd.wbd_hu2sp_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_np21_5070_spx
ON cipsrv_wbd.wbd_hu2sp_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_np21_5070;

