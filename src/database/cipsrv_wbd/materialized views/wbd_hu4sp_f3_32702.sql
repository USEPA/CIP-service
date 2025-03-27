DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_f3_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc4
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
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_f3_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_f3 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32702_pk
ON cipsrv_wbd.wbd_hu4sp_f3_32702(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32702_u01
ON cipsrv_wbd.wbd_hu4sp_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32702_u02
ON cipsrv_wbd.wbd_hu4sp_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_32702_f01
ON cipsrv_wbd.wbd_hu4sp_f3_32702(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_32702_spx
ON cipsrv_wbd.wbd_hu4sp_f3_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_f3_32702;

