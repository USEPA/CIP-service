DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_nphr_26904(
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
      cipsrv_wbd.wbd_hu10sp_nphr_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_nphr c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_pk
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_u01
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_u02
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_f01
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_f02
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_f03
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_spx
ON cipsrv_wbd.wbd_hu8sp_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_26904;

