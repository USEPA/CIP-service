DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_f3(
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
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc8
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc8
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc8
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc8
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc8
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc8
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc8
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_32702 ff
   UNION ALL
   SELECT
    gg.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,gg.areasqkm
   ,gg.areaacres
   ,gg.name
   ,gg.states
   ,gg.huc8
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       bbb.tnmid
      ,ggg.metasourceid
      ,ggg.loaddate
      ,ggg.areasqkm
      ,ggg.areaacres
      ,bbb.name
      ,ggg.states
      ,SUBSTR(ggg.huc12,1,8) AS huc8
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,ggg.shape
      FROM
      cipsrv_wbd.wbd_hu12sp_f3 ggg
      LEFT JOIN
      cipsrv_wbd.wbd_names bbb
      ON
      bbb.huc = SUBSTR(ggg.huc12,1,8)
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
   ) gg
) a;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_pk
ON cipsrv_wbd.wbd_hu8sp_f3(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_u01
ON cipsrv_wbd.wbd_hu8sp_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_u02
ON cipsrv_wbd.wbd_hu8sp_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_f01
ON cipsrv_wbd.wbd_hu8sp_f3(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_f02
ON cipsrv_wbd.wbd_hu8sp_f3(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_f03
ON cipsrv_wbd.wbd_hu8sp_f3(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_spx
ON cipsrv_wbd.wbd_hu8sp_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_f3;

