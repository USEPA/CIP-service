DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_f3(
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
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc6
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
   ,aa.huc6
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc6
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc6
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc6
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc6
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc6
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_32702 ff
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc6
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'220400' AS huc6
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,(SELECT ST_UNION(gggg.shape) FROM cipsrv_wbd.wbd_hu12_f3 gggg WHERE SUBSTR(gggg.huc12,1,6) = '220400') AS shape
      FROM
      cipsrv_wbd.wbd_hu12sp_f3 ggg
      WHERE
      SUBSTR(ggg.huc12,1,6) = '220400'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc6
) a;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_pk
ON cipsrv_wbd.wbd_hu6sp_f3(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_u01
ON cipsrv_wbd.wbd_hu6sp_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_u02
ON cipsrv_wbd.wbd_hu6sp_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_f01
ON cipsrv_wbd.wbd_hu6sp_f3(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_f02
ON cipsrv_wbd.wbd_hu6sp_f3(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_spx
ON cipsrv_wbd.wbd_hu6sp_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_f3;

