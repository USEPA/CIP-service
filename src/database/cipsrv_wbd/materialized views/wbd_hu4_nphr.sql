DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_nphr(
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
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc4
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
   ,aa.huc4
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc4
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc4
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc4
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc4
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc4
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu4_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_pk
ON cipsrv_wbd.wbd_hu4_nphr(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_u01
ON cipsrv_wbd.wbd_hu4_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_u02
ON cipsrv_wbd.wbd_hu4_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_f01
ON cipsrv_wbd.wbd_hu4_nphr(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_spx
ON cipsrv_wbd.wbd_hu4_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_nphr;


