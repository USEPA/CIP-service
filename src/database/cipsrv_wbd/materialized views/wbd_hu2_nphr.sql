DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_nphr(
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
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc2
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
   ,aa.huc2
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc2
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc2
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc2
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_nphr_32161 dd
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc2
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'22' AS huc2
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,'{' || uuid_generate_v1() || '}' AS globalid
      ,(SELECT 
        ST_UNION(gggg.shape) 
        FROM (
           SELECT 
           a1.shape 
           FROM 
           cipsrv_wbd.wbd_hu12_nphr a1 
           WHERE 
           SUBSTR(a1.huc12,1,4) = '2204'
           UNION ALL
           SELECT
           ST_TRANSFORM(a2.shape,4269)
           FROM
           cipsrv_wbd.wbd_hu4_nphr_32655 a2
           UNION ALL
           SELECT
           ST_TRANSFORM(a3.shape,4269)
           FROM
           cipsrv_wbd.wbd_hu4_nphr_32702 a3
        ) gggg
       ) AS shape
      FROM
      cipsrv_wbd.wbd_hu12_nphr ggg
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc2
) a;

ALTER TABLE cipsrv_wbd.wbd_hu2_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_pk
ON cipsrv_wbd.wbd_hu2_nphr(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_u01
ON cipsrv_wbd.wbd_hu2_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_u02
ON cipsrv_wbd.wbd_hu2_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_nphr_spx
ON cipsrv_wbd.wbd_hu2_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_nphr;


