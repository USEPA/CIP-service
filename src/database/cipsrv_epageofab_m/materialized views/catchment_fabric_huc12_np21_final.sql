DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_m.catchment_fabric_huc12_np21_final CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_epageofab_m.catchment_fabric_huc12_np21_final_seq;
CREATE SEQUENCE cipsrv_epageofab_m.catchment_fabric_huc12_np21_final_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_epageofab_m.catchment_fabric_huc12_np21_final(
    objectid
   ,xwalk_huc12
   ,xwalk_huc12_version
   ,areasqkm
   ,globalid
   ,shape
)
AS
SELECT
 NEXTVAL('cipsrv_epageofab_m.catchment_fabric_huc12_np21_final_seq')::INTEGER AS objectid
,a.xwalk_huc12
,a.xwalk_huc12_version
,a.areasqkm
,'{' || uuid_generate_v1() || '}' AS globalid
,a.shape
FROM (
   SELECT
    aa.xwalk_huc12
   ,'NP21'                               AS xwalk_huc12_version
   ,ST_AREA(aa.shape)::NUMERIC / 1000000 AS areasqkm
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM (
      SELECT
       xxa.xwalk_huc12
      ,cipsrv_engine.remove_holes(
          p_input          := ST_COLLECTIONEXTRACT(ST_UNION(aaa.shape,0.001),3)
         ,p_threshold_sqkm := 0.00009973
       ) AS shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_5070_3 aaa
      JOIN
      cipsrv_epageofab_m.catchment_fabric_xwalk xxa
      ON
      xxa.nhdplusid = aaa.nhdplusid
      WHERE
      xxa.xwalk_huc12_version = 'NP21'
      GROUP BY
      xxa.xwalk_huc12
   ) aa
   UNION ALL
   SELECT
    bb.xwalk_huc12
   ,'NP21'                               AS xwalk_huc12_version
   ,ST_AREA(bb.shape)::NUMERIC / 1000000 AS areasqkm
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM (
      SELECT
       xxb.xwalk_huc12
      ,cipsrv_engine.remove_holes(
          p_input          := ST_COLLECTIONEXTRACT(ST_UNION(bbb.shape,0.001),3)
         ,p_threshold_sqkm := 0.00009973
       ) AS shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_3338_3 bbb
      JOIN
      cipsrv_epageofab_m.catchment_fabric_xwalk xxb
      ON
      xxb.nhdplusid = bbb.nhdplusid
      WHERE
      xxb.xwalk_huc12_version = 'NP21'
      GROUP BY
      xxb.xwalk_huc12
   ) bb
   UNION ALL
   SELECT
    cc.xwalk_huc12
   ,'NP21'                               AS xwalk_huc12_version
   ,ST_AREA(cc.shape)::NUMERIC / 1000000 AS areasqkm
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM (
      SELECT
       xxc.xwalk_huc12
      ,cipsrv_engine.remove_holes(
          p_input          := ST_COLLECTIONEXTRACT(ST_UNION(ccc.shape,0.001),3)
         ,p_threshold_sqkm := 0.00009973
       ) AS shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_26904_3 ccc
      JOIN
      cipsrv_epageofab_m.catchment_fabric_xwalk xxc
      ON
      xxc.nhdplusid = ccc.nhdplusid
      WHERE
      xxc.xwalk_huc12_version = 'NP21'
      GROUP BY
      xxc.xwalk_huc12
   ) cc
   UNION ALL
   SELECT
    dd.xwalk_huc12
   ,'NP21'                               AS xwalk_huc12_version
   ,ST_AREA(dd.shape)::NUMERIC / 1000000 AS areasqkm
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM (
      SELECT
       xxd.xwalk_huc12
      ,cipsrv_engine.remove_holes(
          p_input          := ST_COLLECTIONEXTRACT(ST_UNION(ddd.shape,0.001),3)
         ,p_threshold_sqkm := 0.00009973
       ) AS shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_32161_3 ddd
      JOIN
      cipsrv_epageofab_m.catchment_fabric_xwalk xxd
      ON
      xxd.nhdplusid = ddd.nhdplusid
      WHERE
      xxd.xwalk_huc12_version = 'NP21'
      GROUP BY
      xxd.xwalk_huc12
   ) dd
   UNION ALL
   SELECT
    ee.xwalk_huc12
   ,'NP21'                               AS xwalk_huc12_version
   ,ST_AREA(ee.shape)::NUMERIC / 1000000 AS areasqkm
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM (
      SELECT
       xxe.xwalk_huc12
      ,cipsrv_engine.remove_holes(
          p_input          := ST_COLLECTIONEXTRACT(ST_UNION(eee.shape,0.001),3)
         ,p_threshold_sqkm := 0.00009973
       ) AS shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_32655_3 eee
      JOIN
      cipsrv_epageofab_m.catchment_fabric_xwalk xxe
      ON
      xxe.nhdplusid = eee.nhdplusid
      WHERE
      xxe.xwalk_huc12_version = 'NP21'
      GROUP BY
      xxe.xwalk_huc12
   ) ee
   UNION ALL
   SELECT
    ff.xwalk_huc12
   ,'NP21'                               AS xwalk_huc12_version
   ,ST_AREA(ff.shape)::NUMERIC / 1000000 AS areasqkm
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM (
      SELECT
       xxf.xwalk_huc12
      ,cipsrv_engine.remove_holes(
          p_input          := ST_COLLECTIONEXTRACT(ST_UNION(fff.shape,0.001),3)
         ,p_threshold_sqkm := 0.00009973
       ) AS shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_32702_3 fff
      JOIN
      cipsrv_epageofab_m.catchment_fabric_xwalk xxf
      ON
      xxf.nhdplusid = fff.nhdplusid
      WHERE
      xxf.xwalk_huc12_version = 'NP21'
      GROUP BY
      xxf.xwalk_huc12
   ) ff
) a
WHERE
    a.shape IS NOT NULL
AND NOT ST_ISEMPTY(a.shape)
AND a.areasqkm > 0.00000005;

ALTER TABLE cipsrv_epageofab_m.catchment_fabric_huc12_np21_final OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_m.catchment_fabric_huc12_np21_final TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_huc12_np21_final_pk
ON cipsrv_epageofab_m.catchment_fabric_huc12_np21_final(xwalk_huc12);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_huc12_np21_final_u01
ON cipsrv_epageofab_m.catchment_fabric_huc12_np21_final(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_huc12_np21_final_u02
ON cipsrv_epageofab_m.catchment_fabric_huc12_np21_final(globalid);

CREATE INDEX IF NOT EXISTS catchment_fabric_huc12_np21_final_spx
ON cipsrv_epageofab_m.catchment_fabric_huc12_np21_final USING gist(shape);

ANALYZE cipsrv_epageofab_m.catchment_fabric_huc12_np21_final;

--VACUUM FREEZE ANALYZE cipsrv_epageofab_m.catchment_fabric_huc12_np21_final;

