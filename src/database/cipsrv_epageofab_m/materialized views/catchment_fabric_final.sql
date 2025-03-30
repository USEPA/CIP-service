DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_m.catchment_fabric_final CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_m.catchment_fabric_final(
    objectid
   ,catchmentstatecode
   ,nhdplusid
   ,istribal
   ,istribal_areasqkm
   ,sourcefc
   ,gridcode
   ,areasqkm
   ,isnavigable
   ,hasvaa
   ,issink
   ,isheadwater
   ,iscoastal
   ,isocean
   ,isalaskan
   ,h3hexagonaddr
   ,state_count
   ,vpuid
   ,sourcedataset
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.catchmentstatecode
,a.nhdplusid
,a.istribal
,a.istribal_areasqkm
,a.sourcefc
,a.gridcode
,a.areasqkm
,a.isnavigable
,a.hasvaa
,a.issink
,a.isheadwater
,a.iscoastal
,a.isocean
,a.isalaskan
,a.h3hexagonaddr
,a.state_count
,a.vpuid
,a.sourcedataset
,a.globalid
,a.shape
FROM (
   SELECT
    aa.catchmentstatecode
   ,aa.nhdplusid
   ,aa.istribal
   ,aa.istribal_areasqkm
   ,aa.sourcefc
   ,aa.gridcode
   ,aa.areasqkm
   ,aa.isnavigable
   ,aa.hasvaa
   ,aa.issink
   ,aa.isheadwater
   ,aa.iscoastal
   ,aa.isocean
   ,aa.isalaskan
   ,aa.h3hexagonaddr
   ,(SELECT COUNT(*) FROM cipsrv_epageofab_m.catchment_fabric_5070_3 aaa WHERE aaa.nhdplusid = aa.nhdplusid) AS statecount
   ,aa.vpuid
   ,aa.sourcedataset
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM
   cipsrv_epageofab_m.catchment_fabric_5070_3 aa
   UNION ALL
   SELECT
    bb.catchmentstatecode
   ,bb.nhdplusid
   ,bb.istribal
   ,bb.istribal_areasqkm
   ,bb.sourcefc
   ,bb.gridcode
   ,bb.areasqkm
   ,bb.isnavigable
   ,bb.hasvaa
   ,bb.issink
   ,bb.isheadwater
   ,bb.iscoastal
   ,bb.isocean
   ,bb.isalaskan
   ,bb.h3hexagonaddr
   ,(SELECT COUNT(*) FROM cipsrv_epageofab_m.catchment_fabric_3338_3 bbb WHERE bbb.nhdplusid = bb.nhdplusid) AS statecount
   ,bb.vpuid
   ,bb.sourcedataset
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM
   cipsrv_epageofab_m.catchment_fabric_3338_3 bb
   UNION ALL
   SELECT
    cc.catchmentstatecode
   ,cc.nhdplusid
   ,cc.istribal
   ,cc.istribal_areasqkm
   ,cc.sourcefc
   ,cc.gridcode
   ,cc.areasqkm
   ,cc.isnavigable
   ,cc.hasvaa
   ,cc.issink
   ,cc.isheadwater
   ,cc.iscoastal
   ,cc.isocean
   ,cc.isalaskan
   ,cc.h3hexagonaddr
   ,(SELECT COUNT(*) FROM cipsrv_epageofab_m.catchment_fabric_26904_3 ccc WHERE ccc.nhdplusid = cc.nhdplusid) AS statecount
   ,cc.vpuid
   ,cc.sourcedataset
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM
   cipsrv_epageofab_m.catchment_fabric_26904_3 cc
   UNION ALL
   SELECT
    dd.catchmentstatecode
   ,dd.nhdplusid
   ,dd.istribal
   ,dd.istribal_areasqkm
   ,dd.sourcefc
   ,dd.gridcode
   ,dd.areasqkm
   ,dd.isnavigable
   ,dd.hasvaa
   ,dd.issink
   ,dd.isheadwater
   ,dd.iscoastal
   ,dd.isocean
   ,dd.isalaskan
   ,dd.h3hexagonaddr
   ,(SELECT COUNT(*) FROM cipsrv_epageofab_m.catchment_fabric_32161_3 ddd WHERE ddd.nhdplusid = dd.nhdplusid) AS statecount
   ,dd.vpuid
   ,dd.sourcedataset
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM
   cipsrv_epageofab_m.catchment_fabric_32161_3 dd
   UNION ALL
   SELECT
    ee.catchmentstatecode
   ,ee.nhdplusid
   ,ee.istribal
   ,ee.istribal_areasqkm
   ,ee.sourcefc
   ,ee.gridcode
   ,ee.areasqkm
   ,ee.isnavigable
   ,ee.hasvaa
   ,ee.issink
   ,ee.isheadwater
   ,ee.iscoastal
   ,ee.isocean
   ,ee.isalaskan
   ,ee.h3hexagonaddr
   ,(SELECT COUNT(*) FROM cipsrv_epageofab_m.catchment_fabric_32655_3 eee WHERE eee.nhdplusid = ee.nhdplusid) AS statecount
   ,ee.vpuid
   ,ee.sourcedataset
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM
   cipsrv_epageofab_m.catchment_fabric_32655_3 ee
   UNION ALL
   SELECT
    ff.catchmentstatecode
   ,ff.nhdplusid
   ,ff.istribal
   ,ff.istribal_areasqkm
   ,ff.sourcefc
   ,ff.gridcode
   ,ff.areasqkm
   ,ff.isnavigable
   ,ff.hasvaa
   ,ff.issink
   ,ff.isheadwater
   ,ff.iscoastal
   ,ff.isocean
   ,ff.isalaskan
   ,ff.h3hexagonaddr
   ,(SELECT COUNT(*) FROM cipsrv_epageofab_m.catchment_fabric_32702_3 fff WHERE fff.nhdplusid = ff.nhdplusid) AS statecount
   ,ff.vpuid
   ,ff.sourcedataset
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM
   cipsrv_epageofab_m.catchment_fabric_32702_3 ff
) a;

ALTER TABLE cipsrv_epageofab_m.catchment_fabric_final OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_m.catchment_fabric_final TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_final_pk
ON cipsrv_epageofab_m.catchment_fabric_final(catchmentstatecode,nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_final_u01
ON cipsrv_epageofab_m.catchment_fabric_final(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_final_u02
ON cipsrv_epageofab_m.catchment_fabric_final(globalid);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_01i
ON cipsrv_epageofab_m.catchment_fabric_final(catchmentstatecode);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_02i
ON cipsrv_epageofab_m.catchment_fabric_final(nhdplusid);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_03i
ON cipsrv_epageofab_m.catchment_fabric_final(istribal);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_04i
ON cipsrv_epageofab_m.catchment_fabric_final(isnavigable);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_05i
ON cipsrv_epageofab_m.catchment_fabric_final(hasvaa);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_06i
ON cipsrv_epageofab_m.catchment_fabric_final(issink);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_07i
ON cipsrv_epageofab_m.catchment_fabric_final(isheadwater);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_08i
ON cipsrv_epageofab_m.catchment_fabric_final(iscoastal);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_09i
ON cipsrv_epageofab_m.catchment_fabric_final(isocean);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_10i
ON cipsrv_epageofab_m.catchment_fabric_final(isalaskan);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_11i
ON cipsrv_epageofab_m.catchment_fabric_final(vpuid);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_12i
ON cipsrv_epageofab_m.catchment_fabric_final(sourcedataset);

CREATE INDEX IF NOT EXISTS catchment_fabric_final_spx
ON cipsrv_epageofab_m.catchment_fabric_final USING gist(shape);

ANALYZE cipsrv_epageofab_m.catchment_fabric_final;

--VACUUM FREEZE ANALYZE cipsrv_epageofab_m.catchment_fabric_final;

