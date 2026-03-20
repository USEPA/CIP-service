DROP MATERIALIZED VIEW IF EXISTS cipdev_support.epageofab_m_catchment_istribal_final CASCADE;

DROP SEQUENCE IF EXISTS cipdev_support.epageofab_m_catchment_istribal_final_seq;
CREATE SEQUENCE cipdev_support.epageofab_m_catchment_istribal_final_seq START WITH 1;

CREATE MATERIALIZED VIEW cipdev_support.epageofab_m_catchment_istribal_final(
    objectid
   ,nhdplusid
   ,catchmentstatecode
   ,areasqkm
   ,overlap_areasqkm
   ,overlap_percentage
   ,istribal
   ,shape
)
AS
SELECT
 NEXTVAL('cipdev_support.epageofab_m_catchment_istribal_final_seq')::INTEGER AS objectid
,a.nhdplusid
,a.catchmentstatecode
,a.areasqkm
,a.overlap_areasqkm
,a.overlap_percentage
,a.istribal
,a.shape
FROM (
   SELECT
    aa.nhdplusid
   ,aa.catchmentstatecode
   ,aa.areasqkm
   ,aa.overlap_areasqkm
   ,aa.overlap_percentage
   ,aa.istribal
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipdev_support.epageofab_m_catchment_istribal_5070 aa
   UNION ALL
   SELECT
    bb.nhdplusid
   ,bb.catchmentstatecode
   ,bb.areasqkm
   ,bb.overlap_areasqkm
   ,bb.overlap_percentage
   ,bb.istribal
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipdev_support.epageofab_m_catchment_istribal_3338 bb
   UNION ALL
   SELECT
    cc.nhdplusid
   ,cc.catchmentstatecode
   ,cc.areasqkm
   ,cc.overlap_areasqkm
   ,cc.overlap_percentage
   ,cc.istribal
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipdev_support.epageofab_m_catchment_istribal_26904 cc
   UNION ALL
   SELECT
    dd.nhdplusid
   ,dd.catchmentstatecode
   ,dd.areasqkm
   ,dd.overlap_areasqkm
   ,dd.overlap_percentage
   ,dd.istribal
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipdev_support.epageofab_m_catchment_istribal_32161 dd
   UNION ALL
   SELECT
    ee.nhdplusid
   ,ee.catchmentstatecode
   ,ee.areasqkm
   ,ee.overlap_areasqkm
   ,ee.overlap_percentage
   ,ee.istribal
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipdev_support.epageofab_m_catchment_istribal_32655 ee
   UNION ALL
   SELECT
    ff.nhdplusid
   ,ff.catchmentstatecode
   ,ff.areasqkm
   ,ff.overlap_areasqkm
   ,ff.overlap_percentage
   ,ff.istribal
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipdev_support.epageofab_m_catchment_istribal_32702 ff
) a;

ALTER TABLE cipdev_support.epageofab_m_catchment_istribal_final OWNER TO cipsrv;
GRANT SELECT ON cipdev_support.epageofab_m_catchment_istribal_final TO public;

CREATE UNIQUE INDEX IF NOT EXISTS epageofab_m_catchment_istribal_final_pk
ON cipdev_support.epageofab_m_catchment_istribal_final(catchmentstatecode,nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS epageofab_m_catchment_istribal_final_u01
ON cipdev_support.epageofab_m_catchment_istribal_final(objectid);

CREATE INDEX IF NOT EXISTS epageofab_m_catchment_istribal_final_spx
ON cipdev_support.epageofab_m_catchment_istribal_final USING gist(shape);

ANALYZE cipdev_support.epageofab_m_catchment_istribal_final;
