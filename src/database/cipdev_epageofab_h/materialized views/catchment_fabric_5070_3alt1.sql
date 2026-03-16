DROP MATERIALIZED VIEW IF EXISTS cipdev_epageofab_h.catchment_fabric_5070_3alt1 CASCADE;

CREATE MATERIALIZED VIEW cipdev_epageofab_h.catchment_fabric_5070_3alt1(
    catchmentstatecode
   ,shape
)
AS
SELECT
 a.catchmentstatecode
,ST_Subdivide(a.shape,8000) AS shape
FROM (
   SELECT
    bbb.stusps AS catchmentstatecode
   ,bbb.shape
   FROM (
      SELECT
       bbbb.stusps
      ,bbbb.shape
      FROM
      cipsrv_support.tiger_fedstatewaters_5070 bbbb
      UNION ALL
      SELECT
       cccc.itemcode AS stusps
      ,cccc.shape
      FROM
      cipsrv_support.outerwaters_5070 cccc
   ) bbb
) a;

ALTER TABLE cipdev_epageofab_h.catchment_fabric_5070_3alt1 OWNER TO cipsrv;
GRANT SELECT ON cipdev_epageofab_h.catchment_fabric_5070_3alt1 TO public;

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt1_spx
ON cipdev_epageofab_h.catchment_fabric_5070_3alt1 USING gist(shape);

ANALYZE cipdev_epageofab_h.catchment_fabric_5070_3alt1;

--VACUUM FREEZE ANALYZE cipdev_epageofab_h.catchment_fabric_5070_3alt1;

