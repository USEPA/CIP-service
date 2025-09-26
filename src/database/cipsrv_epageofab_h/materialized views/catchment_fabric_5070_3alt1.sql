DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_5070_3alt1 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_5070_3alt1(
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

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_5070_3alt1 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_5070_3alt1 TO public;

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt1_spx
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt1 USING gist(shape);

ANALYZE cipsrv_epageofab_h.catchment_fabric_5070_3alt1;

--VACUUM FREEZE ANALYZE cipsrv_epageofab_h.catchment_fabric_5070_3alt1;

