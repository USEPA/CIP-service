DROP MATERIALIZED VIEW IF EXISTS cipdev_epageofab_h.catchment_fabric_5070_3alt2 CASCADE;

CREATE MATERIALIZED VIEW cipdev_epageofab_h.catchment_fabric_5070_3alt2(
    catchmentstatecode
   ,nhdplusid
)
AS
SELECT
 b.catchmentstatecode
,a.nhdplusid
FROM
cipdev_epageofab_h.catchment_fabric_5070_2 a
INNER JOIN LATERAL (
   SELECT
    bb.catchmentstatecode
	,bb.shape
	FROM 
	cipdev_epageofab_h.catchment_fabric_5070_3alt1 bb
) b
ON
ST_INTERSECTS(b.shape,a.shape);

ALTER TABLE cipdev_epageofab_h.catchment_fabric_5070_3alt2 OWNER TO cipsrv;
GRANT SELECT ON cipdev_epageofab_h.catchment_fabric_5070_3alt2 TO public;

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt2_01i
ON cipdev_epageofab_h.catchment_fabric_5070_3alt2(catchmentstatecode);

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt2_02i
ON cipdev_epageofab_h.catchment_fabric_5070_3alt2(nhdplusid);

ANALYZE cipdev_epageofab_h.catchment_fabric_5070_3alt2;

--VACUUM FREEZE ANALYZE cipdev_epageofab_h.catchment_fabric_5070_3;

