DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_5070_3alt2 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_5070_3alt2(
    catchmentstatecode
   ,nhdplusid
)
AS
SELECT
 b.catchmentstatecode
,a.nhdplusid
FROM
cipsrv_epageofab_h.catchment_fabric_5070_2 a
INNER JOIN LATERAL (
   SELECT
    bb.catchmentstatecode
	,bb.shape
	FROM 
	cipsrv_epageofab_h.catchment_fabric_5070_3alt1 bb
) b
ON
ST_INTERSECTS(b.shape,a.shape);

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_5070_3alt2 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_5070_3alt2 TO public;

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt2_01i
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt2(catchmentstatecode);

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt2_02i
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt2(nhdplusid);

ANALYZE cipsrv_epageofab_h.catchment_fabric_5070_3alt2;

--VACUUM FREEZE ANALYZE cipsrv_epageofab_h.catchment_fabric_5070_3;

