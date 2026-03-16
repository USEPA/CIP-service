DROP MATERIALIZED VIEW IF EXISTS cipdev_epageofab_h.catchment_fabric_5070_3alt3 CASCADE;

CREATE MATERIALIZED VIEW cipdev_epageofab_h.catchment_fabric_5070_3alt3(
    catchmentstatecode
   ,nhdplusid
   ,codecount
)
AS
WITH drilla AS (
   SELECT
    a.catchmentstatecode
	,a.nhdplusid
	FROM
	cipdev_epageofab_h.catchment_fabric_5070_3alt2 a
	GROUP BY 
	 a.catchmentstatecode
	,a.nhdplusid
 ) 
,drillb AS (
   SELECT
    a.nhdplusid
	,COUNT(*) AS codecount
   FROM
	drilla a
   GROUP BY
   a.nhdplusid
 )
SELECT
 a.catchmentstatecode
,a.nhdplusid
,b.codecount
FROM
drilla a
JOIN
drillb b
ON
a.nhdplusid = b.nhdplusid;

ALTER TABLE cipdev_epageofab_h.catchment_fabric_5070_3alt3 OWNER TO cipsrv;
GRANT SELECT ON cipdev_epageofab_h.catchment_fabric_5070_3alt3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_3alt3_pk
ON cipdev_epageofab_h.catchment_fabric_5070_3alt3(catchmentstatecode,nhdplusid);

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt3_01i
ON cipdev_epageofab_h.catchment_fabric_5070_3alt3(catchmentstatecode);

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt3_02i
ON cipdev_epageofab_h.catchment_fabric_5070_3alt3(nhdplusid);

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt3_03i
ON cipdev_epageofab_h.catchment_fabric_5070_3alt3(codecount);

ANALYZE cipdev_epageofab_h.catchment_fabric_5070_3alt3;

--VACUUM FREEZE ANALYZE cipdev_epageofab_h.catchment_fabric_5070_3alt3;

