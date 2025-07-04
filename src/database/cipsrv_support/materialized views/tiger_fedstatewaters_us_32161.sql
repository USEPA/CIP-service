DROP MATERIALIZED VIEW IF EXISTS cipsrv_support.tiger_fedstatewaters_32161_mv CASCADE;

CREATE MATERIALIZED VIEW cipsrv_support.tiger_fedstatewaters_32161_mv(
    objectid
   ,statens
   ,geoid
   ,stusps
   ,name
   ,aland
   ,awater
   ,intptlat
   ,intptlon
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.statens
,a.geoid
,a.stusps
,a.name
,a.aland
,a.awater
,a.intptlat
,a.intptlon
,'{' || uuid_generate_v1() || '}' AS globalid
,cipsrv_nhdplus_h.snap_to_common_grid(
    p_geometry      := ST_TRANSFORM(a.shape,32161)
   ,p_known_region  := '32161'
   ,p_grid_size     := 0.001
)  AS shape   
FROM
cipsrv_support.tiger_fedstatewaters a
WHERE
a.stusps IN ('PR','VI');

ALTER TABLE cipsrv_support.tiger_fedstatewaters_32161_mv OWNER TO cipsrv;
GRANT SELECT ON cipsrv_support.tiger_fedstatewaters_32161_mv TO public;

CREATE UNIQUE INDEX IF NOT EXISTS tiger_fedstatewaters_32161_mv_u01
ON cipsrv_support.tiger_fedstatewaters_32161_mv(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS tiger_fedstatewaters_32161_mv_u02
ON cipsrv_support.tiger_fedstatewaters_32161_mv(globalid);

CREATE INDEX IF NOT EXISTS tiger_fedstatewaters_32161_mv_spx
ON cipsrv_support.tiger_fedstatewaters_32161_mv USING gist(shape);

ANALYZE cipsrv_support.tiger_fedstatewaters_32161_mv;

--VACUUM FREEZE ANALYZE cipsrv_support.tiger_fedstatewaters_32161_mv;

