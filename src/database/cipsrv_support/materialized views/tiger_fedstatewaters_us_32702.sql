DROP MATERIALIZED VIEW IF EXISTS cipsrv_support.tiger_fedstatewaters_us_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_support.tiger_fedstatewaters_us_32702(
    objectid
   ,globalid
   ,shape
)
AS
SELECT
 1::INTEGER AS objectid
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_COLLECTIONEXTRACT(
    ST_UNION(
        cipsrv_nhdplus_h.snap_to_common_grid(
           p_geometry      := a.shape
          ,p_known_region  := '32702'
          ,p_grid_size     := 0.001
        )
    )
   ,3
) AS shape   
FROM
cipsrv_support.tiger_fedstatewaters_32702 a;

ALTER TABLE cipsrv_support.tiger_fedstatewaters_us_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_support.tiger_fedstatewaters_us_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS tiger_fedstatewaters_us_32702_u01
ON cipsrv_support.tiger_fedstatewaters_us_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS tiger_fedstatewaters_us_32702_u02
ON cipsrv_support.tiger_fedstatewaters_us_32702(globalid);

CREATE INDEX IF NOT EXISTS tiger_fedstatewaters_us_32702_spx
ON cipsrv_support.tiger_fedstatewaters_us_32702 USING gist(shape);

ANALYZE cipsrv_support.tiger_fedstatewaters_us_32702;

--VACUUM FREEZE ANALYZE cipsrv_support.tiger_fedstatewaters_us_32702;

