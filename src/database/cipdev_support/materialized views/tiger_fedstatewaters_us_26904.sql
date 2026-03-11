DROP MATERIALIZED VIEW IF EXISTS cipdev_support.tiger_fedstatewaters_us_26904 CASCADE;

CREATE MATERIALIZED VIEW cipdev_support.tiger_fedstatewaters_us_26904(
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
        cipdev_nhdplus_h.snap_to_common_grid(
           p_geometry      := a.shape
          ,p_known_region  := '26904'
          ,p_grid_size     := 0.001
        )
    )
   ,3
) AS shape   
FROM
cipsrv_support.tiger_fedstatewaters_26904 a;

ALTER TABLE cipdev_support.tiger_fedstatewaters_us_26904 OWNER TO cipdev;
GRANT SELECT ON cipdev_support.tiger_fedstatewaters_us_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS tiger_fedstatewaters_us_26904_u01
ON cipdev_support.tiger_fedstatewaters_us_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS tiger_fedstatewaters_us_26904_u02
ON cipdev_support.tiger_fedstatewaters_us_26904(globalid);

CREATE INDEX IF NOT EXISTS tiger_fedstatewaters_us_26904_spx
ON cipdev_support.tiger_fedstatewaters_us_26904 USING gist(shape);

ANALYZE cipdev_support.tiger_fedstatewaters_us_26904;

--VACUUM FREEZE ANALYZE cipdev_support.tiger_fedstatewaters_us_26904;

