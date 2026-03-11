DROP MATERIALIZED VIEW IF EXISTS cipdev_support.tiger_fedstatewaters_us_32655 CASCADE;

CREATE MATERIALIZED VIEW cipdev_support.tiger_fedstatewaters_us_32655(
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
          ,p_known_region  := '32655'
          ,p_grid_size     := 0.001
        )
    )
   ,3
) AS shape   
FROM
cipsrv_support.tiger_fedstatewaters_32655 a;

ALTER TABLE cipdev_support.tiger_fedstatewaters_us_32655 OWNER TO cipdev;
GRANT SELECT ON cipdev_support.tiger_fedstatewaters_us_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS tiger_fedstatewaters_us_32655_u01
ON cipdev_support.tiger_fedstatewaters_us_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS tiger_fedstatewaters_us_32655_u02
ON cipdev_support.tiger_fedstatewaters_us_32655(globalid);

CREATE INDEX IF NOT EXISTS tiger_fedstatewaters_us_32655_spx
ON cipdev_support.tiger_fedstatewaters_us_32655 USING gist(shape);

ANALYZE cipdev_support.tiger_fedstatewaters_us_32655;

--VACUUM FREEZE ANALYZE cipdev_support.tiger_fedstatewaters_us_32655;

