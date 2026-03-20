DROP MATERIALIZED VIEW IF EXISTS cipdev_support.epa_segs_air_26904 CASCADE;

CREATE MATERIALIZED VIEW cipdev_support.epa_segs_air_26904(
    objectid
   ,name
   ,tribe_name
   ,namelsad
   ,epa_id
   ,geoid
   ,aiannhns
   ,bia_code
   ,classfp
   ,intptlat
   ,intptlon
   ,aland_mi
   ,aland_km
   ,awater_mi
   ,awater_km
   ,totalarea_mi
   ,totalarea_km
   ,current_to
   ,source
   ,state
   ,region
   ,lead_region
   ,current_use
   ,comment
   ,name_history
   ,tribalkey
   ,shape
)
AS
SELECT
 a.objectid
,a.name
,a.tribe_name
,a.namelsad
,a.epa_id
,a.geoid
,a.aiannhns
,a.bia_code
,a.classfp
,a.intptlat
,a.intptlon
,a.aland_mi
,a.aland_km
,a.awater_mi
,a.awater_km
,a.totalarea_mi
,a.totalarea_km
,a.current_to
,a.source
,a.state
,a.region
,a.lead_region
,a.current_use
,a.comment
,a.name_history
,a.tribalkey
,ST_TRANSFORM(a.shape,26904) AS shape
FROM
cipdev_support.epa_segs_air a
WHERE
    a.shape && cipsrv_nhdplus_h.generic_common_mbr('26904')
AND cipsrv_nhdplus_h.determine_grid_srid_f(a.shape) = 26904;

ALTER TABLE cipdev_support.epa_segs_air_26904 OWNER TO cipsrv;
GRANT SELECT ON cipdev_support.epa_segs_air_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS epa_segs_air_26904_pk
ON cipdev_support.epa_segs_air_26904(objectid);

CREATE INDEX IF NOT EXISTS epa_segs_air_26904_01i
ON cipdev_support.epa_segs_air_26904(epa_id);

CREATE INDEX IF NOT EXISTS epa_segs_air_26904_02i
ON cipdev_support.epa_segs_air_26904(geoid);

CREATE INDEX IF NOT EXISTS epa_segs_air_26904_spx
ON cipdev_support.epa_segs_air_26904 USING gist(shape);

ANALYZE cipdev_support.epa_segs_air_26904;
