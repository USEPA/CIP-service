DROP MATERIALIZED VIEW IF EXISTS cipdev_support.epa_segs_air_32702 CASCADE;

CREATE MATERIALIZED VIEW cipdev_support.epa_segs_air_32702(
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
,ST_TRANSFORM(a.shape,32702) AS shape
FROM
cipdev_support.epa_segs_air a
WHERE
    a.shape && cipsrv_nhdplus_h.generic_common_mbr('32702')
AND cipsrv_nhdplus_h.determine_grid_srid_f(a.shape) = 32702;

ALTER TABLE cipdev_support.epa_segs_air_32702 OWNER TO cipsrv;
GRANT SELECT ON cipdev_support.epa_segs_air_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS epa_segs_air_32702_pk
ON cipdev_support.epa_segs_air_32702(objectid);

CREATE INDEX IF NOT EXISTS epa_segs_air_32702_01i
ON cipdev_support.epa_segs_air_32702(epa_id);

CREATE INDEX IF NOT EXISTS epa_segs_air_32702_02i
ON cipdev_support.epa_segs_air_32702(geoid);

CREATE INDEX IF NOT EXISTS epa_segs_air_32702_spx
ON cipdev_support.epa_segs_air_32702 USING gist(shape);

ANALYZE cipdev_support.epa_segs_air_32702;
