DROP TYPE IF EXISTS cipsrv_engine.cip_feature CASCADE;

CREATE TYPE cipsrv_engine.cip_feature
AS (
    globalid              VARCHAR
   ,gtype                 VARCHAR
   ,geometry              GEOMETRY
   ,lengthkm              NUMERIC
   ,areasqkm              NUMERIC
   ,isring                BOOLEAN
   ,properties            JSONB
   ,source_featureid      VARCHAR
   ,nhdplus_version       VARCHAR
   ,known_region          VARCHAR
   ,int_srid              INTEGER
   ,point_indexing_method VARCHAR
   ,line_indexing_method  VARCHAR
   ,ring_indexing_method  VARCHAR
   ,area_indexing_method  VARCHAR
   ,line_threshold        NUMERIC
   ,areacat_threshold     NUMERIC
   ,areaevt_threshold     NUMERIC
);

GRANT usage ON TYPE cipsrv_engine.cip_feature TO public;

