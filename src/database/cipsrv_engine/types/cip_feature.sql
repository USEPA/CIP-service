DROP TYPE IF EXISTS cipsrv_engine.cip_feature CASCADE;

CREATE TYPE cipsrv_engine.cip_feature
AS (
    globalid                    VARCHAR
   ,gtype                       VARCHAR
   ,geometry                    GEOMETRY
   ,lengthkm                    NUMERIC
   ,areasqkm                    NUMERIC
   ,isring                      BOOLEAN
   ,properties                  JSONB
   ,source_featureid            VARCHAR
   ,permid_joinkey              VARCHAR
   ,nhdplus_version             VARCHAR
   ,known_region                VARCHAR
   ,int_srid                    INTEGER
   ,indexing_method_used        VARCHAR
   ,converted_to_ring           BOOLEAN
   
   ,point_indexing_method       VARCHAR
   
   ,line_indexing_method        VARCHAR
   ,line_threshold              NUMERIC
   ,line_threshold_used         NUMERIC
   
   ,ring_indexing_method        VARCHAR
   ,ring_areacat_threshold      NUMERIC
   ,ring_areacat_threshold_used NUMERIC
   ,ring_areaevt_threshold      NUMERIC
   ,ring_areaevt_threshold_used NUMERIC
   
   ,area_indexing_method        VARCHAR
   ,areacat_threshold           NUMERIC
   ,areacat_threshold_used      NUMERIC
   ,areaevt_threshold           NUMERIC
   ,areaevt_threshold_used      NUMERIC
);

GRANT USAGE ON TYPE cipsrv_engine.cip_feature TO public;

