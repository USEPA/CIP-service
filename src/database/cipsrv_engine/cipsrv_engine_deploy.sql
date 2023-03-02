--******************************--
----- types/cip_feature.sql 

DROP TYPE IF EXISTS cipsrv_engine.cip_feature CASCADE;

CREATE TYPE cipsrv_engine.cip_feature
AS (
    globalid               VARCHAR
   ,gtype                  VARCHAR
   ,geometry               GEOMETRY
   ,lengthkm               NUMERIC
   ,areasqkm               NUMERIC
   ,isring                 BOOLEAN
   ,properties             JSONB
   ,source_featureid       VARCHAR
   ,nhdplus_version        VARCHAR
   ,known_region           VARCHAR
   ,int_srid               INTEGER
   ,indexing_method_used   VARCHAR
   ,converted_to_ring      BOOLEAN
   ,point_indexing_method  VARCHAR
   ,line_indexing_method   VARCHAR
   ,ring_indexing_method   VARCHAR
   ,area_indexing_method   VARCHAR
   ,line_threshold         NUMERIC
   ,line_threshold_used    NUMERIC
   ,areacat_threshold      NUMERIC
   ,areacat_threshold_used NUMERIC
   ,areaevt_threshold      NUMERIC
   ,areaevt_threshold_used NUMERIC
);

GRANT usage ON TYPE cipsrv_engine.cip_feature TO public;

--******************************--
----- functions/temp_table_exists.sql 

CREATE or REPLACE FUNCTION cipsrv_engine.temp_table_exists(
   IN p_table_name VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_table_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for temp table
   ----------------------------------------------------------------------------
   SELECT 
    n.nspname
   INTO str_table_name
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   where 
       n.nspname like 'pg_temp_%'
   AND pg_catalog.pg_table_is_visible(c.oid)
   AND UPPER(relname) = UPPER(p_table_name);

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_table_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.temp_table_exists(
   VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.temp_table_exists(
   VARCHAR
) TO PUBLIC;

--******************************--
----- functions/create_cip_temp_tables.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.create_cip_temp_tables()
RETURNS INT4
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_cip temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_cip')
   THEN
      TRUNCATE TABLE tmp_cip;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_cip(
         nhdplusid             BIGINT
      );

      CREATE UNIQUE INDEX tmp_cip_pk 
      ON tmp_cip(nhdplusid);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_cip_out temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_cip_out')
   THEN
      TRUNCATE TABLE tmp_cip_out;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_cip_out(
          nhdplusid            BIGINT
         ,catchmentstatecode   VARCHAR(2)
         ,xwalk_huc12          VARCHAR(12)
         ,areasqkm             NUMERIC
         ,tribal               BOOLEAN
         ,shape                GEOMETRY
      );

      CREATE UNIQUE INDEX tmp_cip_out_pk 
      ON tmp_cip_out(catchmentstatecode,nhdplusid);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.create_cip_temp_tables() OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.create_cip_temp_tables() TO PUBLIC;

--******************************--
----- functions/jsonb2feature.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.jsonb2feature(
    IN  p_feature               JSONB
   ,IN  p_geometry_override     GEOMETRY DEFAULT NULL
   ,IN  p_globalid              VARCHAR  DEFAULT NULL
   ,IN  p_source_featureid      VARCHAR  DEFAULT NULL
   ,IN  p_nhdplus_version       VARCHAR  DEFAULT NULL
   ,IN  p_known_region          VARCHAR  DEFAULT NULL
   ,IN  p_int_srid              INTEGER  DEFAULT NULL
   ,IN  p_point_indexing_method VARCHAR  DEFAULT NULL
   ,IN  p_line_indexing_method  VARCHAR  DEFAULT NULL
   ,IN  p_ring_indexing_method  VARCHAR  DEFAULT NULL
   ,IN  p_area_indexing_method  VARCHAR  DEFAULT NULL
   ,IN  p_line_threshold        NUMERIC  DEFAULT NULL
   ,IN  p_areacat_threshold     NUMERIC  DEFAULT NULL
   ,IN  p_areaevt_threshold     NUMERIC  DEFAULT NULL
) RETURNS cipsrv_engine.cip_feature[]
IMMUTABLE
AS $BODY$ 
DECLARE
   rec                       RECORD;
   obj_rez cipsrv_engine.cip_feature;
   ary_rez cipsrv_engine.cip_feature[];
   has_properties            BOOLEAN;
   boo_isring                BOOLEAN;
   str_globalid              VARCHAR;
   str_nhdplus_version       VARCHAR;
   str_known_region          VARCHAR;
   int_srid                  INTEGER;
   str_point_indexing_method VARCHAR;
   str_line_indexing_method  VARCHAR;
   str_ring_indexing_method  VARCHAR;
   str_area_indexing_method  VARCHAR;
   str_source_featureid      VARCHAR;
   num_line_threshold        NUMERIC;
   num_areacat_threshold     NUMERIC;
   num_areaevt_threshold     NUMERIC;
   sdo_geometry              GEOMETRY;
   sdo_geometry2             GEOMETRY;
   json_feature              JSONB := p_feature;
   num_line_lengthkm         NUMERIC;
   num_area_areasqkm         NUMERIC;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF json_feature IS NULL
   THEN
      RETURN ARRAY[obj_rez];
      
   ELSIF JSONB_TYPEOF(json_feature) != 'object'
   OR json_feature->'type' IS NULL
   THEN
      RAISE EXCEPTION 'input jsonb is not geojson feature';
   
   ELSE
      IF p_geometry_override IS NOT NULL
      THEN
         json_feature := JSONB_BUILD_OBJECT(
             'type'       ,'Feature'
            ,'geometry'   ,ST_AsGeoJSON(ST_Transform(p_geometry_override,4326))::JSONB
            ,'properties' ,json_feature->'properties'
         );
      
      ELSIF json_feature->>'type' IN (
          'Point'
         ,'LineString'
         ,'Polygon'
         ,'MultiPoint'
         ,'MultiLineString'
         ,'MultiPolygon'
         ,'GeometryCollection'
      )
      THEN
         -- If naked geometry, repack into feature
         json_feature := JSONB_BUILD_OBJECT(
             'type'       ,'Feature'
            ,'geometry'   ,json_feature
         );
         
      END IF;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Get quick boolean if original properties exist
   ----------------------------------------------------------------------------
   IF json_feature->'properties' IS NULL
   THEN
      has_properties := FALSE;

   ELSE
      has_properties := TRUE;

   END IF;

   ----------------------------------------------------------------------------
   -- Extract the geometry
   ----------------------------------------------------------------------------
   IF json_feature->'geometry' IS NOT NULL
   THEN
      sdo_geometry := ST_GeomFromGeoJSON(json_feature->'geometry')::JSONB;
      
      -- This fixes a bug in PostGIS 3.1.5
      IF sdo_geometry IS NOT NULL
      AND ( ST_SRID(sdo_geometry) IS NULL OR ST_SRID(sdo_geometry) = 0 )
      THEN
         sdo_geometry := ST_SetSRID(sdo_geometry,4326);
         
      END IF;
      
      -- Break up geometry collections and multilinestrings
      IF ST_GeometryType(sdo_geometry) IN (
          'ST_MultiLineString'
         ,'ST_GeometryCollection'   
      )
      THEN
         FOR i IN 1 .. ST_NumGeometries(sdo_geometry)
         LOOP
            sdo_geometry2 := ST_GeometryN(sdo_geometry,i);
            
            ary_rez := array_cat(ary_rez,
               cipsrv_engine.jsonb2feature(
                   p_feature               := json_feature
                  ,p_geometry_override     := sdo_geometry2
                  ,p_nhdplus_version       := p_nhdplus_version
                  ,p_known_region          := p_known_region
                  ,p_int_srid              := p_int_srid
                  ,p_point_indexing_method := p_point_indexing_method
                  ,p_line_indexing_method  := p_line_indexing_method
                  ,p_ring_indexing_method  := p_ring_indexing_method
                  ,p_area_indexing_method  := p_area_indexing_method
                  ,p_line_threshold        := p_line_threshold
                  ,p_areacat_threshold     := p_areacat_threshold
                  ,p_areaevt_threshold     := p_areaevt_threshold
               )
            );
            
         END LOOP;
         
         RETURN ary_rez;
 
      END IF;
      
      IF NOT ST_IsValid(sdo_geometry)
      THEN
         sdo_geometry := ST_MakeValid(sdo_geometry);
         
      END IF;

      IF ST_GeometryType(sdo_geometry) = 'ST_LineString'
      THEN
         boo_isring := ST_IsRing(sdo_geometry);
      
      ELSE
         boo_isring := FALSE;
         
      END IF;   
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for globalid override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'globalid' IS NOT NULL
   THEN
      str_globalid := json_feature->'properties'->>'globalid';
      
   ELSIF p_globalid IS NOT NULL
   THEN
      str_globalid := p_globalid;
      
   ELSE
      str_globalid := '{' || uuid_generate_v1() || '}';

   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for source_featureid override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'source_featureid' IS NOT NULL
   THEN
      str_source_featureid := json_feature->'properties'->>'source_featureid';
      
   ELSIF p_source_featureid IS NOT NULL
   THEN
      str_source_featureid := p_source_featureid;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for nhdplus_version override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'nhdplus_version' IS NOT NULL
   THEN
      str_nhdplus_version := json_feature->'properties'->>'nhdplus_version';
      
   ELSIF p_nhdplus_version IS NOT NULL
   THEN
      str_nhdplus_version := p_nhdplus_version;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for known_region override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'known_region' IS NOT NULL
   THEN
      str_known_region := json_feature->'properties'->>'known_region';
      
   ELSIF p_known_region IS NOT NULL
   THEN
      str_known_region := p_known_region;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Test for int_srid override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'int_srid' IS NOT NULL
   THEN
      int_srid := json_feature->'properties'->'int_srid';
      
   ELSIF p_int_srid IS NOT NULL
   THEN
      int_srid := p_int_srid;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Try to sort out int_srid with overrides
   ----------------------------------------------------------------------------
   IF  int_srid IS NULL
   AND str_nhdplus_version IS NOT NULL
   AND str_known_region IS NOT NULL
   THEN
      rec := cipsrv_engine.determine_grid_srid(
          p_geometry        := NULL
         ,p_nhdplus_version := str_nhdplus_version
         ,p_known_region    := str_known_region
      );
      int_srid := rec.out_srid;
      
   ELSIF int_srid IS NULL
   AND str_nhdplus_version IS NOT NULL
   AND str_known_region IS NULL
   AND sdo_geometry IS NOT NULL
   THEN
      rec := cipsrv_engine.determine_grid_srid(
          p_geometry        := sdo_geometry
         ,p_nhdplus_version := str_nhdplus_version
         ,p_known_region    := NULL
      );
      int_srid := rec.out_srid;
      str_known_region := rec.out_srid::VARCHAR;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for point indexing_method override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'point_indexing_method' IS NOT NULL
   THEN
      str_point_indexing_method := json_feature->'properties'->>'point_indexing_method';
      
   ELSIF p_point_indexing_method IS NOT NULL
   THEN
      str_point_indexing_method := p_point_indexing_method;

   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for line indexing_method override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'line_indexing_method' IS NOT NULL
   THEN
      str_line_indexing_method := json_feature->'properties'->>'line_indexing_method';
      
   ELSIF p_line_indexing_method IS NOT NULL
   THEN
      str_line_indexing_method := p_line_indexing_method;

   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for ring indexing_method override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'ring_indexing_method' IS NOT NULL
   THEN
      str_ring_indexing_method := json_feature->'properties'->>'ring_indexing_method';
      
   ELSIF p_ring_indexing_method IS NOT NULL
   THEN
      str_ring_indexing_method := p_ring_indexing_method;

   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for area indexing_method override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'area_indexing_method' IS NOT NULL
   THEN
      str_area_indexing_method := json_feature->'properties'->>'area_indexing_method';
      
   ELSIF p_area_indexing_method IS NOT NULL
   THEN
      str_area_indexing_method := p_area_indexing_method;

   END IF;
      
   ----------------------------------------------------------------------------
   -- Test for line_threshold override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'line_threshold' IS NOT NULL
   THEN
      num_line_threshold := json_feature->'properties'->'line_threshold';
      
   ELSIF p_line_threshold IS NOT NULL
   THEN
      num_line_threshold := p_line_threshold;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for areacat_threshold override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'areacat_threshold' IS NOT NULL
   THEN
      num_areacat_threshold := json_feature->'properties'->'areacat_threshold';
      
   ELSIF p_areacat_threshold IS NOT NULL
   THEN
      num_areacat_threshold := p_areacat_threshold;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Test for areaevt_threshold override
   ----------------------------------------------------------------------------
   IF has_properties
   AND json_feature->'properties'->'areaevt_threshold' IS NOT NULL
   THEN
      num_areaevt_threshold := json_feature->'properties'->'areaevt_threshold';
      
   ELSIF p_areaevt_threshold IS NOT NULL
   THEN
      num_areaevt_threshold := p_areaevt_threshold;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Measure geometry size
   ----------------------------------------------------------------------------
   IF int_srid IS NOT NULL
   AND ST_GeometryType(sdo_geometry) IN ('ST_LineString','ST_MultiLineString')
   THEN
      num_line_lengthkm := ROUND(ST_Length(ST_Transform(
          sdo_geometry
         ,int_srid
      ))::NUMERIC * 0.001,8);
               
   ELSIF int_srid IS NOT NULL
   AND ST_GeometryType(sdo_geometry) IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      num_area_areasqkm := ROUND(ST_Area(ST_Transform(
          sdo_geometry
         ,int_srid
      ))::NUMERIC / 1000000,8);
      
   END IF;

   ----------------------------------------------------------------------------
   -- Create the object
   ----------------------------------------------------------------------------
   obj_rez := (
       str_globalid
      ,ST_GeometryType(sdo_geometry)
      ,sdo_geometry
      ,num_line_lengthkm
      ,num_area_areasqkm
      ,boo_isring
      ,json_feature->'properties'
      ,str_source_featureid
      ,str_nhdplus_version
      ,str_known_region
      ,int_srid
      ,NULL
      ,NULL
      ,str_point_indexing_method
      ,str_line_indexing_method
      ,str_ring_indexing_method
      ,str_area_indexing_method
      ,num_line_threshold
      ,NULL
      ,num_areacat_threshold
      ,NULL
      ,num_areaevt_threshold
      ,NULL
   )::cipsrv_engine.cip_feature;

   RETURN ARRAY[obj_rez];   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.jsonb2feature(
    JSONB
   ,GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.jsonb2feature(
    JSONB
   ,GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/jsonb2features.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.jsonb2features(
    IN  p_features                      JSONB
   ,IN  p_nhdplus_version               VARCHAR DEFAULT NULL
   ,IN  p_known_region                  VARCHAR DEFAULT NULL
   ,IN  p_int_srid                      INTEGER DEFAULT NULL
   ,IN  p_default_point_indexing_method VARCHAR DEFAULT NULL
   ,IN  p_default_line_indexing_method  VARCHAR DEFAULT NULL
   ,IN  p_default_ring_indexing_method  VARCHAR DEFAULT NULL
   ,IN  p_default_area_indexing_method  VARCHAR DEFAULT NULL
   ,IN  p_default_line_threshold        NUMERIC DEFAULT NULL
   ,IN  p_default_areacat_threshold     NUMERIC DEFAULT NULL
   ,IN  p_default_areaevt_threshold     NUMERIC DEFAULT NULL
) RETURNS cipsrv_engine.cip_feature[]
VOLATILE
AS $BODY$ 
DECLARE
   obj_rez cipsrv_engine.cip_feature[];
   ary_rez cipsrv_engine.cip_feature[];
   str_nhdplus_version               VARCHAR;
   str_known_region                  VARCHAR;
   int_srid                          INTEGER;
   str_default_point_indexing_method VARCHAR;
   str_default_line_indexing_method  VARCHAR;
   str_default_ring_indexing_method  VARCHAR;
   str_default_area_indexing_method  VARCHAR;
   num_default_line_threshold        NUMERIC;
   num_default_areacat_threshold     NUMERIC;
   num_default_areaevt_threshold     NUMERIC;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_features IS NULL
   OR ( JSONB_TYPEOF(p_features) = 'array'
   AND JSONB_ARRAY_LENGTH(p_features) = 0 )
   THEN
      RETURN ary_rez;
      
   END IF;
   
   str_nhdplus_version               := p_nhdplus_version;
   str_known_region                  := p_known_region;
   int_srid                          := p_int_srid;
   str_default_point_indexing_method := p_default_point_indexing_method;
   str_default_line_indexing_method  := p_default_line_indexing_method;
   str_default_ring_indexing_method  := p_default_ring_indexing_method;
   str_default_area_indexing_method  := p_default_area_indexing_method;
   num_default_line_threshold        := p_default_line_threshold;
   num_default_areacat_threshold     := p_default_areacat_threshold;
   num_default_areaevt_threshold     := p_default_areaevt_threshold;
   
   ----------------------------------------------------------------------------
   -- Build the features
   ----------------------------------------------------------------------------
   IF JSONB_TYPEOF(p_features) = 'object'
   AND p_features->>'type' = 'Feature'
   THEN
      obj_rez := cipsrv_engine.jsonb2feature(
          p_feature               := p_features
         ,p_nhdplus_version       := str_nhdplus_version
         ,p_known_region          := str_known_region
         ,p_int_srid              := int_srid
         ,p_point_indexing_method := str_default_point_indexing_method
         ,p_line_indexing_method  := str_default_line_indexing_method
         ,p_ring_indexing_method  := str_default_ring_indexing_method
         ,p_area_indexing_method  := str_default_area_indexing_method
         ,p_line_threshold        := num_default_line_threshold
         ,p_areacat_threshold     := num_default_areacat_threshold
         ,p_areaevt_threshold     := num_default_areaevt_threshold
      );
      
      ary_rez := array_cat(ary_rez,obj_rez);
   
   ELSIF JSONB_TYPEOF(p_features) = 'array'
   AND p_features->>'type' = 'FeatureCollection'
   THEN
      FOR i IN 1 .. JSONB_ARRAY_LENGTH(p_features)
      LOOP
         obj_rez := cipsrv_engine.jsonb2feature(
             p_feature               := p_features->i-1
            ,p_nhdplus_version       := str_nhdplus_version
            ,p_known_region          := str_known_region
            ,p_int_srid              := int_srid
            ,p_point_indexing_method := str_default_point_indexing_method
            ,p_line_indexing_method  := str_default_line_indexing_method
            ,p_ring_indexing_method  := str_default_ring_indexing_method
            ,p_area_indexing_method  := str_default_area_indexing_method
            ,p_line_threshold        := num_default_line_threshold
            ,p_areacat_threshold     := num_default_areacat_threshold
            ,p_areaevt_threshold     := num_default_areaevt_threshold
         );
      
         ary_rez := array_cat(ary_rez,obj_rez);
   
      END LOOP;
      
   ELSE
      RAISE EXCEPTION 'input jsonb is not geojson';
   
   END IF;
   
   RETURN ary_rez;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.jsonb2features(
    JSONB
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.jsonb2features(
    JSONB
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/feature2jsonb.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.feature2jsonb(
   IN  p_feature      cipsrv_engine.cip_feature
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE
   json_rez        JSONB;
   json_properties JSONB;
   
BEGIN

   IF p_feature IS NULL
   THEN
      RETURN json_rez;
      
   END IF;
   
   json_properties := (p_feature).properties;
   
   IF json_properties IS NULL
   THEN
      json_properties := '{}'::JSONB;
      
   END IF;
   
   IF (p_feature).globalid IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['globalid']
         ,replacement       := TO_JSONB((p_feature).globalid)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).lengthkm IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['lengthkm']
         ,replacement       := TO_JSONB((p_feature).lengthkm)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).areasqkm IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['areasqkm']
         ,replacement       := TO_JSONB((p_feature).areasqkm)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).converted_to_ring IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['converted_to_ring']
         ,replacement       := TO_JSONB((p_feature).converted_to_ring)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).indexing_method_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['indexing_method_used']
         ,replacement       := TO_JSONB((p_feature).indexing_method_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).line_threshold_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['line_threshold_used']
         ,replacement       := TO_JSONB((p_feature).line_threshold_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).areacat_threshold_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['areacat_threshold_used']
         ,replacement       := TO_JSONB((p_feature).areacat_threshold_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).areaevt_threshold_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['areaevt_threshold_used']
         ,replacement       := TO_JSONB((p_feature).areaevt_threshold_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   json_rez := JSONB_BUILD_OBJECT(
       'type'       ,'Feature'
      ,'geometry'   ,ST_AsGeoJSON(ST_Transform((p_feature).geometry,4326))::JSONB
      ,'properties' ,json_properties
   );

   RETURN json_rez;   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.feature2jsonb(
   cipsrv_engine.cip_feature
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.feature2jsonb(
   cipsrv_engine.cip_feature
) TO PUBLIC;

--******************************--
----- functions/features2jsonb.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.features2jsonb(
    IN  p_features           cipsrv_engine.cip_feature[]
   ,IN  p_geometry_type      VARCHAR DEFAULT NULL
   ,IN  p_empty_collection   BOOLEAN DEFAULT FALSE
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE
   obj_rez JSONB;
   ary_rez JSONB;
   
BEGIN

   IF p_features IS NULL
   OR array_length(p_features,1) = 0
   THEN
      IF p_empty_collection
      THEN
         RETURN JSON_BUILD_OBJECT(
             'type'    , 'FeatureCollection'
            ,'features', '[]'::JSONB
         );
      
      ELSE
         RETURN NULL;
         
      END IF;
      
   END IF;
   
   FOR i IN 1 .. array_length(p_features,1)
   LOOP
      IF p_geometry_type IS NULL
      OR ( p_geometry_type IN ('P')
         AND p_features[i].gtype IN ('ST_Point','ST_MultiPoint')
      ) 
      OR ( p_geometry_type IN ('L')
         AND p_features[i].gtype IN ('ST_LineString','ST_MultiLineString')
      ) 
      OR ( p_geometry_type IN ('A')
         AND p_features[i].gtype IN ('ST_Polygon','ST_MultiPolygon')
      )
      THEN
         obj_rez := cipsrv_engine.feature2jsonb(
            p_feature := p_features[i]
         );
         
         IF ary_rez IS NULL
         THEN
            ary_rez := JSON_BUILD_ARRAY(obj_rez);
            
         ELSE
            ary_rez := ary_rez || obj_rez;
         
         END IF;
         
      END IF;
   
   END LOOP;
   
   IF ary_rez IS NULL
   OR JSONB_ARRAY_LENGTH(ary_rez) = 0
   THEN
      IF p_empty_collection
      THEN
         RETURN JSON_BUILD_OBJECT(
             'type'    , 'FeatureCollection'
            ,'features', '[]'::JSONB
         );
      
      ELSE
         RETURN NULL;
         
      END IF;
      
   ELSE
      RETURN JSON_BUILD_OBJECT(
          'type'    , 'FeatureCollection'
         ,'features', ary_rez
      );      
   
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.features2jsonb(
    cipsrv_engine.cip_feature[]
   ,VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.features2jsonb(
    cipsrv_engine.cip_feature[]
   ,VARCHAR
   ,BOOLEAN
) TO PUBLIC;

--******************************--
----- functions/features2geomcollection.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.features2geomcollection(
    IN  p_features           cipsrv_engine.cip_feature[]
) RETURNS GEOMETRY
IMMUTABLE
AS $BODY$ 
DECLARE
   sdo_rez GEOMETRY;
   
BEGIN

   IF p_features IS NULL
   OR array_length(p_features,1) = 0
   THEN
      RETURN NULL;
      
   END IF;
   
   FOR i IN 1 .. array_length(p_features,1)
   LOOP
      IF sdo_rez IS NULL
      THEN
         sdo_rez := ST_Transform(p_features[i].geometry,4326);
         
      ELSE
         sdo_rez := ST_Collect(sdo_rez,ST_Transform(p_features[i].geometry,4326));
      
      END IF;
   
   END LOOP;
   
   RETURN sdo_rez;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.features2geomcollection(
    cipsrv_engine.cip_feature[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.features2geomcollection(
    cipsrv_engine.cip_feature[]
) TO PUBLIC;

--******************************--
----- functions/preprocess2summary.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.preprocess2summary(
    IN  p_features           cipsrv_engine.cip_feature[]
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE
   obj_rez JSONB;
   int_point_count INTEGER;
   int_line_count  INTEGER;
   int_area_count  INTEGER;
   
BEGIN

   IF p_features IS NULL
   OR array_length(p_features,1) = 0
   THEN
      RETURN NULL;
      
   END IF;
   
   int_point_count := 0;
   int_line_count  := 0;
   int_area_count  := 0;
   
   FOR i IN 1 .. array_length(p_features,1)
   LOOP
      IF p_features[i].gtype IN ('ST_Point','ST_MultiPoint')
      THEN
         int_point_count := int_point_count + 1;
         
      ELSIF p_features[i].gtype IN ('ST_LineString','ST_MultiLineString')
      THEN
         int_line_count := int_line_count + 1;
         
      ELSIF p_features[i].gtype IN ('ST_Polygon','ST_MultiPolygon')
      THEN
         int_area_count := int_area_count + 1;
         
      END IF;
   
   END LOOP;
   
   obj_rez := JSONB_BUILD_OBJECT(
       'point_count', int_point_count
      ,'line_count' , int_line_count
      ,'area_count' , int_area_count
   );
   
   RETURN JSONB_BUILD_OBJECT(
       'input_features', obj_rez
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.preprocess2summary(
    cipsrv_engine.cip_feature[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.preprocess2summary(
    cipsrv_engine.cip_feature[]
) TO PUBLIC;

--******************************--
----- functions/unpackjsonb.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.unpackjsonb(
    IN  p_points                        JSONB
   ,IN  p_lines                         JSONB
   ,IN  p_areas                         JSONB
   ,IN  p_geometry                      JSONB 
   ,IN  p_nhdplus_version               VARCHAR
   ,IN  p_known_region                  VARCHAR DEFAULT NULL
   ,IN  p_int_srid                      INTEGER DEFAULT NULL
   ,IN  p_default_point_indexing_method VARCHAR DEFAULT NULL
   ,IN  p_default_line_indexing_method  VARCHAR DEFAULT NULL
   ,IN  p_default_ring_indexing_method  VARCHAR DEFAULT NULL
   ,IN  p_default_area_indexing_method  VARCHAR DEFAULT NULL
   ,IN  p_default_line_threshold        NUMERIC DEFAULT NULL
   ,IN  p_default_areacat_threshold     NUMERIC DEFAULT NULL
   ,IN  p_default_areaevt_threshold     NUMERIC DEFAULT NULL
   ,OUT out_return_code                 INTEGER
   ,OUT out_status_message              VARCHAR
   ,OUT out_features                    cipsrv_engine.cip_feature[]
)
IMMUTABLE
AS $BODY$ 
DECLARE 
   rec              RECORD;
   ary_points       cipsrv_engine.cip_feature[];
   ary_lines        cipsrv_engine.cip_feature[];
   ary_areas        cipsrv_engine.cip_feature[];
   str_known_region VARCHAR := p_known_region;
   int_srid         INTEGER := p_int_srid;
   
BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  p_points   IS NULL
   AND p_lines    IS NULL
   AND p_areas    IS NULL
   AND p_geometry IS NULL
   THEN
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Branch when geometry is provided
   ----------------------------------------------------------------------------
   IF p_geometry IS NOT NULL
   THEN
      out_features := cipsrv_engine.jsonb2feature(
          p_feature               := p_geometry
         ,p_geometry_override     := NULL
         ,p_globalid              := NULL
         ,p_source_featureid      := NULL
         ,p_nhdplus_version       := p_nhdplus_version
         ,p_known_region          := str_known_region
         ,p_int_srid              := int_srid
         ,p_point_indexing_method := p_default_point_indexing_method
         ,p_line_indexing_method  := p_default_line_indexing_method
         ,p_ring_indexing_method  := p_default_ring_indexing_method
         ,p_area_indexing_method  := p_default_area_indexing_method
         ,p_line_threshold        := p_default_line_threshold
         ,p_areacat_threshold     := p_default_areacat_threshold
         ,p_areaevt_threshold     := p_default_areaevt_threshold
      );
   
   ELSE
      ary_points := cipsrv_engine.jsonb2feature(
          p_feature               := p_points
         ,p_geometry_override     := NULL
         ,p_globalid              := NULL
         ,p_source_featureid      := NULL
         ,p_nhdplus_version       := p_nhdplus_version
         ,p_known_region          := str_known_region
         ,p_int_srid              := int_srid
         ,p_point_indexing_method := p_default_point_indexing_method
         ,p_line_indexing_method  := p_default_line_indexing_method
         ,p_ring_indexing_method  := p_default_ring_indexing_method
         ,p_area_indexing_method  := p_default_area_indexing_method
         ,p_line_threshold        := p_default_line_threshold
         ,p_areacat_threshold     := p_default_areacat_threshold
         ,p_areaevt_threshold     := p_default_areaevt_threshold
      );
      
      ary_lines := cipsrv_engine.jsonb2feature(
          p_feature               := p_lines
         ,p_geometry_override     := NULL
         ,p_globalid              := NULL
         ,p_source_featureid      := NULL
         ,p_nhdplus_version       := p_nhdplus_version
         ,p_known_region          := str_known_region
         ,p_int_srid              := int_srid
         ,p_point_indexing_method := p_default_point_indexing_method
         ,p_line_indexing_method  := p_default_line_indexing_method
         ,p_ring_indexing_method  := p_default_ring_indexing_method
         ,p_area_indexing_method  := p_default_area_indexing_method
         ,p_line_threshold        := p_default_line_threshold
         ,p_areacat_threshold     := p_default_areacat_threshold
         ,p_areaevt_threshold     := p_default_areaevt_threshold
      );
      
      ary_areas := cipsrv_engine.jsonb2feature(
          p_feature               := p_areas
         ,p_geometry_override     := NULL
         ,p_globalid              := NULL
         ,p_source_featureid      := NULL
         ,p_nhdplus_version       := p_nhdplus_version
         ,p_known_region          := str_known_region
         ,p_int_srid              := int_srid
         ,p_point_indexing_method := p_default_point_indexing_method
         ,p_line_indexing_method  := p_default_line_indexing_method
         ,p_ring_indexing_method  := p_default_ring_indexing_method
         ,p_area_indexing_method  := p_default_area_indexing_method
         ,p_line_threshold        := p_default_line_threshold
         ,p_areacat_threshold     := p_default_areacat_threshold
         ,p_areaevt_threshold     := p_default_areaevt_threshold
      );
      
      out_features := array_cat(out_features,ary_points);
      out_features := array_cat(out_features,ary_lines);
      out_features := array_cat(out_features,ary_areas);
      
   END IF;
      
   -------------------------------------------------------------------------
   -- Ring Handling
   -------------------------------------------------------------------------
   IF out_features IS NOT NULL
   AND array_length(out_features,1) > 0
   THEN
      FOR i IN 1 .. array_length(out_features,1)
      LOOP
         IF out_features[i].isRing 
         AND out_features[i].ring_indexing_method != 'treat_as_lines'
         THEN
            out_features[i].geometry := ST_MakePolygon(out_features[i].geometry);
            out_features[i].gtype    := ST_GeometryType(out_features[i].geometry);
            out_features[i].converted_to_ring := TRUE;
            out_features[i].area_indexing_method := out_features[i].ring_indexing_method;
            
            out_features[i].lengthkm := NULL;
            out_features[i].areasqkm := ROUND(ST_Area(ST_Transform(
                out_features[i].geometry
               ,out_features[i].int_srid
            ))::NUMERIC / 1000000,8);
            
         END IF;
      
      END LOOP;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.unpackjsonb(
    JSONB
   ,JSONB
   ,JSONB
   ,JSONB
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.unpackjsonb(
    JSONB
   ,JSONB
   ,JSONB
   ,JSONB
   ,VARCHAR
   ,VARCHAR
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/feature_clip.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.feature_clip(
    IN  p_features                      cipsrv_engine.cip_feature[]
   ,IN  p_clippers                      VARCHAR[]
   ,IN  p_known_region                  VARCHAR
   ,OUT out_return_code                 INTEGER
   ,OUT out_status_message              VARCHAR
   ,OUT out_features                    cipsrv_engine.cip_feature[]
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                  RECORD;
   obj_rez cipsrv_engine.cip_feature;
   ary_rez cipsrv_engine.cip_feature[];
   str_known_region     VARCHAR := p_known_region;
   sdo_output           GEOMETRY;
   
BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_features IS NULL
   OR array_length(p_features,1) = 0
   THEN
      RETURN;
      
   END IF;
   
   IF p_clippers IS NULL
   OR array_length(p_clippers,1) = 0
   THEN
      out_features := p_features;
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Loop over the features
   ----------------------------------------------------------------------------
   FOR i IN 1 .. array_length(p_features,1)
   LOOP
      obj_rez := p_features[i];
      
      rec := cipsrv_support.geometry_clip(
          p_geometry      := (obj_rez).geometry
         ,p_clippers      := p_clippers
         ,p_known_region  := str_known_region
      );
      sdo_output         := rec.out_clipped_geometry;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
         
      END IF;
      
      IF sdo_output IS NULL
      OR ST_IsEmpty(sdo_output)
      THEN
         NULL;
         
      ELSE
         obj_rez.geometry := sdo_output;
         out_features     := array_append(out_features,obj_rez);
         
      END IF;
         
   END LOOP;
   
   RETURN;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.feature_clip(
    cipsrv_engine.cip_feature[]
   ,VARCHAR[]
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.feature_clip(
    cipsrv_engine.cip_feature[]
   ,VARCHAR[]
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/json2geometry.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.json2geometry(
    IN  p_in                         JSONB
) RETURNS GEOMETRY
IMMUTABLE
AS $BODY$ 
DECLARE 
   json_feature JSONB;
   
BEGIN

   IF p_in IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF NOT JSONB_PATH_EXISTS(p_in,'$.type')
   THEN
      RETURN NULL;
      
   END IF;
   
   IF p_in->>'type' IN ('Point','LineString','Polygon','MultiPoint','MultiLineString','MultiPolygon','GeometryCollection')
   THEN
      RETURN ST_GeomFromGeoJSON(p_in);
      
   ELSIF p_in->>'type' = 'Feature'
   THEN
      IF NOT JSONB_PATH_EXISTS(p_in,'$.geometry')
      THEN
         RETURN NULL;
         
      END IF;
      
      RETURN ST_GeomFromGeoJSON(p_in->'geometry');
   
   ELSIF p_in->>'type' = 'FeatureCollection'
   THEN
      IF NOT JSONB_PATH_EXISTS(p_in,'$.features')
      OR p_in->'features' IS NULL
      OR JSONB_ARRAY_LENGTH(p_in->'features') = 0
      THEN
         RETURN NULL;
         
      END IF;
      
      json_feature := p_in->'features'->0;
      
      IF NOT JSONB_PATH_EXISTS(json_feature,'$.geometry')
      THEN
         RETURN NULL;
         
      END IF;
      
      RETURN ST_GeomFromGeoJSON(json_feature->'geometry');
   
   END IF;
   
   RETURN NULL;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.json2geometry(
   JSONB
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.json2geometry(
   JSONB
) TO PUBLIC;

--******************************--
----- functions/json2numeric.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.json2numeric(
    IN  p_in                         JSONB
) RETURNS NUMERIC
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   IF p_in IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF JSONB_TYPEOF(p_in) = 'string'
   THEN
      IF p_in::VARCHAR IN ('',' ','null','""')
      THEN
         RETURN NULL;

      ELSE            
         RETURN REPLACE(
            p_in::VARCHAR
           ,'"'
           ,''           
         )::NUMERIC;
         
      END IF;
   
   ELSE
      RETURN p_in;

   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.json2numeric(
   JSONB
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.json2numeric(
   JSONB
) TO PUBLIC;

--******************************--
----- functions/determine_grid_srid.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.determine_grid_srid(
    IN  p_geometry             GEOMETRY
   ,IN  p_nhdplus_version      VARCHAR
   ,IN  p_known_region         VARCHAR
   ,OUT out_srid               INTEGER
   ,OUT out_grid_size          NUMERIC
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec RECORD;
   
BEGIN

   IF p_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cipsrv_nhdplus_m.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      out_srid           := rec.out_srid;
      out_grid_size      := rec.out_grid_size;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
   
   ELSIF p_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cipsrv_nhdplus_h.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      out_srid           := rec.out_srid;
      out_grid_size      := rec.out_grid_size;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;

   ELSE
      RAISE EXCEPTION 'err %',p_nhdplus_version;

   END IF;   
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/parse_catchment_filter.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.parse_catchment_filter(
    IN  p_catchment_filter             VARCHAR[]
   ,OUT out_filter_by_state            BOOLEAN
   ,OUT out_state_filters              VARCHAR[]
   ,OUT out_filter_by_tribal           BOOLEAN
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
IMMUTABLE
AS $BODY$ 
DECLARE 
   rec           RECORD;
   sdo_geom      GEOMETRY;
   num_lengthkm  NUMERIC;
   num_areasqkm  NUMERIC;
   ary_states    VARCHAR[];
   
BEGIN

   out_return_code      := 0;
   out_filter_by_state  := FALSE;
   out_filter_by_tribal := FALSE;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   ary_states := ARRAY[
       'AL','AK','AS','AZ','AR','CA','CO','CT','DE','DC','FL','GA','GU','HI'
      ,'ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MO','MP'
      ,'MS','MT','NE','NV','NH','NJ','NM','NY','NC','ND','MP','OH','OK','OR'
      ,'PA','PR','RI','SC','SD','TN','TX','UT','VT','VI','VA','WA','WV','WI'
      ,'WY'
   ];
   
   IF p_catchment_filter IS NOT NULL
   AND array_length(p_catchment_filter,1) > 0
   THEN
      FOR i IN 1 .. array_length(p_catchment_filter,1)
      LOOP
         IF UPPER(p_catchment_filter[i]) IN ('ALLTRIBES','TRIBAL')
         THEN
            out_filter_by_tribal := TRUE;
            
         ELSIF UPPER(p_catchment_filter[i]) = ANY(ary_states)
         THEN
            out_filter_by_state := TRUE;
            out_state_filters := array_append(out_state_filters,UPPER(p_catchment_filter[i]));
         
         END IF;
         
      END LOOP;
      
   END IF;
   
   RETURN;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.parse_catchment_filter(
    VARCHAR[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.parse_catchment_filter(
    VARCHAR[]
) TO PUBLIC;

--******************************--
----- functions/cipsrv_index.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.cipsrv_index(
    IN  p_points                        JSONB
   ,IN  p_lines                         JSONB
   ,IN  p_areas                         JSONB
   ,IN  p_geometry                      JSONB 
   ,IN  p_geometry_clip                 VARCHAR[]
   ,IN  p_geometry_clip_stage           VARCHAR
   ,IN  p_catchment_filter              VARCHAR[]
   ,IN  p_nhdplus_version               VARCHAR
   ,IN  p_wbd_version                   VARCHAR
   ,IN  p_default_point_indexing_method VARCHAR
   ,IN  p_default_line_indexing_method  VARCHAR
   ,IN  p_default_ring_indexing_method  VARCHAR
   ,IN  p_default_area_indexing_method  VARCHAR
   ,IN  p_default_line_threshold        NUMERIC
   ,IN  p_default_areacat_threshold     NUMERIC
   ,IN  p_default_areaevt_threshold     NUMERIC
   ,IN  p_known_region                  VARCHAR
   ,IN  p_return_indexed_features       BOOLEAN
   ,IN  p_return_indexed_collection     BOOLEAN
   ,IN  p_return_catchment_geometry     BOOLEAN
   ,IN  p_return_indexing_summary       BOOLEAN
   ,OUT out_indexed_points              JSONB
   ,OUT out_indexed_lines               JSONB
   ,OUT out_indexed_areas               JSONB
   ,OUT out_indexed_collection          GEOMETRY
   ,OUT out_indexing_summary            JSONB
   ,OUT out_catchment_count             INTEGER
   ,OUT out_catchment_areasqkm          NUMERIC
   ,OUT out_return_code                 INTEGER
   ,OUT out_status_message              VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                               RECORD;
   int_srid                          INTEGER;
   ary_features                      cipsrv_engine.cip_feature[];   
   boo_return_geometry               BOOLEAN;
   
   str_default_point_indexing_method VARCHAR;
   str_default_line_indexing_method  VARCHAR;
   str_default_ring_indexing_method  VARCHAR;
   str_default_area_indexing_method  VARCHAR;
   num_default_line_threshold        NUMERIC;
   num_default_areacat_threshold     NUMERIC;
   num_default_areaevt_threshold     NUMERIC;
   
   str_known_region                  VARCHAR;
   str_geometry_clip_stage           VARCHAR;
   boo_filter_by_state               BOOLEAN;
   boo_return_indexed_features       BOOLEAN;
   boo_return_indexing_summary       BOOLEAN;
   ary_state_filters                 VARCHAR[];
   boo_filter_by_tribal              BOOLEAN;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  p_points    IS NULL
   AND p_lines     IS NULL
   AND p_areas     IS NULL
   AND p_geometry    IS NULL
   THEN
      out_return_code    := -10;
      out_status_message := 'input geometry cannot be null';
      RETURN;
   
   END IF;
   
   IF p_geometry_clip_stage IS NULL
   THEN
      str_geometry_clip_stage := 'AFTER';
      
   ELSIF UPPER(p_geometry_clip_stage) IN ('BEFORE','AFTER')
   THEN
      str_geometry_clip_stage := UPPER(p_geometry_clip_stage);
   
   END IF;
   
   rec := cipsrv_engine.parse_catchment_filter(
      p_catchment_filter := p_catchment_filter
   );
   boo_filter_by_state  := rec.out_filter_by_state;
   ary_state_filters    := rec.out_state_filters;
   boo_filter_by_tribal := rec.out_filter_by_tribal;
   
   IF p_nhdplus_version IS NULL
   THEN
      out_return_code    := -10;
      out_status_message := 'nhdplus version cannot be null';
      RETURN;
      
   ELSIF p_nhdplus_version NOT IN ('nhdplus_m','nhdplus_h') 
   THEN
      out_return_code    := -10;
      out_status_message := 'invalid nhdplus version';
      RETURN;
   
   END IF;
   
   str_default_point_indexing_method  := p_default_point_indexing_method;
   IF str_default_point_indexing_method IS NULL
   THEN
      str_default_point_indexing_method := 'point_simple';
      
   ELSIF str_default_point_indexing_method NOT IN ('point_simple')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP point indexing method';
      RETURN;
    
   END IF;
   
   str_default_line_indexing_method  := p_default_line_indexing_method;
   IF str_default_line_indexing_method IS NULL
   THEN
      str_default_line_indexing_method := 'line_simple';
      
   ELSIF str_default_line_indexing_method NOT IN ('line_simple','line_levelpath')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP line indexing method';
      RETURN;
    
   END IF;
   
   str_default_area_indexing_method  := p_default_area_indexing_method;
   IF str_default_area_indexing_method IS NULL
   THEN
      str_default_area_indexing_method := 'area_simple';
      
   ELSIF str_default_area_indexing_method NOT IN ('area_simple','area_centroid','area_artpath')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP area indexing method';
      RETURN;
    
   END IF;
   
   str_default_ring_indexing_method  := p_default_ring_indexing_method;
   IF str_default_ring_indexing_method IS NULL
   THEN
      str_default_ring_indexing_method := 'area_simple';
      
   ELSIF str_default_ring_indexing_method NOT IN ('area_simple','area_centroid','area_artpath')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP ring indexing method';
      RETURN;
    
   END IF;
   
   num_default_line_threshold := p_default_line_threshold;
   IF num_default_line_threshold IS NULL
   THEN
      num_default_line_threshold := 10;
      
   END IF;
   
   num_default_areacat_threshold := p_default_areacat_threshold;
   IF num_default_areacat_threshold IS NULL
   THEN
      num_default_areacat_threshold := 10;
      
   END IF;
   
   num_default_areaevt_threshold := p_default_areaevt_threshold;
   IF num_default_areaevt_threshold IS NULL
   THEN
      num_default_areaevt_threshold := 10;
      
   END IF;
   
   boo_return_indexed_features := p_return_indexed_features;
   IF boo_return_indexed_features IS NULL
   THEN
      boo_return_indexed_features := TRUE;
      
   END IF;
   
   boo_return_indexing_summary := p_return_indexing_summary;
   IF boo_return_indexing_summary IS NULL
   THEN
      boo_return_indexing_summary := TRUE;
      
   END IF;

   boo_return_geometry := p_return_catchment_geometry;
   IF boo_return_geometry IS NULL
   THEN
      boo_return_geometry := FALSE;
      
   END IF;
   
   str_known_region := p_known_region;
   out_return_code := cipsrv_engine.create_cip_temp_tables();
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Build features from JSONB inputs
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.unpackjsonb(
       p_points                        := p_points
      ,p_lines                         := p_lines
      ,p_areas                         := p_areas
      ,p_geometry                      := p_geometry 
      ,p_nhdplus_version               := p_nhdplus_version
      ,p_known_region                  := str_known_region
      ,p_int_srid                      := NULL
      ,p_default_point_indexing_method := str_default_point_indexing_method
      ,p_default_line_indexing_method  := str_default_line_indexing_method
      ,p_default_ring_indexing_method  := str_default_ring_indexing_method
      ,p_default_area_indexing_method  := str_default_area_indexing_method
      ,p_default_line_threshold        := num_default_line_threshold
      ,p_default_areacat_threshold     := num_default_areacat_threshold
      ,p_default_areaevt_threshold     := num_default_areaevt_threshold
   );
   out_return_code     := rec.out_return_code;
   out_status_message  := rec.out_status_message;
   ary_features        := rec.out_features;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Harvest summary data before processing
   ----------------------------------------------------------------------------
   IF boo_return_indexing_summary
   THEN
      out_indexing_summary := cipsrv_engine.preprocess2summary(
         p_features          := ary_features
      );

   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Before Clip over each feature
   ----------------------------------------------------------------------------
   IF str_geometry_clip_stage = 'BEFORE'
   AND p_geometry_clip IS NOT NULL
   AND array_length(p_geometry_clip,1) > 0
   THEN
      rec := cipsrv_engine.feature_clip(
          p_features           := ary_features
         ,p_clippers           := p_geometry_clip
         ,p_known_region       := str_known_region
      );
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      ary_features       := rec.out_features;

   END IF;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Loop over and index each feature
   ----------------------------------------------------------------------------
   IF array_length(ary_features,1) > 0
   THEN
      FOR i IN 1 .. array_length(ary_features,1)
      LOOP
         IF (ary_features[i]).gtype IN ('ST_Point','ST_MultiPoint')
         AND (ary_features[i]).point_indexing_method = 'point_simple'
         THEN
            ary_features[i].indexing_method_used := 'point_simple';
            
            IF p_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_point_simple(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_known_region           := str_known_region
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
                        
            ELSIF p_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_point_simple(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_known_region           := str_known_region
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
         ELSIF (ary_features[i]).gtype IN ('ST_LineString','ST_MultiLineString')
         AND (ary_features[i]).line_indexing_method = 'line_simple'
         THEN
            ary_features[i].indexing_method_used := 'line_simple';
            
            IF p_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_line_simple(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_lengthkm      := (ary_features[i]).lengthkm
                  ,p_known_region           := str_known_region
                  ,p_line_threashold_perc   := (ary_features[i]).line_threshold
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            ELSIF p_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_line_simple(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_lengthkm      := (ary_features[i]).lengthkm
                  ,p_known_region           := str_known_region
                  ,p_line_threashold_perc   := (ary_features[i]).line_threshold
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
            ary_features[i].line_threshold_used := (ary_features[i]).line_threshold;
            
         ELSIF (ary_features[i]).gtype IN ('ST_LineString','ST_MultiLineString')
         AND (ary_features[i]).line_indexing_method = 'line_levelpath'
         THEN
            ary_features[i].indexing_method_used := 'line_levelpath';
            
            IF p_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_line_levelpath(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_lengthkm      := (ary_features[i]).lengthkm
                  ,p_known_region           := str_known_region
                  ,p_line_threashold_perc   := (ary_features[i]).line_threshold
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            ELSIF p_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_line_levelpath(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_lengthkm      := (ary_features[i]).lengthkm
                  ,p_known_region           := str_known_region
                  ,p_line_threashold_perc   := (ary_features[i]).line_threshold
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
            ary_features[i].line_threshold_used := (ary_features[i]).line_threshold;
            
         ELSIF (ary_features[i]).gtype IN ('ST_Polygon','ST_MultiPolygon')
         AND (ary_features[i]).area_indexing_method = 'area_artpath'
         THEN
            ary_features[i].indexing_method_used := 'area_artpath';
            
            IF p_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_area_artpath(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_areasqkm      := (ary_features[i]).areasqkm
                  ,p_known_region           := str_known_region
                  ,p_cat_threashold_perc    := (ary_features[i]).areacat_threshold
                  ,p_evt_threashold_perc    := (ary_features[i]).areaevt_threshold
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
            
            ELSIF p_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_area_artpath(
                   p_geometry               := (ary_features[i]).geometry
                  ,p_geometry_areasqkm      := (ary_features[i]).areasqkm
                  ,p_known_region           := str_known_region
                  ,p_cat_threashold_perc    := (ary_features[i]).areacat_threshold
                  ,p_evt_threashold_perc    := (ary_features[i]).areaevt_threshold
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
            ary_features[i].areacat_threshold_used := (ary_features[i]).areacat_threshold;
            ary_features[i].areaevt_threshold_used := (ary_features[i]).areaevt_threshold;
            
         ELSIF (ary_features[i]).gtype IN ('ST_Polygon','ST_MultiPolygon')
         AND (ary_features[i]).area_indexing_method = 'area_simple'
         THEN
            ary_features[i].indexing_method_used := 'area_simple';
            
            IF p_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_area_simple(
                   p_geometry             := (ary_features[i]).geometry
                  ,p_geometry_areasqkm    := (ary_features[i]).areasqkm
                  ,p_known_region         := str_known_region
                  ,p_cat_threashold_perc  := (ary_features[i]).areacat_threshold
                  ,p_evt_threashold_perc  := (ary_features[i]).areaevt_threshold
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;

            ELSIF p_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_area_simple(
                   p_geometry             := (ary_features[i]).geometry
                  ,p_geometry_areasqkm    := (ary_features[i]).areasqkm
                  ,p_known_region         := str_known_region
                  ,p_cat_threashold_perc  := (ary_features[i]).areacat_threshold
                  ,p_evt_threashold_perc  := (ary_features[i]).areaevt_threshold
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
            ary_features[i].areacat_threshold_used := (ary_features[i]).areacat_threshold;
            ary_features[i].areaevt_threshold_used := (ary_features[i]).areaevt_threshold;
            
         ELSIF (ary_features[i]).gtype IN ('ST_Polygon','ST_MultiPolygon')
         AND (ary_features[i]).area_indexing_method = 'area_centroid'
         THEN
            ary_features[i].indexing_method_used := 'area_centroid';
            
            IF p_nhdplus_version = 'nhdplus_m'
            THEN
               rec := cipsrv_nhdplus_m.index_area_centroid(
                   p_geometry             := (ary_features[i]).geometry
                  ,p_geometry_areasqkm    := (ary_features[i]).areasqkm
                  ,p_known_region         := str_known_region
                  ,p_cat_threashold_perc  := (ary_features[i]).areacat_threshold
                  ,p_evt_threashold_perc  := (ary_features[i]).areaevt_threshold
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            ELSIF p_nhdplus_version = 'nhdplus_h'
            THEN
               rec := cipsrv_nhdplus_h.index_area_centroid(
                   p_geometry             := (ary_features[i]).geometry
                  ,p_geometry_areasqkm    := (ary_features[i]).areasqkm
                  ,p_known_region         := str_known_region
                  ,p_cat_threashold_perc  := (ary_features[i]).areacat_threshold
                  ,p_evt_threashold_perc  := (ary_features[i]).areaevt_threshold
               );
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
            END IF;
            
            ary_features[i].areacat_threshold_used := (ary_features[i]).areacat_threshold;
            ary_features[i].areaevt_threshold_used := (ary_features[i]).areaevt_threshold;
         
         ELSE
            RAISE EXCEPTION 'err %',(ary_features[i]).gtype;
            
         END IF;

         IF out_return_code != 0
         THEN
            RETURN;
            
         END IF;      
         
      END LOOP;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Clip AFTER indexing if requested
   ----------------------------------------------------------------------------
   IF array_length(ary_features,1) > 0
   THEN
      IF str_geometry_clip_stage = 'AFTER'
      AND p_geometry_clip IS NOT NULL
      AND array_length(p_geometry_clip,1) > 0
      THEN
         rec := cipsrv_engine.feature_clip(
             p_features           := ary_features
            ,p_clippers           := p_geometry_clip
            ,p_known_region       := str_known_region
         );
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         ary_features       := rec.out_features;
      
      END IF;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- Return filtered catchment results
   ----------------------------------------------------------------------------
   IF p_nhdplus_version = 'nhdplus_m'
   THEN
      INSERT INTO tmp_cip_out(
          nhdplusid
         ,catchmentstatecode
         ,xwalk_huc12
         ,areasqkm
         ,shape
      )
      SELECT
       a.nhdplusid
      ,a.catchmentstatecode
      ,a.xwalk_huc12_mr
      ,a.areasqkm
      ,CASE
       WHEN boo_return_geometry
       THEN
         a.shape
       ELSE
         CAST(NULL AS GEOMETRY)       
       END AS shape
      FROM
      cipsrv_nhdplus_m.catchment_fabric a
      WHERE
      EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
      AND (NOT boo_filter_by_state  OR a.catchmentstatecode = ANY(ary_state_filters) )
      AND (NOT boo_filter_by_tribal OR a.istribal = 'Y');
   
   ELSIF p_nhdplus_version = 'nhdplus_h'
   THEN
      INSERT INTO tmp_cip_out(
          nhdplusid
         ,catchmentstatecode
         ,xwalk_huc12
         ,areasqkm
         ,shape
      )
      SELECT
       a.nhdplusid
      ,a.catchmentstatecode
      ,a.xwalk_huc12_mr
      ,a.areasqkm
      ,CASE
       WHEN boo_return_geometry
       THEN
         a.shape
       ELSE
         CAST(NULL AS GEOMETRY)       
       END AS shape
      FROM
      cipsrv_nhdplus_h.catchment_fabric a
      WHERE
      EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
      AND (NOT boo_filter_by_state  OR a.catchmentstatecode = ANY(ary_state_filters) )
      AND (NOT boo_filter_by_tribal OR a.istribal = 'Y');
   
   ELSE
      RAISE EXCEPTION 'err';
   
   END IF;
   
   SELECT
    COUNT(*)
   ,SUM(a.areasqkm)
   INTO
    out_catchment_count
   ,out_catchment_areasqkm
   FROM
   tmp_cip_out a;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Build the output results
   ----------------------------------------------------------------------------
   IF array_length(ary_features,1) > 0
   THEN
      IF boo_return_indexed_features
      THEN
         out_indexed_points := cipsrv_engine.features2jsonb(
             p_features         := ary_features
            ,p_geometry_type    := 'P'
            ,p_empty_collection := FALSE
         );
         
         out_indexed_lines := cipsrv_engine.features2jsonb(
             p_features         := ary_features
            ,p_geometry_type    := 'L'
            ,p_empty_collection := FALSE
         );
         
         out_indexed_areas := cipsrv_engine.features2jsonb(
             p_features         := ary_features
            ,p_geometry_type    := 'A'
            ,p_empty_collection := FALSE
         );
      
      END IF;
      
      IF p_return_indexed_collection
      THEN
         out_indexed_collection := cipsrv_engine.features2geomcollection(
            p_features          := ary_features
         );
      
      END IF;
      
   END IF;  
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.cipsrv_index(
    JSONB
   ,JSONB
   ,JSONB
   ,JSONB
   ,VARCHAR[]
   ,VARCHAR
   ,VARCHAR[]
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,VARCHAR
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.cipsrv_index(
    JSONB
   ,JSONB
   ,JSONB
   ,JSONB
   ,VARCHAR[]
   ,VARCHAR
   ,VARCHAR[]
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,VARCHAR
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
) TO PUBLIC;

