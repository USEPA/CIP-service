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
----- functions/append_feature.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.append_feature(
    IN  p_in                         JSONB
   ,IN  p_feature                    JSONB
   ,IN  p_geometry                   GEOMETRY
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE 
   json_result      JSONB;
   int_count        INTEGER;
   
BEGIN

   json_result := p_in;
   
   IF json_result IS NULL
   THEN
      json_result := JSONB_BUILD_OBJECT(
          'type',    'FeatureCollection'
         ,'features', JSONB_BUILD_ARRAY()
      );
      
   END IF;
   
   int_count := JSONB_ARRAY_LENGTH(json_result->'features');
   
   IF p_feature IS NOT NULL
   THEN
      json_result := JSONB_SET(
          jsonb_in          := json_result
         ,path              := ARRAY['features',int_count::VARCHAR]
         ,replacement       := p_feature 
         ,create_if_missing := TRUE
      );   
   
   ELSIF p_geometry IS NOT NULL
   THEN
      json_result := JSONB_SET(
          jsonb_in          := json_result
         ,path              := ARRAY['features',int_count::VARCHAR]
         ,replacement       := JSONB_BUILD_OBJECT(
             'type',      'Feature'
            ,'geometry',  ST_AsGeoJSON(p_geometry)::JSONB
          ) 
         ,create_if_missing := TRUE
      );
   
   END IF;
   
   RETURN json_result;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.append_feature(
    JSONB
   ,JSONB
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.append_feature(
    JSONB
   ,JSONB
   ,GEOMETRY
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
----- functions/sample_geometry.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.sample_geometry(
    IN  p_in                         GEOMETRY
) RETURNS GEOMETRY
IMMUTABLE
AS $BODY$ 
DECLARE 
   str_geometry_type VARCHAR;
   sdo_first_geom    GEOMETRY;
   
BEGIN

   IF p_in IS NULL
   OR ST_IsEmpty(p_in)
   THEN
      RETURN NULL;
      
   END IF;
   
   str_geometry_type := ST_GeometryType(p_in);
   
   IF str_geometry_type = 'ST_Point'
   THEN
      RETURN p_in;
      
   ELSIF str_geometry_type = 'ST_LineString'
   THEN
      RETURN ST_PointN(p_in,1);
      
   ELSIF str_geometry_type = 'ST_Polygon'
   THEN
      RETURN ST_PointN(ST_ExteriorRing(p_in),1);
      
   ELSIF str_geometry_type = 'ST_MultiPoint'
   THEN
      RETURN ST_GeometryN(p_in,1);
      
   ELSIF str_geometry_type = 'ST_MultiLineString'
   THEN
      RETURN ST_PointN(ST_GeometryN(p_in,1),1);
      
   ELSIF str_geometry_type = 'ST_MultiPolygon'
   THEN
      RETURN ST_PointN(ST_ExteriorRing(ST_GeometryN(p_in,1)),1);
      
   ELSIF str_geometry_type = 'ST_MultiPolygon'
   THEN
      sdo_first_geom := ST_GeometryN(p_in,1);
      str_geometry_type := ST_GeometryType(sdo_first_geom);
   
      IF str_geometry_type = 'ST_Point'
      THEN
         RETURN sdo_first_geom;
         
      ELSIF str_geometry_type = 'ST_LineString'
      THEN
         RETURN ST_PointN(sdo_first_geom,1);
         
      ELSIF str_geometry_type = 'ST_Polygon'
      THEN
         RETURN ST_PointN(ST_ExteriorRing(sdo_first_geom),1);
         
      END IF;
   
   ELSE
      RAISE EXCEPTION 'err %',str_geometry_type;
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.sample_geometry(
   GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.sample_geometry(
   GEOMETRY
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
----- functions/validate_feature.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.validate_feature(
    IN  p_in                           JSONB
   ,IN  p_category                     VARCHAR
   ,IN  p_known_region_srid            INTEGER
   ,IN  p_default_point_method         VARCHAR
   ,IN  p_default_line_method          VARCHAR
   ,IN  p_default_ring_method          VARCHAR
   ,IN  p_default_area_method          VARCHAR
   ,IN  p_default_line_threshold       NUMERIC
   ,IN  p_default_areacat_threshold    NUMERIC
   ,IN  p_default_areaevt_threshold    NUMERIC
   ,OUT out_cleaned_feature            JSONB
   ,OUT out_property_count             INTEGER
   ,OUT out_geometry_type              VARCHAR
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
   
BEGIN

   out_return_code := 0;
   out_cleaned_feature := p_in;

   IF JSONB_PATH_EXISTS(
       target := out_cleaned_feature
      ,path   := '$.type'
   )
   THEN
      IF out_cleaned_feature->>'type' = 'Feature' 
      THEN
         IF NOT JSONB_PATH_EXISTS(
             target := out_cleaned_feature
            ,path   := '$.properties'
         )
         THEN
            out_cleaned_feature := JSONB_SET(
                jsonb_in          := out_cleaned_feature
               ,path              := ARRAY['properties']
               ,replacement       := JSONB_BUILD_OBJECT()
               ,create_if_missing := TRUE
            );
         
         END IF;
         
         IF JSONB_PATH_EXISTS(
             target := out_cleaned_feature
            ,path   := '$.geometry'
         )
         THEN
            sdo_geom := ST_GeomFromGeoJSON(out_cleaned_feature->'geometry');
            out_geometry_type := out_cleaned_feature->'geometry'->>'type';
            
            IF NOT ST_IsValid(sdo_geom)
            THEN
               sdo_geom := ST_MakeValid(sdo_geom);
               
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['geometry']
                  ,replacement       := ST_AsGeoJSON(sdo_geom)::JSONB
                  ,create_if_missing := FALSE
               );
               
            END IF;
            
            IF  ST_GeometryType(sdo_geom) = 'ST_LineString'
            AND ST_IsRing(sdo_geom)
            THEN
               sdo_geom := ST_MakePolygon(sdo_geom);
               
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['geometry']
                  ,replacement       := ST_AsGeoJSON(sdo_geom)::JSONB
                  ,create_if_missing := FALSE
               );
               
               IF NOT JSONB_PATH_EXISTS(
                   target := out_cleaned_feature
                  ,path   := '$.properties.indexing_method'
               )
               OR UPPER(SUBSTR(out_cleaned_feature->'properties'->>'indexing_method',1,4)) != 'AREA'
               THEN
                  out_cleaned_feature := JSONB_SET(
                      jsonb_in          := out_cleaned_feature
                     ,path              := ARRAY['properties','indexing_method']
                     ,replacement       := TO_JSONB(p_default_ring_method)
                     ,create_if_missing := TRUE
                  );
                  
               END IF;
            
            END IF;
            
         ELSE
            out_return_code    := -20;
            out_status_message := 'no geometry included in feature record';
            RETURN;
            
         END IF;
         
         IF NOT JSONB_PATH_EXISTS(
             target := out_cleaned_feature
            ,path   := '$.properties.globalid'
         )
         THEN
            out_cleaned_feature := JSONB_SET(
                jsonb_in          := out_cleaned_feature
               ,path              := ARRAY['properties','globalid']
               ,replacement       := TO_JSONB('{' || uuid_generate_v1() || '}') 
               ,create_if_missing := TRUE
            );
            
         END IF;
           
         IF NOT JSONB_PATH_EXISTS(
             target := out_cleaned_feature
            ,path   := '$.properties.indexing_method'
         )
         THEN
            IF out_geometry_type IN ('Point','MultiPoint')
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','indexing_method']
                  ,replacement       := TO_JSONB(p_default_point_method)
                  ,create_if_missing := TRUE
               );
               
            ELSIF out_geometry_type IN ('LineString','MultiLineString')
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','indexing_method']
                  ,replacement       := TO_JSONB(p_default_line_method) 
                  ,create_if_missing := TRUE
               );
               
            ELSIF out_geometry_type IN ('Polygon','MultiPolygon')
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','indexing_method']
                  ,replacement       := TO_JSONB(p_default_area_method) 
                  ,create_if_missing := TRUE
               );
               
            ELSE
               RAISE EXCEPTION '%',out_geometry_type;
            
            END IF;
            
         END IF;
         
         IF out_geometry_type IN ('LineString','MultiLineString')
         THEN
            IF NOT JSONB_PATH_EXISTS(
                target := out_cleaned_feature
               ,path   := '$.properties.line_threshold'
            ) AND p_default_line_threshold IS NOT NULL
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','line_threshold']
                  ,replacement       := TO_JSONB(p_default_line_threshold)
                  ,create_if_missing := TRUE
               );

            END IF;
            
            num_lengthkm := ROUND(ST_Length(ST_Transform(sdo_geom,p_known_region_srid))::NUMERIC * 0.001,8);
            out_cleaned_feature := JSONB_SET(
                jsonb_in          := out_cleaned_feature
               ,path              := ARRAY['properties','lengthkm']
               ,replacement       := TO_JSONB(num_lengthkm)
               ,create_if_missing := TRUE
            );
            
         END IF;
         
         IF out_geometry_type IN ('Polygon','MultiPolygon')
         THEN
            IF NOT JSONB_PATH_EXISTS(
                target := out_cleaned_feature
               ,path   := '$.properties.areacat_threshold'
            ) AND p_default_areacat_threshold IS NOT NULL
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','areacat_threshold']
                  ,replacement       := TO_JSONB(p_default_areacat_threshold) 
                  ,create_if_missing := TRUE
               );
               
            END IF;
         
            IF NOT JSONB_PATH_EXISTS(
                target := out_cleaned_feature
               ,path   := '$.properties.areaevt_threshold'
            ) AND p_default_areaevt_threshold IS NOT NULL
            THEN
               out_cleaned_feature := JSONB_SET(
                   jsonb_in          := out_cleaned_feature
                  ,path              := ARRAY['properties','areaevt_threshold']
                  ,replacement       := TO_JSONB(p_default_areaevt_threshold) 
                  ,create_if_missing := TRUE
               );
               
            END IF;
            
            num_areasqkm := ROUND(ST_Area(ST_Transform(sdo_geom,p_known_region_srid))::NUMERIC / 1000000,8);
            out_cleaned_feature := JSONB_SET(
                jsonb_in          := out_cleaned_feature
               ,path              := ARRAY['properties','areasqkm']
               ,replacement       := TO_JSONB(num_areasqkm)
               ,create_if_missing := TRUE
            );
            
         END IF;
         
         out_cleaned_feature := JSONB_SET(
             jsonb_in          := out_cleaned_feature
            ,path              := ARRAY['properties','category']
            ,replacement       := TO_JSONB(p_category) 
            ,create_if_missing := TRUE
         );

      ELSE
         out_return_code    := -10;
         out_status_message := 'invalid GeoJSON feature';
         RETURN;
         
      END IF;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'invalid GeoJSON feature';
      RETURN;
   
   END IF;

EXCEPTION
   WHEN OTHERS
   THEN
      RAISE;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.validate_feature(
    JSONB
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

GRANT EXECUTE ON FUNCTION cipsrv_engine.validate_feature(
    JSONB
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
----- functions/validate_feature_collection.sql 

CREATE OR REPLACE FUNCTION cipsrv_engine.validate_feature_collection(
    IN  p_in                           JSONB
   ,IN  p_test_type                    VARCHAR
   ,IN  p_category                     VARCHAR
   ,IN  p_known_region_srid            INTEGER
   ,IN  p_default_point_method         VARCHAR
   ,IN  p_default_line_method          VARCHAR
   ,IN  p_default_ring_method          VARCHAR
   ,IN  p_default_area_method          VARCHAR
   ,IN  p_default_line_threshold       NUMERIC
   ,IN  p_default_areacat_threshold    NUMERIC
   ,IN  p_default_areaevt_threshold    NUMERIC
   ,OUT out_cleaned_feature_collection JSONB
   ,OUT out_feature_count              INTEGER
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
IMMUTABLE
AS $BODY$ 
DECLARE 
   rec              RECORD;
   json_feature     JSONB;
   str_feature_type VARCHAR;
   
BEGIN

   out_return_code := 0;
   out_cleaned_feature_collection := p_in;
   
   IF JSONB_PATH_EXISTS(out_cleaned_feature_collection,'$.type')
   THEN
      IF out_cleaned_feature_collection->>'type' IN (
          'Point'
         ,'LineString'
         ,'Polygon'
         ,'MultiPoint'
         ,'MultiLineString'
         ,'MultiPolygon'
         ,'GeometryCollection'
      )
      THEN
         out_cleaned_feature_collection := JSONB_BUILD_OBJECT(
             'type',     'Feature'
            ,'geometry', out_cleaned_feature_collection
         );
      
      END IF;

      IF out_cleaned_feature_collection->>'type' = 'Feature'
      THEN
         out_cleaned_feature_collection := JSONB_BUILD_OBJECT(
             'type',     'FeatureCollection'
            ,'features', JSONB_BUILD_ARRAY(
               out_cleaned_feature_collection
             )
         );
      
      END IF;

      IF  out_cleaned_feature_collection->>'type' = 'FeatureCollection' 
      AND JSONB_PATH_EXISTS(out_cleaned_feature_collection,'$.features')
      THEN
         IF JSONB_TYPEOF(out_cleaned_feature_collection->'features') = 'array'
         THEN
            out_feature_count := JSONB_ARRAY_LENGTH(out_cleaned_feature_collection->'features');

            FOR i IN 0 .. out_feature_count - 1
            LOOP
               rec := cipsrv_engine.validate_feature(
                   p_in                        := out_cleaned_feature_collection->'features'->i
                  ,p_category                  := p_category
                  ,p_known_region_srid         := p_known_region_srid
                  ,p_default_point_method      := p_default_point_method
                  ,p_default_line_method       := p_default_line_method
                  ,p_default_area_method       := p_default_area_method
                  ,p_default_ring_method       := p_default_ring_method
                  ,p_default_line_threshold    := p_default_line_threshold
                  ,p_default_areacat_threshold := p_default_areacat_threshold
                  ,p_default_areaevt_threshold := p_default_areaevt_threshold 
               );
               json_feature       := rec.out_cleaned_feature;
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               str_feature_type   := rec.out_geometry_type;

               IF out_return_code != 0
               THEN
                  RETURN;
                  
               END IF;
               
               IF p_test_type IS NOT NULL
               THEN
                  -- Note this needs to be loose enough to allow ring polygons
                  IF (p_test_type = 'Point'      AND str_feature_type NOT IN ('Point','MultiPoint'))
                  OR (p_test_type = 'LineString' AND str_feature_type NOT IN ('LineString','MultiLineString','ST_Polygon'))
                  OR (p_test_type = 'Polygon'    AND str_feature_type NOT IN ('Polygon','MultiPolygon'))
                  THEN
                     out_return_code    := -30;
                     out_status_message := 'geometry type ' || str_feature_type || ' does not match test type ' || p_test_type;
                     RETURN;
                     
                  END IF;
               
               END IF;

               out_cleaned_feature_collection := JSONB_SET(
                   out_cleaned_feature_collection
                  ,ARRAY['features',i::VARCHAR]
                  ,json_feature
                  ,FALSE
               );

               IF NOT JSONB_PATH_EXISTS(
                   target := out_cleaned_feature_collection
                  ,path   := '$.globalid'
               )
               THEN
                  out_cleaned_feature_collection := JSONB_SET(
                      out_cleaned_feature_collection
                     ,ARRAY['globalid']
                     ,TO_JSONB('{' || uuid_generate_v1() || '}') 
                     ,TRUE
                  );
                  
               END IF;
               
            END LOOP;
         
         ELSE
            out_return_code    := -10;
            out_status_message := 'invalid GeoJSON feature collection';
      
         END IF;
      
      ELSE
         out_return_code    := -10;
         out_status_message := 'invalid GeoJSON feature collection';
      
      END IF;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'invalid GeoJSON feature collection';
   
   END IF;
 
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.validate_feature_collection(
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

GRANT EXECUTE ON FUNCTION cipsrv_engine.validate_feature_collection(
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
    IN  p_points                    JSONB
   ,IN  p_lines                     JSONB
   ,IN  p_areas                     JSONB
   ,IN  p_geometry                  GEOMETRY  
   ,IN  p_geometry_clip             VARCHAR[]
   ,IN  p_geometry_clip_stage       VARCHAR
   ,IN  p_catchment_filter          VARCHAR[]
   ,IN  p_nhdplus_version           VARCHAR
   ,IN  p_wbd_version               VARCHAR
   ,IN  p_default_point_method      VARCHAR
   ,IN  p_default_line_method       VARCHAR
   ,IN  p_default_ring_method       VARCHAR
   ,IN  p_default_area_method       VARCHAR
   ,IN  p_default_line_threshold    NUMERIC
   ,IN  p_default_areacat_threshold NUMERIC
   ,IN  p_default_areaevt_threshold NUMERIC
   ,IN  p_known_region              VARCHAR
   ,IN  p_return_catchment_geometry BOOLEAN
   ,OUT out_indexed_points          JSONB
   ,OUT out_indexed_lines           JSONB
   ,OUT out_indexed_areas           JSONB
   ,OUT out_indexed_collection      GEOMETRY
   ,OUT out_indexing_summary        JSONB
   ,OUT out_catchment_count         INTEGER
   ,OUT out_catchment_areasqkm      NUMERIC
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                           RECORD;
   str_gtype                     VARCHAR;
   int_meassrid                  INTEGER;
   int_gridsize                  INTEGER;
   
   json_points                   JSONB;
   json_lines                    JSONB;
   json_areas                    JSONB;
   json_features                 JSONB;
   json_features2                JSONB;
   json_feature                  JSONB;
   
   sdo_input                     GEOMETRY;
   sdo_output                    GEOMETRY;
   sdo_sampler                   GEOMETRY;
   sdo_state_clipped             GEOMETRY;
   boo_return_geometry           BOOLEAN;
   
   str_default_point_method      VARCHAR;
   str_default_line_method       VARCHAR;
   str_default_ring_method       VARCHAR;
   str_default_area_method       VARCHAR;
   num_default_line_threshold    NUMERIC;
   num_default_areacat_threshold NUMERIC;
   num_default_areaevt_threshold NUMERIC;
   
   str_known_region              VARCHAR;
   str_geometry_type             VARCHAR;
   int_feature_count             INTEGER;
   str_indexing_method           VARCHAR;
   num_lengthkm                  NUMERIC;
   num_areasqkm                  NUMERIC;
   num_line_threshold            NUMERIC;
   num_areacat_threshold         NUMERIC;
   num_areaevt_threshold         NUMERIC;
   str_category                  VARCHAR;
   boo_check                     BOOLEAN;
   
   str_geometry_clip_stage       VARCHAR;
   boo_filter_by_state           BOOLEAN;
   ary_state_filters             VARCHAR[];
   boo_filter_by_tribal          BOOLEAN;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   json_points := p_points;
   json_lines  := p_lines;
   json_areas  := p_areas;
   IF  json_points   IS NULL
   AND json_lines    IS NULL
   AND json_areas    IS NULL
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
   
   str_default_point_method  := p_default_point_method;
   IF str_default_point_method IS NULL
   THEN
      str_default_point_method := 'point_simple';
      
   ELSIF str_default_point_method NOT IN ('point_simple')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP point indexing method';
      RETURN;
    
   END IF;
   
   str_default_line_method  := p_default_line_method;
   IF str_default_line_method IS NULL
   THEN
      str_default_line_method := 'line_simple';
      
   ELSIF str_default_line_method NOT IN ('line_simple','line_levelpath')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP line indexing method';
      RETURN;
    
   END IF;
   
   str_default_area_method  := p_default_area_method;
   IF str_default_area_method IS NULL
   THEN
      str_default_area_method := 'area_simple';
      
   ELSIF str_default_area_method NOT IN ('area_simple','area_centroid','area_artpath')
   THEN
      out_return_code    := -10;
      out_status_message := 'unknown CIP area indexing method';
      RETURN;
    
   END IF;
   
   str_default_ring_method  := p_default_ring_method;
   IF str_default_ring_method IS NULL
   THEN
      str_default_ring_method := 'area_simple';
      
   ELSIF str_default_ring_method NOT IN ('area_simple','area_centroid','area_artpath')
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
   
   boo_return_geometry := p_return_catchment_geometry;
   IF boo_return_geometry IS NULL
   THEN
      boo_return_geometry := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine measurement srid
   ----------------------------------------------------------------------------
   str_known_region := p_known_region;
   
   IF str_known_region IS NULL
   THEN
      IF p_geometry IS NOT NULL
      THEN
         sdo_sampler := cipsrv_engine.sample_geometry(p_in := p_geometry);

      ELSE
         IF json_points IS NOT NULL
         THEN
            sdo_sampler := cipsrv_engine.sample_geometry(
               p_in := ST_GeomFromGeoJSON(json_points->'features'->0->'geometry')
            );
            
         ELSE
            IF json_lines IS NOT NULL
            THEN
               sdo_sampler := cipsrv_engine.sample_geometry(
                  p_in := ST_GeomFromGeoJSON(json_lines->'features'->0->'geometry')
               );
               
            ELSE
               sdo_sampler := cipsrv_engine.sample_geometry(
                  p_in := ST_GeomFromGeoJSON(json_areas->'features'->0->'geometry')
               );
               
            END IF;
            
         END IF;

      END IF;
      
   END IF;

   rec := cipsrv_engine.determine_grid_srid(
       p_geometry        := sdo_sampler
      ,p_nhdplus_version := p_nhdplus_version
      ,p_known_region    := str_known_region
   );
   int_meassrid       := rec.out_srid;
   int_gridsize       := rec.out_grid_size;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_meassrid::VARCHAR;

   ----------------------------------------------------------------------------
   -- Step 30
   -- Validate incoming geometry workload
   ----------------------------------------------------------------------------
   IF p_geometry IS NOT NULL
   THEN
      json_points := NULL;
      json_lines  := NULL;
      json_areas  := NULL;
      
      str_geometry_type := ST_GeometryType(p_geometry);
      IF str_geometry_type NOT IN ('GeometryCollection')
      THEN
         IF str_geometry_type IN ('ST_Point','ST_MultiPoint')
         THEN
            json_points := cipsrv_engine.append_feature(
                p_in         := json_points
               ,p_feature    := NULL
               ,p_geometry   := p_geometry
            );
            
         ELSIF str_geometry_type IN ('ST_LineString','ST_MultiLineString')
         THEN
            json_lines := cipsrv_engine.append_feature(
                p_in         := json_lines
               ,p_feature    := NULL
               ,p_geometry   := p_geometry
            );
            
         ELSIF str_geometry_type IN ('ST_Polygon','ST_MultiPolygon')
         THEN
            json_areas := cipsrv_engine.append_feature(
                p_in         := json_areas
               ,p_feature    := NULL
               ,p_geometry   := p_geometry
            );
         
         ELSE
            RAISE EXCEPTION 'err %',str_geometry_type;
         
         END IF;
      
      ELSE
         FOR i IN 1 .. ST_NumGeometries(p_geometry)
         LOOP
            sdo_input := ST_Transform(ST_GeometryN(p_geometry,i),4326);
            str_geometry_type := ST_GeometryType(sdo_input);
    
            IF str_geometry_type = 'ST_Point'
            THEN
               json_points := cipsrv_engine.append_feature(
                   p_in         := json_points
                  ,p_feature    := NULL
                  ,p_geometry   := sdo_input
               );
               
            ELSIF str_geometry_type = 'ST_LineString'
            THEN
               json_lines := cipsrv_engine.append_feature(
                   p_in         := json_lines
                  ,p_feature    := NULL
                  ,p_geometry   := sdo_input
               );
               
            ELSIF str_geometry_type = 'ST_Polygon'
            THEN
               json_areas := cipsrv_engine.append_feature(
                   p_in         := json_areas
                  ,p_feature    := NULL
                  ,p_geometry   := sdo_input
               );
            
            ELSE
               RAISE EXCEPTION 'err %',str_geometry_type;
            
            END IF;
            
         END LOOP;
         
      END IF;

   END IF;

   IF json_points IS NOT NULL
   THEN
      rec := cipsrv_engine.validate_feature_collection(
          p_in                        := json_points
         ,p_test_type                 := 'Point'
         ,p_category                  := 'points'
         ,p_known_region_srid         := int_meassrid
         ,p_default_point_method      := str_default_point_method
         ,p_default_line_method       := str_default_line_method
         ,p_default_ring_method       := str_default_ring_method
         ,p_default_area_method       := str_default_area_method
         ,p_default_line_threshold    := num_default_line_threshold
         ,p_default_areacat_threshold := num_default_areacat_threshold
         ,p_default_areaevt_threshold := num_default_areaevt_threshold 
      );
      json_points        := rec.out_cleaned_feature_collection;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
      
      END IF;         
      
   END IF;

   IF json_lines IS NOT NULL
   THEN
      rec := cipsrv_engine.validate_feature_collection(
          p_in                        := json_lines
         ,p_test_type                 := 'LineString'
         ,p_category                  := 'lines'
         ,p_known_region_srid         := int_meassrid
         ,p_default_point_method      := str_default_point_method
         ,p_default_line_method       := str_default_line_method
         ,p_default_ring_method       := str_default_ring_method
         ,p_default_area_method       := str_default_area_method
         ,p_default_line_threshold    := num_default_line_threshold
         ,p_default_areacat_threshold := num_default_areacat_threshold
         ,p_default_areaevt_threshold := num_default_areaevt_threshold 
      );
      json_lines         := rec.out_cleaned_feature_collection;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;

      IF out_return_code != 0
      THEN
         RETURN;
      
      END IF;
      
   END IF;
   
   IF json_areas IS NOT NULL
   THEN
      rec := cipsrv_engine.validate_feature_collection(
          p_in                        := json_areas
         ,p_test_type                 := 'Polygon'
         ,p_category                  := 'areas'
         ,p_known_region_srid         := int_meassrid
         ,p_default_point_method      := str_default_point_method
         ,p_default_line_method       := str_default_line_method
         ,p_default_ring_method       := str_default_ring_method
         ,p_default_area_method       := str_default_area_method
         ,p_default_line_threshold    := num_default_line_threshold
         ,p_default_areacat_threshold := num_default_areacat_threshold
         ,p_default_areaevt_threshold := num_default_areaevt_threshold 
      );
      json_areas         := rec.out_cleaned_feature_collection;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
      
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Load incoming geometry events into features
   ----------------------------------------------------------------------------
   out_return_code := cipsrv_engine.create_cip_temp_tables();
   json_features := NULL;   
   
   IF json_points IS NOT NULL
   THEN
      IF json_features IS NULL
      OR JSONB_ARRAY_LENGTH(json_features) = 0
      THEN
         json_features := json_points->'features';
      
      ELSE
         json_features := json_features || (json_points->'features');
         
      END IF;
      
   END IF;
 
   IF json_lines IS NOT NULL
   THEN
      IF json_features IS NULL
      OR JSONB_ARRAY_LENGTH(json_features) = 0
      THEN
         json_features := json_lines->'features';
      
      ELSE
         json_features := json_features || (json_lines->'features');
         
      END IF;
      
   END IF;
   
   IF json_areas IS NOT NULL
   THEN
      IF json_features IS NULL
      OR JSONB_ARRAY_LENGTH(json_features) = 0
      THEN
         json_features := json_areas->'features';
      
      ELSE
         json_features := json_features || (json_areas->'features');
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Before Clip over each feature
   ----------------------------------------------------------------------------
   IF str_geometry_clip_stage = 'BEFORE'
   AND p_geometry_clip IS NOT NULL
   AND array_length(p_geometry_clip,1) > 0
   THEN
      json_features2 := NULL;
      int_feature_count := JSONB_ARRAY_LENGTH(json_features);

      FOR i IN 0 .. int_feature_count - 1
      LOOP
         sdo_input := ST_GeomFromGeoJSON(json_features->i->'geometry');
         
         rec := cipsrv_support.geometry_clip(
             p_geometry      := sdo_input
            ,p_clippers      := p_geometry_clip
            ,p_known_region  := str_known_region
         );
         sdo_output      := rec.out_clipped_geometry;
         out_return_code := rec.out_return_code;
         out_status_message := rec.out_status_message;
         
         IF out_return_code != 0
         THEN
            RETURN;
            
         END IF;
         
         IF sdo_output IS NOT NULL
         THEN
            json_features[i]['geometry'] := ST_AsGeoJSON(sdo_output);
            
            IF json_features2 IS NULL
            OR JSONB_ARRAY_LENGTH(json_features2) = 0
            THEN
               json_features2 := '[]'::JSONB;
               
            END IF;
            
            json_features2 := json_features2 || json_features->i;
            
         END IF;         
      
      END LOOP;
      
      IF json_features2 IS NULL
      OR JSONB_ARRAY_LENGTH(json_features2) = 0
      THEN
         out_return_code    := 0;
         out_status_message := 'No results from clipping';
         RETURN;
         
      END IF;
      
      json_features  := json_features2;
      json_features2 := NULL;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Loop over and index each feature
   ----------------------------------------------------------------------------
   int_feature_count := JSONB_ARRAY_LENGTH(json_features);

   FOR i IN 0 .. int_feature_count - 1
   LOOP
      str_indexing_method   := json_features->i->'properties'->>'indexing_method';
      sdo_input             := ST_GeomFromGeoJSON(json_features->i->'geometry');
      num_lengthkm          := json_features->i->'properties'->>'lengthkm';
      num_areasqkm          := json_features->i->'properties'->>'areasqkm';
      num_line_threshold    := json_features->i->'properties'->>'line_threshold';
      num_areacat_threshold := json_features->i->'properties'->>'areacat_threshold';
      num_areaevt_threshold := json_features->i->'properties'->>'areaevt_threshold';

      IF str_indexing_method = 'point_simple'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cipsrv_nhdplus_m.index_point_simple(
                p_geometry               := sdo_input
               ,p_known_region           := str_known_region
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
                     
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cipsrv_nhdplus_h.index_point_simple(
                p_geometry               := sdo_input
               ,p_known_region           := str_known_region
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
         
      ELSIF str_indexing_method = 'line_simple'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cipsrv_nhdplus_m.index_line_simple(
                p_geometry               := sdo_input
               ,p_geometry_lengthkm      := num_lengthkm
               ,p_known_region           := str_known_region
               ,p_line_threashold_perc   := num_line_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cipsrv_nhdplus_h.index_line_simple(
                p_geometry               := sdo_input
               ,p_geometry_lengthkm      := num_lengthkm
               ,p_known_region           := str_known_region
               ,p_line_threashold_perc   := num_line_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
         
      ELSIF str_indexing_method = 'line_levelpath'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cipsrv_nhdplus_m.index_line_levelpath(
                p_geometry               := sdo_input
               ,p_geometry_lengthkm      := num_lengthkm
               ,p_known_region           := str_known_region
               ,p_line_threashold_perc   := num_line_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cipsrv_nhdplus_h.index_line_levelpath(
                p_geometry               := sdo_input
               ,p_geometry_lengthkm      := num_lengthkm
               ,p_known_region           := str_known_region
               ,p_line_threashold_perc   := num_line_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
         
      ELSIF str_indexing_method = 'area_artpath'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cipsrv_nhdplus_m.index_area_artpath(
                p_geometry               := sdo_input
               ,p_geometry_areasqkm      := num_areasqkm
               ,p_known_region           := str_known_region
               ,p_cat_threashold_perc    := num_areacat_threshold
               ,p_evt_threashold_perc    := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
         
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cipsrv_nhdplus_h.index_area_artpath(
                p_geometry               := sdo_input
               ,p_geometry_areasqkm      := num_areasqkm
               ,p_known_region           := str_known_region
               ,p_cat_threashold_perc    := num_areacat_threshold
               ,p_evt_threashold_perc    := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
         
      ELSIF str_indexing_method = 'area_simple'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cipsrv_nhdplus_m.index_area_simple(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := num_areasqkm
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := num_areacat_threshold
               ,p_evt_threashold_perc  := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;

         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cipsrv_nhdplus_h.index_area_simple(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := num_areasqkm
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := num_areacat_threshold
               ,p_evt_threashold_perc  := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
         
      ELSIF str_indexing_method = 'area_centroid'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cipsrv_nhdplus_m.index_area_centroid(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := num_areasqkm
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := num_areacat_threshold
               ,p_evt_threashold_perc  := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cipsrv_nhdplus_h.index_area_centroid(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := num_areasqkm
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := num_areacat_threshold
               ,p_evt_threashold_perc  := num_areaevt_threshold
            );
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
         END IF;
      
      ELSE
         RAISE EXCEPTION 'err %',str_indexing_method;
         
      END IF;

      IF out_return_code != 0
      THEN
         RETURN;
         
      END IF;      
      
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Clip AFTER indexing if requested
   ----------------------------------------------------------------------------
   IF str_geometry_clip_stage = 'AFTER'
   AND p_geometry_clip IS NOT NULL
   AND array_length(p_geometry_clip,1) > 0
   THEN
      json_features2 := NULL;
      int_feature_count := JSONB_ARRAY_LENGTH(json_features);

      FOR i IN 0 .. int_feature_count - 1
      LOOP
         sdo_input := ST_GeomFromGeoJSON(json_features->i->'geometry');
         
         rec := cipsrv_support.geometry_clip(
             p_geometry      := sdo_input
            ,p_clippers      := p_geometry_clip
            ,p_known_region  := str_known_region
         );
         sdo_output      := rec.out_clipped_geometry;
         out_return_code := rec.out_return_code;
         out_status_message := rec.out_status_message;
         
         IF out_return_code != 0
         THEN
            RETURN;
            
         END IF;
         
         IF sdo_output IS NOT NULL
         THEN
            json_features[i]['geometry'] := ST_AsGeoJSON(sdo_output);
            
            IF json_features2 IS NULL
            OR JSONB_ARRAY_LENGTH(json_features2) = 0
            THEN
               json_features2 := '[]'::JSONB;
               
            END IF;
            
            json_features2 := json_features2 || json_features->i;
            
         END IF;         
      
      END LOOP;
      
      IF json_features2 IS NULL
      OR JSONB_ARRAY_LENGTH(json_features2) = 0
      THEN
         out_return_code    := 0;
         out_status_message := 'No results from clipping';
         RETURN;
         
      END IF;
      
      json_features  := json_features2;
      json_features2 := NULL;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 80
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
   int_feature_count := JSONB_ARRAY_LENGTH(json_features);
      
   FOR i IN 0 .. int_feature_count - 1
   LOOP
      json_feature := json_features->i;
      str_category := json_feature->'properties'->>'category';

      json_feature := JSONB_SET(
          jsonb_in          := json_feature
         ,path              := ARRAY['geometry']
         ,replacement       := ST_AsGeoJSON(ST_Transform(sdo_input,4326))::JSONB
         ,create_if_missing := TRUE
      );
      
      IF str_category = 'points'
      THEN
         out_indexed_points := cipsrv_engine.append_feature(
             p_in        := out_indexed_points
            ,p_feature   := json_feature
            ,p_geometry  := NULL
         );
         
      ELSIF str_category = 'lines'
      THEN
         out_indexed_lines := cipsrv_engine.append_feature(
             p_in        := out_indexed_lines
            ,p_feature   := json_feature
            ,p_geometry  := NULL
         );
      
      ELSIF str_category = 'areas'
      THEN
         out_indexed_areas := cipsrv_engine.append_feature(
             p_in        := out_indexed_areas
            ,p_feature   := json_feature
            ,p_geometry  := NULL
         );
   
      END IF;

   END LOOP;  
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.cipsrv_index(
    JSONB
   ,JSONB
   ,JSONB
   ,GEOMETRY
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
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.cipsrv_index(
    JSONB
   ,JSONB
   ,JSONB
   ,GEOMETRY
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
) TO PUBLIC;

