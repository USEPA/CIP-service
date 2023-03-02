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

