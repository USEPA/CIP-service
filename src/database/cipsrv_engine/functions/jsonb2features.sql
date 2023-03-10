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
   AND p_features->>'type' IN ('Point','LineString','Polygon','MultiPoint','MultiLineString','MultiPolygon','GeometryCollection')
   THEN
      obj_rez := cipsrv_engine.jsonb2feature(
          p_feature               := JSONB_BUILD_OBJECT(
             'type',     'Feature'
            ,'geometry', p_features
          )
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
      
      ary_rez := cipsrv_engine.featurecat(ary_rez,obj_rez);
   
   ELSIF JSONB_TYPEOF(p_features) = 'object'
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
      
      ary_rez := cipsrv_engine.featurecat(ary_rez,obj_rez);
   
   ELSIF JSONB_TYPEOF(p_features) = 'object'
   AND p_features->>'type' = 'FeatureCollection'
   THEN
      FOR i IN 1 .. JSONB_ARRAY_LENGTH(p_features->'features')
      LOOP
         obj_rez := cipsrv_engine.jsonb2feature(
             p_feature               := p_features->'features'->i-1
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
      
         ary_rez := cipsrv_engine.featurecat(ary_rez,obj_rez);
   
      END LOOP;
      
   ELSE
      RAISE EXCEPTION 'input jsonb is not geojson %', p_features;
   
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

