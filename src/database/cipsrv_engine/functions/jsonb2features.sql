DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.jsonb2features';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.jsonb2features(
    IN  p_features                       JSONB
   ,IN  p_nhdplus_version                VARCHAR DEFAULT NULL
   ,IN  p_known_region                   VARCHAR DEFAULT NULL
   ,IN  p_int_srid                       INTEGER DEFAULT NULL
   
   ,IN  p_default_point_indexing_method  VARCHAR DEFAULT NULL
   
   ,IN  p_default_line_indexing_method   VARCHAR DEFAULT NULL
   ,IN  p_default_line_threshold         NUMERIC DEFAULT NULL
   
   ,IN  p_default_ring_indexing_method   VARCHAR DEFAULT NULL
   ,IN  p_default_ring_areacat_threshold NUMERIC DEFAULT NULL
   ,IN  p_default_ring_areaevt_threshold NUMERIC DEFAULT NULL
   
   ,IN  p_default_area_indexing_method   VARCHAR DEFAULT NULL
   ,IN  p_default_areacat_threshold      NUMERIC DEFAULT NULL
   ,IN  p_default_areaevt_threshold      NUMERIC DEFAULT NULL

   ,OUT out_cip_features                 cipsrv_engine.cip_feature[]
   ,OUT out_known_region                 VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                                RECORD;
   obj_rez                            cipsrv_engine.cip_feature[];
   ary_rez                            cipsrv_engine.cip_feature[];
   str_nhdplus_version                VARCHAR;
   int_srid                           INTEGER;
   
   str_default_point_indexing_method  VARCHAR;
   
   str_default_line_indexing_method   VARCHAR;
   num_default_line_threshold         NUMERIC;
   
   str_default_ring_indexing_method   VARCHAR;
   num_default_ring_areacat_threshold NUMERIC;
   num_default_ring_areaevt_threshold NUMERIC;
   
   str_default_area_indexing_method   VARCHAR;
   num_default_areacat_threshold      NUMERIC;
   num_default_areaevt_threshold      NUMERIC;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_features IS NULL
   OR ( JSONB_TYPEOF(p_features) = 'array'
   AND JSONB_ARRAY_LENGTH(p_features) = 0 )
   THEN
      out_cip_features := ary_rez;
      RETURN;
      
   END IF;
   
   str_nhdplus_version                := p_nhdplus_version;
   out_known_region                   := p_known_region;
   int_srid                           := p_int_srid;
   
   str_default_point_indexing_method  := p_default_point_indexing_method;
   
   str_default_line_indexing_method   := p_default_line_indexing_method;
   num_default_line_threshold         := p_default_line_threshold;
   
   str_default_ring_indexing_method   := p_default_ring_indexing_method;
   num_default_ring_areacat_threshold := p_default_ring_areacat_threshold;
   num_default_ring_areaevt_threshold := p_default_ring_areaevt_threshold;

   str_default_area_indexing_method   := p_default_area_indexing_method;
   num_default_areacat_threshold      := p_default_areacat_threshold;
   num_default_areaevt_threshold      := p_default_areaevt_threshold;

   ----------------------------------------------------------------------------
   -- Build the features
   ----------------------------------------------------------------------------
   IF JSONB_TYPEOF(p_features) = 'object'
   AND p_features->>'type' IN ('Point','LineString','Polygon','MultiPoint','MultiLineString','MultiPolygon','GeometryCollection')
   THEN
      rec := cipsrv_engine.jsonb2feature(
          p_feature               := JSONB_BUILD_OBJECT(
             'type',     'Feature'
            ,'geometry', p_features
          )
         ,p_nhdplus_version        := str_nhdplus_version
         ,p_known_region           := out_known_region
         ,p_int_srid               := int_srid
         
         ,p_point_indexing_method  := str_default_point_indexing_method
         
         ,p_line_indexing_method   := str_default_line_indexing_method
         ,p_line_threshold         := num_default_line_threshold
         
         ,p_ring_indexing_method   := str_default_ring_indexing_method
         ,p_ring_areacat_threshold := num_default_ring_areacat_threshold
         ,p_ring_areaevt_threshold := num_default_ring_areaevt_threshold
         
         ,p_area_indexing_method   := str_default_area_indexing_method
         ,p_areacat_threshold      := num_default_areacat_threshold
         ,p_areaevt_threshold      := num_default_areaevt_threshold
      );
      obj_rez          := rec.out_cip_features;
      out_known_region := rec.out_known_region;
      
      ary_rez := cipsrv_engine.featurecat(ary_rez,obj_rez);
   
   ELSIF JSONB_TYPEOF(p_features) = 'object'
   AND p_features->>'type' = 'Feature'
   THEN
      rec := cipsrv_engine.jsonb2feature(
          p_feature                := p_features
         ,p_nhdplus_version        := str_nhdplus_version
         ,p_known_region           := out_known_region
         ,p_int_srid               := int_srid
         
         ,p_point_indexing_method  := str_default_point_indexing_method
         
         ,p_line_indexing_method   := str_default_line_indexing_method
         ,p_line_threshold         := num_default_line_threshold
         
         ,p_ring_indexing_method   := str_default_ring_indexing_method
         ,p_ring_areacat_threshold := num_default_ring_areacat_threshold
         ,p_ring_areaevt_threshold := num_default_ring_areaevt_threshold
         
         ,p_area_indexing_method   := str_default_area_indexing_method
         ,p_areacat_threshold      := num_default_areacat_threshold
         ,p_areaevt_threshold      := num_default_areaevt_threshold
          
      );
      obj_rez          := rec.out_cip_features;
      out_known_region := rec.out_known_region;
      
      ary_rez := cipsrv_engine.featurecat(ary_rez,obj_rez);
   
   ELSIF JSONB_TYPEOF(p_features) = 'object'
   AND p_features->>'type' = 'FeatureCollection'
   THEN
      FOR i IN 1 .. JSONB_ARRAY_LENGTH(p_features->'features')
      LOOP
         rec := cipsrv_engine.jsonb2feature(
             p_feature                := p_features->'features'->i-1
            ,p_nhdplus_version        := str_nhdplus_version
            ,p_known_region           := out_known_region
            ,p_int_srid               := int_srid
            
            ,p_point_indexing_method  := str_default_point_indexing_method
            
            ,p_line_indexing_method   := str_default_line_indexing_method
            ,p_line_threshold         := num_default_line_threshold
            
            ,p_ring_indexing_method   := str_default_ring_indexing_method
            ,p_ring_areacat_threshold := num_default_ring_areacat_threshold
            ,p_ring_areaevt_threshold := num_default_ring_areaevt_threshold
            
            ,p_area_indexing_method   := str_default_area_indexing_method
            ,p_areacat_threshold      := num_default_areacat_threshold
            ,p_areaevt_threshold      := num_default_areaevt_threshold
            
         );
         obj_rez          := rec.out_cip_features;
         out_known_region := rec.out_known_region;
      
         ary_rez := cipsrv_engine.featurecat(ary_rez,obj_rez);
   
      END LOOP;
      
   ELSE
      RAISE EXCEPTION 'input jsonb is not geojson %', p_features;
   
   END IF;
   
   out_cip_features := ary_rez;
   RETURN;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.jsonb2features';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;

