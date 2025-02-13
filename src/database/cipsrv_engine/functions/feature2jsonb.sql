DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.feature2jsonb';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

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
   OR JSONB_TYPEOF(json_properties) = 'null'
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
   
   
   
   IF (p_feature).ring_areacat_threshold_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['ring_areacat_threshold_used']
         ,replacement       := TO_JSONB((p_feature).ring_areacat_threshold_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).ring_areaevt_threshold_used IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['ring_areaevt_threshold_used']
         ,replacement       := TO_JSONB((p_feature).ring_areaevt_threshold_used)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   json_rez := JSONB_BUILD_OBJECT(
       'type'       ,'Feature'
      ,'geometry'   ,ST_AsGeoJSON(ST_Transform((p_feature).geometry,4326))::JSONB
      ,'obj_type'   ,'event_feature_properties'
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

