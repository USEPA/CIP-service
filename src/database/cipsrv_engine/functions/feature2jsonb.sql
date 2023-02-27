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
   
   IF (p_feature).globalid IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['globalid']
         ,replacement       := TO_JSONB((p_feature).globalid)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).gtype IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['gtype']
         ,replacement       := TO_JSONB((p_feature).gtype)
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
   
   IF (p_feature).isring IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['isring']
         ,replacement       := TO_JSONB((p_feature).isring)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).nhdplus_version IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['nhdplus_version']
         ,replacement       := TO_JSONB((p_feature).nhdplus_version)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).known_region IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['known_region']
         ,replacement       := TO_JSONB((p_feature).known_region)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).int_srid IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['int_srid']
         ,replacement       := TO_JSONB((p_feature).int_srid)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).point_indexing_method IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['point_indexing_method']
         ,replacement       := TO_JSONB((p_feature).point_indexing_method)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).line_indexing_method IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['line_indexing_method']
         ,replacement       := TO_JSONB((p_feature).line_indexing_method)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).ring_indexing_method IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['ring_indexing_method']
         ,replacement       := TO_JSONB((p_feature).ring_indexing_method)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).area_indexing_method IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['area_indexing_method']
         ,replacement       := TO_JSONB((p_feature).area_indexing_method)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).line_threshold IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['line_threshold']
         ,replacement       := TO_JSONB((p_feature).line_threshold)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).areacat_threshold IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['areacat_threshold']
         ,replacement       := TO_JSONB((p_feature).areacat_threshold)
         ,create_if_missing := TRUE
      );
      
   END IF;
   
   IF (p_feature).areaevt_threshold IS NOT NULL
   THEN
      json_properties := JSONB_SET(
          jsonb_in          := json_properties
         ,path              := ARRAY['areaevt_threshold']
         ,replacement       := TO_JSONB((p_feature).areaevt_threshold)
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

