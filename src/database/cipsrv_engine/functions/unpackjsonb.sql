DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.unpackjsonb';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.unpackjsonb(
    IN  p_points                         JSONB
   ,IN  p_lines                          JSONB
   ,IN  p_areas                          JSONB
   ,IN  p_geometry                       JSONB
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
   
   ,OUT out_return_code                  INTEGER
   ,OUT out_status_message               VARCHAR
   ,OUT out_features                     cipsrv_engine.cip_feature[]
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
   
   IF p_nhdplus_version IS NULL
   THEN
      RAISE EXCEPTION 'nhdplus version cannot be null';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Branch when geometry is provided
   ----------------------------------------------------------------------------
   IF p_geometry IS NOT NULL
   THEN
      out_features := cipsrv_engine.jsonb2features(
          p_features                      := p_geometry
         ,p_nhdplus_version               := p_nhdplus_version
         ,p_known_region                  := str_known_region
         ,p_int_srid                      := int_srid
         ,p_default_point_indexing_method := p_default_point_indexing_method
         ,p_default_line_indexing_method  := p_default_line_indexing_method
         ,p_default_ring_indexing_method  := p_default_ring_indexing_method
         ,p_default_area_indexing_method  := p_default_area_indexing_method
         ,p_default_line_threshold        := p_default_line_threshold
         ,p_default_areacat_threshold     := p_default_areacat_threshold
         ,p_default_areaevt_threshold     := p_default_areaevt_threshold
      );
   
   ELSE
      ary_points := cipsrv_engine.jsonb2features(
          p_features                      := p_points
         ,p_nhdplus_version               := p_nhdplus_version
         ,p_known_region                  := str_known_region
         ,p_int_srid                      := int_srid
         ,p_default_point_indexing_method := p_default_point_indexing_method
         ,p_default_line_indexing_method  := p_default_line_indexing_method
         ,p_default_ring_indexing_method  := p_default_ring_indexing_method
         ,p_default_area_indexing_method  := p_default_area_indexing_method
         ,p_default_line_threshold        := p_default_line_threshold
         ,p_default_areacat_threshold     := p_default_areacat_threshold
         ,p_default_areaevt_threshold     := p_default_areaevt_threshold
      );
      
      ary_lines := cipsrv_engine.jsonb2features(
          p_features                      := p_lines
         ,p_nhdplus_version               := p_nhdplus_version
         ,p_known_region                  := str_known_region
         ,p_int_srid                      := int_srid
         ,p_default_point_indexing_method := p_default_point_indexing_method
         ,p_default_line_indexing_method  := p_default_line_indexing_method
         ,p_default_ring_indexing_method  := p_default_ring_indexing_method
         ,p_default_area_indexing_method  := p_default_area_indexing_method
         ,p_default_line_threshold        := p_default_line_threshold
         ,p_default_areacat_threshold     := p_default_areacat_threshold
         ,p_default_areaevt_threshold     := p_default_areaevt_threshold
      );
      
      ary_areas := cipsrv_engine.jsonb2features(
          p_features                      := p_areas
         ,p_nhdplus_version               := p_nhdplus_version
         ,p_known_region                  := str_known_region
         ,p_int_srid                      := int_srid
         ,p_default_point_indexing_method := p_default_point_indexing_method
         ,p_default_line_indexing_method  := p_default_line_indexing_method
         ,p_default_ring_indexing_method  := p_default_ring_indexing_method
         ,p_default_area_indexing_method  := p_default_area_indexing_method
         ,p_default_line_threshold        := p_default_line_threshold
         ,p_default_areacat_threshold     := p_default_areacat_threshold
         ,p_default_areaevt_threshold     := p_default_areaevt_threshold
      );
      
      out_features := cipsrv_engine.featurecat(out_features,ary_points);
      out_features := cipsrv_engine.featurecat(out_features,ary_lines);
      out_features := cipsrv_engine.featurecat(out_features,ary_areas);
      
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
            out_features[i].areacat_threshold    := out_features[i].ring_areacat_threshold;
            out_features[i].areaevt_threshold    := out_features[i].ring_areaevt_threshold;
            
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
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   
   ,VARCHAR
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
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

