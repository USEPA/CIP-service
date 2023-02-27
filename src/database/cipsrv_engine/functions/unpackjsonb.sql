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
      
      -- Add ring handling here
      
      out_features := array_cat(out_features,ary_points);
      out_features := array_cat(out_features,ary_lines);
      out_features := array_cat(out_features,ary_areas);
   
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

