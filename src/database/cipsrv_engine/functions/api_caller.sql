SELECT * FROM cipsrv_engine.cipsrv_index(
    p_points                         := NULL
   ,p_lines                          := (SELECT ST_AsGeoJSON(shape)::JSONB FROM cipsrv_upload.nh_11113300_2024_ln_lines a WHERE a.source_featureid = 'NHRIV700060202-23')
   ,p_areas                          := NULL
   ,p_geometry                       := NULL
   ,p_geometry_clip                  := NULL
   ,p_geometry_clip_stage            := NULL
   ,p_catchment_filter               := ARRAY['NH','NOTRIBAL']::VARCHAR[]
   ,p_nhdplus_version                := 'HR'
   ,p_wbd_version                    := NULL
   
   ,p_default_point_indexing_method  := 'point_simple'
   
   ,p_default_line_indexing_method   := 'line_simple'
   ,p_default_line_threshold         := 10.0
   
   ,p_default_ring_indexing_method   := 'area_simple'
   ,p_default_ring_areacat_threshold := 50.0
   ,p_default_ring_areaevt_threshold := 1.0
   
   ,p_default_area_indexing_method   := 'area_simple'
   ,p_default_areacat_threshold      := 50.0
   ,p_default_areaevt_threshold      := 1.0
   
   ,p_known_region                   := '5070'
   ,p_return_indexed_features        := FALSE
   ,p_return_indexed_collection      := FALSE
   ,p_return_catchment_geometry      := FALSE
   ,p_return_indexing_summary        := TRUE
);

