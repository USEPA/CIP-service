CREATE OR REPLACE FUNCTION cipsrv_tap.point_simple_mr()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   int_nhdplusid BIGINT;
   str_catchmentstatecode VARCHAR;
   json_input    JSONB;
   
BEGIN

   RETURN NEXT tap.has_extension(
       'postgis'::NAME
      ,'check existence of postgis'::TEXT
   );
   
   RETURN NEXT tap.has_function(
       'cipsrv_engine'::NAME
      ,'cipsrv_version'::NAME
      ,'check existence of cipsrv_version'::TEXT
   );
   
   RETURN NEXT tap.has_function(
       'cipsrv_engine'::NAME
      ,'cipsrv_index'::NAME
      ,'check existence of cipsrv_index'::TEXT
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'Point'
         ,'coordinates',JSONB_BUILD_ARRAY(-76.98669433593751,38.88595542095899)
       )
   );
   
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := json_input
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := ARRAY['DC']
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := 'point_simple'
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := NULL
      ,p_default_areacat_threshold      := NULL
      ,p_default_areaevt_threshold      := NULL
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   RETURN NEXT tap.diag(json_input::TEXT);
   
   -- 1.1
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return code'
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   -- 1.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 1.2 - basic catchment count'
   );
   
   -- 1.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-76.98669
      ,'test 1.3 - check longitude passthrough'
   );
   
   -- 1.4
   RETURN NEXT tap.is(
       int_nhdplusid
      ,22338109::BIGINT
      ,'test 1.4 - check nhdplusid'
   );

   -- 1.5
   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'DC'
      ,'test 1.5 - check state code'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'Point'
         ,'coordinates',JSONB_BUILD_ARRAY(-91.58889770507812,31.05646337884346)
       )
   );
       
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := json_input
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := 'point_simple'
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := NULL
      ,p_default_areacat_threshold      := NULL
      ,p_default_areaevt_threshold      := NULL
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   RETURN NEXT tap.diag(json_input::TEXT);
   
   -- 2.1
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 2.1 - return_code'
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   -- 2.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 2 - basic catchment count'
   );
   
   -- 2.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->1)::NUMERIC,5)
      ,31.05646
      ,'test 2.3 - check latitude passthrough'
   );
   
   -- 2.4
   RETURN NEXT tap.is(
       int_nhdplusid
      ,19406818::BIGINT
      ,'test 2.4 - check nhdplusid'
   );

   -- 2.5
   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'LA'
      ,'test 2.5 - check state code'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'Point'
         ,'coordinates',JSONB_BUILD_ARRAY(-117.05795288085939,42.07580094787546)
       )
   );
       
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := json_input
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := ARRAY['OR']
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := 'point_simple'
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := NULL
      ,p_default_areacat_threshold      := NULL
      ,p_default_areaevt_threshold      := NULL
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   RETURN NEXT tap.diag(json_input::TEXT);
   
   -- 3.1
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1 - return_code'
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   -- 3.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 3.2 - basic catchment count'
   );
   
   -- 3.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-117.05795
      ,'test 3.3 - check longitude passthrough'
   );
   
   -- 3.4
   RETURN NEXT tap.is(
       int_nhdplusid
      ,23344282::BIGINT
      ,'test 3.4 - check nhdplusid'
   );

   -- 3.5
   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'OR'
      ,'test 3.5 - check state code'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'Point'
         ,'coordinates',JSONB_BUILD_ARRAY(-157.80349731445315,21.414719215736195)
       )
   );
       
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := json_input
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := 'point_simple'
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := NULL
      ,p_default_areacat_threshold      := NULL
      ,p_default_areaevt_threshold      := NULL
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   RETURN NEXT tap.diag(json_input::TEXT);
   
   -- 4.1
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 4.1 - return_code'
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   -- 4.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 4.2 - basic catchment count'
   );
   
   -- 4.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-157.80350
      ,'test 4.3 - check longitude passthrough'
   );
   
   -- 4.4
   RETURN NEXT tap.is(
       int_nhdplusid
      ,800016810::BIGINT
      ,'test 4.4 - check nhdplusid'
   );

   -- 4.5
   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'HI'
      ,'test 4.5 - check state code'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'Point'
         ,'coordinates',JSONB_BUILD_ARRAY(-64.95057106018068,18.342631352511194)
       )
   );
       
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := json_input
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := 'point_simple'
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := NULL
      ,p_default_areacat_threshold      := NULL
      ,p_default_areaevt_threshold      := NULL
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   RETURN NEXT tap.diag(json_input::TEXT);
   
   -- 5.1
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code'
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   -- 5.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 5.2 - basic catchment count'
   );
   
   -- 5.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-64.95057
      ,'test 5.3 - check longitude passthrough'
   );
   
   -- 5.4
   RETURN NEXT tap.is(
       int_nhdplusid
      ,800041667::BIGINT
      ,'test 5.4 - check nhdplusid'
   );

   -- 5.5
   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'VI'
      ,'test 5.5 - check state code'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'Point'
         ,'coordinates',JSONB_BUILD_ARRAY(-170.72050094604495,-14.32842599932282)
       )
   );
   
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := json_input
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := 'point_simple'
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := NULL
      ,p_default_areacat_threshold      := NULL
      ,p_default_areaevt_threshold      := NULL
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   RETURN NEXT tap.diag(json_input::TEXT);
   
   -- 6.1
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 6.1 - return_code'
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   -- 6.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 6.2 - basic catchment count'
   );
   
   -- 6.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-170.72050
      ,'test 6.3 - check longitude passthrough'
   );
   
   -- 6.4
   RETURN NEXT tap.is(
       int_nhdplusid
      ,810113492::BIGINT
      ,'test 6.4 - check nhdplusid'
   );

   -- 6.5
   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'AS'
      ,'test 6.5 - check state code'
   );

END;$$;
