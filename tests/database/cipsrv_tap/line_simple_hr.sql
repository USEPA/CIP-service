CREATE OR REPLACE FUNCTION cipsrv_tap.line_simple_hr()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   ary_nhdplusid BIGINT[];
   ary_catchmentstatecode VARCHAR[];
   json_input    JSONB;
   
BEGIN
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'LineString'
         ,'coordinates',JSONB_BUILD_ARRAY(
             JSONB_BUILD_ARRAY(-108.75761,36.99131)
            ,JSONB_BUILD_ARRAY(-108.753147,36.979518)
            ,JSONB_BUILD_ARRAY(-108.740788,36.98706)
            ,JSONB_BUILD_ARRAY(-108.734779,36.981986)
            ,JSONB_BUILD_ARRAY(-108.723621,36.977598)
            ,JSONB_BUILD_ARRAY(-108.717442,36.982398)
          )
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
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := 'line_simple'
      ,p_default_line_threshold         := 10
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
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 1.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,11
      ,'test 1.2 - basic catchment count ' || rec.out_catchment_count || ' = 11'
   );
   
   -- 1.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-108.75761
      ,'test 1.3 - check longitude passthrough of first ordinate'
   );
   
   -- 1.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,41000600117044::BIGINT
      ,'test 1.4 - check nhdplusid 1'
   );

   -- 1.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'CO'
      ,'test 1.5 - check state code 1'
   );
   
   -- 1.6
   RETURN NEXT tap.is(
       ary_nhdplusid[6]
      ,41000600095221::BIGINT
      ,'test 1.6 - check nhdplusid 1'
   );

   -- 1.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[6]
      ,'NM'
      ,'test 1.7 - check state code 1'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'LineString'
         ,'coordinates',JSONB_BUILD_ARRAY(
             JSONB_BUILD_ARRAY(-83.085651,42.312862)
            ,JSONB_BUILD_ARRAY(-83.096809,42.299024)
            ,JSONB_BUILD_ARRAY(-83.084965,42.289247)
            ,JSONB_BUILD_ARRAY(-83.097839,42.265369)
            ,JSONB_BUILD_ARRAY(-83.117752,42.267274)
            ,JSONB_BUILD_ARRAY(-83.12273,42.277944)
          )
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
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := 'line_simple'
      ,p_default_line_threshold         := 10
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
      ,'test 2.1 - return code'
   );
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 2.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,5
      ,'test 2.2 - basic catchment count ' || rec.out_catchment_count || ' = 5'
   );
   
   -- 2.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-83.08565
      ,'test 2.3 - check longitude passthrough of first ordinate'
   );
   
   -- 2.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,60001500030443::BIGINT
      ,'test 2.4 - check nhdplusid 1'
   );

   -- 2.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MI'
      ,'test 2.5 - check state code 1'
   );
   
   -- 2.6
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,60001500030695::BIGINT
      ,'test 2.6 - check nhdplusid 1'
   );

   -- 2.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'MI'
      ,'test 2.7 - check state code 1'
   );
   
   -- 2.8
   RETURN NEXT tap.is(
       ary_nhdplusid[3]
      ,60001500030697::BIGINT
      ,'test 2.8 - check nhdplusid 1'
   );

   -- 2.9
   RETURN NEXT tap.is(
       ary_catchmentstatecode[3]
      ,'MI'
      ,'test 2.9 - check state code 1'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'LineString'
         ,'coordinates',JSONB_BUILD_ARRAY(
             JSONB_BUILD_ARRAY(-156.177092,20.628768)
            ,JSONB_BUILD_ARRAY(-156.173573,20.628607)
            ,JSONB_BUILD_ARRAY(-156.170998,20.630615)
            ,JSONB_BUILD_ARRAY(-156.168337,20.629009)
          )
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
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := 'line_simple'
      ,p_default_line_threshold         := 10
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
      ,'test 3.1 - return code'
   );
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 3.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 3.2 - basic catchment count ' || rec.out_catchment_count || ' = 1'
   );
   
   -- 3.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-156.17709
      ,'test 3.3 - check longitude passthrough of first ordinate'
   );
   
   -- 3.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,80000200001830::BIGINT
      ,'test 3.4 - check nhdplusid 1'
   );

   -- 3.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'HI'
      ,'test 3.5 - check state code 1'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'LineString'
         ,'coordinates',JSONB_BUILD_ARRAY(
             JSONB_BUILD_ARRAY(-84.02936,33.710454)
            ,JSONB_BUILD_ARRAY(-84.026184,33.710811)
            ,JSONB_BUILD_ARRAY(-84.024982,33.708776)
            ,JSONB_BUILD_ARRAY(-84.025712,33.70642)
            ,JSONB_BUILD_ARRAY(-84.024425,33.704635)
            ,JSONB_BUILD_ARRAY(-84.022579,33.703028)
            ,JSONB_BUILD_ARRAY(-84.019275,33.704742)
            ,JSONB_BUILD_ARRAY(-84.019318,33.708276)
            ,JSONB_BUILD_ARRAY(-84.019532,33.711203)
            ,JSONB_BUILD_ARRAY(-84.024982,33.708776)
          )
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
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := 'line_simple'
      ,p_default_line_threshold         := 10
      ,p_default_ring_indexing_method   := 'area_simple'
      ,p_default_ring_areacat_threshold := 50
      ,p_default_ring_areaevt_threshold := 1
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
      ,'test 4.1 - return code'
   );
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 4.1
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,2
      ,'test 4.1 - basic catchment count ' || rec.out_catchment_count || ' = 2'
   );
   
   -- 4.2
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-84.02936
      ,'test 4.2 - check longitude passthrough of first ordinate'
   );
   
   -- 4.3
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,15001600047400::BIGINT
      ,'test 4.3 - check nhdplusid 1'
   );

   -- 4.4
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'GA'
      ,'test 4.4 - check state code 1'
   );
   
   -- 4.5
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,15001600124060::BIGINT
      ,'test 4.5 - check nhdplusid 2'
   );

   -- 4.6
   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'GA'
      ,'test 4.6 - check state code 2'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'LineString'
         ,'coordinates',JSONB_BUILD_ARRAY(
             JSONB_BUILD_ARRAY(-95.690918,48.821333)
            ,JSONB_BUILD_ARRAY(-95.96283,48.713619)
            ,JSONB_BUILD_ARRAY(-95.722504,48.544796)
            ,JSONB_BUILD_ARRAY(-95.458832,48.649242)
            ,JSONB_BUILD_ARRAY(-95.377808,48.793295)
            ,JSONB_BUILD_ARRAY(-95.690918,48.821333)
          )
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
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := 'line_simple'
      ,p_default_line_threshold         := 10
      ,p_default_ring_indexing_method   := 'area_simple'
      ,p_default_ring_areacat_threshold := 50
      ,p_default_ring_areaevt_threshold := 1
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
      ,'test 5.1 - return code'
   );
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 5.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,930
      ,'test 5.2 - basic catchment count ' || rec.out_catchment_count || ' = 930' 
   );
   
   -- 5.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-95.69092
      ,'test 5.3 - check longitude passthrough of first ordinate'
   );
   
   -- 5.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,65000200000196::BIGINT
      ,'test 5.4 - check nhdplusid 1'
   );

   -- 5.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MN'
      ,'test 5.5 - check state code 1'
   );
   
   -- 5.6
   RETURN NEXT tap.is(
       ary_nhdplusid[236]
      ,65000200034219::BIGINT
      ,'test 5.6 - check nhdplusid 2'
   );

   -- 5.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[236]
      ,'MN'
      ,'test 5.7 - check state code 2'
   );

END;$$;
