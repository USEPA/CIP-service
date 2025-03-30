CREATE OR REPLACE FUNCTION cipsrv_tap.area_simple_mr()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   ary_nhdplusid BIGINT[];
   ary_catchmentstatecode VARCHAR[];
   
BEGIN
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Polygon'
            ,'coordinates',JSONB_BUILD_ARRAY(JSONB_BUILD_ARRAY(
                JSONB_BUILD_ARRAY(-96.759682,46.893751)
               ,JSONB_BUILD_ARRAY(-96.767578,46.860426)
               ,JSONB_BUILD_ARRAY(-96.751099,46.832248)
               ,JSONB_BUILD_ARRAY(-96.717453,46.839528)
               ,JSONB_BUILD_ARRAY(-96.70475,46.877795)
               ,JSONB_BUILD_ARRAY(-96.727409,46.896801)
               ,JSONB_BUILD_ARRAY(-96.759682,46.893751)
             ))
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := 'area_simple'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchment          := FALSE
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,9
      ,'test 1 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-96.75968
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,6667855::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MN'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,6667867::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'MN'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[8]
      ,6667869::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[8]
      ,'ND'
      ,'test 1 - check state code 1'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Polygon'
            ,'coordinates',JSONB_BUILD_ARRAY(JSONB_BUILD_ARRAY(
                JSONB_BUILD_ARRAY(-65.700302,18.168035)
               ,JSONB_BUILD_ARRAY(-65.697556,18.229351)
               ,JSONB_BUILD_ARRAY(-65.739441,18.201957)
               ,JSONB_BUILD_ARRAY(-65.727768,18.184997)
               ,JSONB_BUILD_ARRAY(-65.700302,18.168035)
             ))
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := 'area_simple'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchment          := FALSE
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,35
      ,'test 2 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-65.70030
      ,'test 2 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,710005574::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'PR'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[6]
      ,800025807::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[6]
      ,'PR'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[30]
      ,800038707::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[8]
      ,'PR'
      ,'test 2 - check state code 1'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Polygon'
            ,'coordinates',JSONB_BUILD_ARRAY(JSONB_BUILD_ARRAY(
                JSONB_BUILD_ARRAY(-215.209036,13.469597)
               ,JSONB_BUILD_ARRAY(-215.211611,13.46413)
               ,JSONB_BUILD_ARRAY(-215.20762,13.460833)
               ,JSONB_BUILD_ARRAY(-215.204573,13.468637)
               ,JSONB_BUILD_ARRAY(-215.209036,13.469597)
             ))
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := 'area_simple'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchment          := FALSE
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,2
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-215.20904
      ,'test 3 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,810007336::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'GU'
      ,'test 3 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,810007615::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'GU'
      ,'test 3 - check state code 1'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'MultiPolygon'
            ,'coordinates',JSONB_BUILD_ARRAY(
                JSONB_BUILD_ARRAY(JSONB_BUILD_ARRAY(
                   JSONB_BUILD_ARRAY(-87.772865,42.503744)
                  ,JSONB_BUILD_ARRAY(-87.879295,42.501972)
                  ,JSONB_BUILD_ARRAY(-87.921867,42.539169)
                  ,JSONB_BUILD_ARRAY(-87.84462,42.560414)
                  ,JSONB_BUILD_ARRAY(-87.837067,42.533603)
                  ,JSONB_BUILD_ARRAY(-87.798271,42.564713)
                  ,JSONB_BUILD_ARRAY(-87.766342,42.537145)
                  ,JSONB_BUILD_ARRAY(-87.772865,42.503744)
                ))
               ,JSONB_BUILD_ARRAY(JSONB_BUILD_ARRAY(
                   JSONB_BUILD_ARRAY(-87.799988,42.574827)
                  ,JSONB_BUILD_ARRAY(-87.842903,42.586203)
                  ,JSONB_BUILD_ARRAY(-87.815437,42.605411)
                  ,JSONB_BUILD_ARRAY(-87.798615,42.592774)
                  ,JSONB_BUILD_ARRAY(-87.799988,42.574827)
                ))
             )
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := 'area_simple'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchment          := FALSE
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,39
      ,'test 4 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0->0)::NUMERIC,5)
      ,-87.77287
      ,'test 4 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,14783695::BIGINT
      ,'test 4 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'IL'
      ,'test 4 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[39]
      ,708066288::BIGINT
      ,'test 4 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[39]
      ,'WI'
      ,'test 4 - check state code 2'
   );

END;$$;
