CREATE OR REPLACE FUNCTION cipsrv_tap.line_levelpath_mr()
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
             'type'       ,'LineString'
            ,'coordinates',JSONB_BUILD_ARRAY(
                JSONB_BUILD_ARRAY(-91.106186,42.886782)
               ,JSONB_BUILD_ARRAY(-91.099319,42.876467)
               ,JSONB_BUILD_ARRAY(-91.099319,42.86137)
               ,JSONB_BUILD_ARRAY(-91.093483,42.847779)
               ,JSONB_BUILD_ARRAY(-91.08181,42.836954)
             )
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := 'line_levelpath'
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
      ,12
      ,'test 1 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-91.10619
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,13326724::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'IA'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[12]
      ,13327010::BIGINT
      ,'test 1 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[12]
      ,'WI'
      ,'test 1 - check state code 2'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'LineString'
            ,'coordinates',JSONB_BUILD_ARRAY(
                JSONB_BUILD_ARRAY(-82.592983,34.010693)
               ,JSONB_BUILD_ARRAY(-82.58955,34.001443)
               ,JSONB_BUILD_ARRAY(-82.584229,33.995466)
               ,JSONB_BUILD_ARRAY(-82.573071,34.000304)
               ,JSONB_BUILD_ARRAY(-82.575817,33.991907)
             )
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := 'line_levelpath'
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
      ,4
      ,'test 2 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-82.59298
      ,'test 2 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,11737871::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'GA'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[4]
      ,11738899::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[4]
      ,'SC'
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
             'type'       ,'LineString'
            ,'coordinates',JSONB_BUILD_ARRAY(
                JSONB_BUILD_ARRAY(-91.869049,44.710634)
               ,JSONB_BUILD_ARRAY(-91.864929,44.712952)
               ,JSONB_BUILD_ARRAY(-91.855831,44.709048)
               ,JSONB_BUILD_ARRAY(-91.846561,44.711366)
               ,JSONB_BUILD_ARRAY(-91.841583,44.718197)
             )
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := 'line_levelpath'
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
      ,3
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-91.86905
      ,'test 3 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,13107309::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'WI'
      ,'test 3 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[3]
      ,13107317::BIGINT
      ,'test 3 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[3]
      ,'WI'
      ,'test 3 - check state code 2'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'LineString'
            ,'coordinates',JSONB_BUILD_ARRAY(
                JSONB_BUILD_ARRAY(-65.728369,18.185486)
               ,JSONB_BUILD_ARRAY(-65.731716,18.187362)
               ,JSONB_BUILD_ARRAY(-65.733175,18.183529)
               ,JSONB_BUILD_ARRAY(-65.737982,18.18728)
               ,JSONB_BUILD_ARRAY(-65.737896,18.192988)
               ,JSONB_BUILD_ARRAY(-65.743904,18.196494)
             )
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := 'line_levelpath'
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
      ,5
      ,'test 4 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-65.72837
      ,'test 4 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,800030108::BIGINT
      ,'test 4 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'PR'
      ,'test 4 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[5]
      ,800036551::BIGINT
      ,'test 4 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[5]
      ,'PR'
      ,'test 4 - check state code 2'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'MultiLineString'
            ,'coordinates',JSONB_BUILD_ARRAY(
                JSONB_BUILD_ARRAY(
                   JSONB_BUILD_ARRAY(-116.006012,48.085419)
                  ,JSONB_BUILD_ARRAY(-115.979233,48.085419)
                  ,JSONB_BUILD_ARRAY(-115.97683,48.072115)
                  ,JSONB_BUILD_ARRAY(-115.961723,48.066609)
                )
               ,JSONB_BUILD_ARRAY(
                   JSONB_BUILD_ARRAY(-115.84877,48.092299)
                  ,JSONB_BUILD_ARRAY(-115.922928,48.044579)
                  ,JSONB_BUILD_ARRAY(-115.86731,47.979811)
                  ,JSONB_BUILD_ARRAY(-115.701141,48.014731)
                  ,JSONB_BUILD_ARRAY(-115.84877,48.092299)
                )
             )
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := 'line_levelpath'
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
      ,49
      ,'test 5 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-115.84877
      ,'test 5 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,22975990::BIGINT
      ,'test 5 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MT'
      ,'test 5 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[46]
      ,25073170::BIGINT
      ,'test 5 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[46]
      ,'MT'
      ,'test 5 - check state code 2'
   );

END;$$;
