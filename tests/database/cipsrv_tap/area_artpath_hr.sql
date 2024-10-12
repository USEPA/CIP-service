CREATE OR REPLACE FUNCTION cipsrv_tap.area_artpath_hr()
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
                JSONB_BUILD_ARRAY(-119.162178,34.267999)
               ,JSONB_BUILD_ARRAY(-119.134712,34.288425)
               ,JSONB_BUILD_ARRAY(-119.116516,34.300905)
               ,JSONB_BUILD_ARRAY(-119.094543,34.310547)
               ,JSONB_BUILD_ARRAY(-119.122353,34.290977)
               ,JSONB_BUILD_ARRAY(-119.162178,34.260054)
               ,JSONB_BUILD_ARRAY(-119.184837,34.251257)
               ,JSONB_BUILD_ARRAY(-119.162178,34.267999)
             ))
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := 'area_artpath'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
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
      ,21
      ,'test 1 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-119.16218
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,50000100001205::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'CA'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,50000100001215::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'CA'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[20]
      ,50000100198451::BIGINT
      ,'test 1 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[20]
      ,'CA'
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
             'type'       ,'Polygon'
            ,'coordinates',JSONB_BUILD_ARRAY(JSONB_BUILD_ARRAY(
                JSONB_BUILD_ARRAY(-158.08176,21.615462)
               ,JSONB_BUILD_ARRAY(-158.100128,21.59631)
               ,JSONB_BUILD_ARRAY(-158.060646,21.590085)
               ,JSONB_BUILD_ARRAY(-158.031292,21.603014)
               ,JSONB_BUILD_ARRAY(-158.08176,21.615462)
             ))
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := 'area_artpath'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
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
      ,18
      ,'test 2 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-158.08176
      ,'test 2 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,80000600000136::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'HI'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[10]
      ,80000600001442::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[10]
      ,'HI'
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
                JSONB_BUILD_ARRAY(-99.707794,35.315125)
               ,JSONB_BUILD_ARRAY(-99.73938,35.245619)
               ,JSONB_BUILD_ARRAY(-99.615784,35.224307)
               ,JSONB_BUILD_ARRAY(-99.541626,35.291589)
               ,JSONB_BUILD_ARRAY(-99.707794,35.315125)
             ))
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
      ,p_default_point_indexing_method  := NULL
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := 'area_artpath'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
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
      ,64
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-99.70779
      ,'test 3 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,21000800000006::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'OK'
      ,'test 3 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[15]
      ,21000800021815::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[15]
      ,'OK'
      ,'test 3 - check state code 1'
   );

END;$$;
