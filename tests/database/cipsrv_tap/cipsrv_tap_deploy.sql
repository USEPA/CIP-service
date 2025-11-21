--******************************--
----- area_artpath_hr.sql 

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
      ,p_wbd_version                    := 'NP21'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,p_wbd_version                    := 'NP21'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,p_wbd_version                    := 'NP21'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
--******************************--
----- area_artpath_mr.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.area_artpath_mr()
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
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   -- 1
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
   
   -- 2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,26
      ,'test 1 - basic catchment count'
   );
   
   -- 3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-119.16218
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   -- 4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,17573033::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   -- 5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'CA'
      ,'test 1 - check state code 1'
   );
   
   -- 6
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,17573055::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   -- 7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'CA'
      ,'test 1 - check state code 1'
   );
   
   -- 8
   RETURN NEXT tap.is(
       ary_nhdplusid[20]
      ,17573739::BIGINT
      ,'test 1 - check nhdplusid 2'
   );

   -- 9
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
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,23
      ,'test 2 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-158.08176
      ,'test 2 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,800000849::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'HI'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[20]
      ,800016537::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[20]
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
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,32
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-99.70779
      ,'test 3 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,13755389::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'OK'
      ,'test 3 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[15]
      ,13755457::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[15]
      ,'OK'
      ,'test 3 - check state code 1'
   );

END;$$;
--******************************--
----- area_centroid_hr.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.area_centroid_hr()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   ary_nhdplusid BIGINT[];
   ary_catchmentstatecode VARCHAR[];
   json_input    JSONB;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- test 1
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'Polygon'
         ,'coordinates',JSONB_BUILD_ARRAY(JSONB_BUILD_ARRAY(
             JSONB_BUILD_ARRAY(-89.213104,37.050245)
            ,JSONB_BUILD_ARRAY(-89.202118,37.01626)
            ,JSONB_BUILD_ARRAY(-89.175682,36.96937)
            ,JSONB_BUILD_ARRAY(-89.124184,36.981164)
            ,JSONB_BUILD_ARRAY(-89.160576,37.034625)
            ,JSONB_BUILD_ARRAY(-89.194565,37.052985)
            ,JSONB_BUILD_ARRAY(-89.213104,37.050245)
          ))
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
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := 'area_centroid'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
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
      ,32
      ,'test 1.2 - basic catchment count: ' || rec.out_catchment_count || ' = 32 '
   );
   
   -- 1.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-89.21310
      ,'test 1.3 - check longitude passthrough of first ordinate'
   );
   
   -- 1.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,22000100008208::BIGINT
      ,'test 1.4 - check nhdplusid 1'
   );

   -- 1.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'IL'
      ,'test 1.5 - check state code ' || ary_catchmentstatecode[1]
   );
   
   -- 1.6
   --raise warning '%',ary_nhdplusid[23];
   RETURN NEXT tap.is(
       ary_nhdplusid[23]
      ,24000100421021::BIGINT
      ,'test 1.6 - check nhdplusid 2 ' || ary_nhdplusid[23]::VARCHAR
   );

   -- 1.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[23]
      ,'KY'
      ,'test 1.7 - check state code 2 ' || ary_catchmentstatecode[23]
   );
   
   -- 1.8
   RETURN NEXT tap.is(
       ary_nhdplusid[31]
      ,22000100008400::BIGINT
      ,'test 1.8 - check nhdplusid 3 ' ||ary_nhdplusid[31]::VARCHAR
   );

   -- 1.9
   RETURN NEXT tap.is(
       ary_catchmentstatecode[31]
      ,'MO'
      ,'test 1.9 - check state code 3 ' || ary_catchmentstatecode[31]
   );
   
   ----------------------------------------------------------------------------
   -- 2
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'Polygon'
         ,'coordinates',JSONB_BUILD_ARRAY(JSONB_BUILD_ARRAY(
             JSONB_BUILD_ARRAY(-67.17144,18.4047)
            ,JSONB_BUILD_ARRAY(-67.175903,18.393624)
            ,JSONB_BUILD_ARRAY(-67.156677,18.384176)
            ,JSONB_BUILD_ARRAY(-67.145004,18.398185)
            ,JSONB_BUILD_ARRAY(-67.156334,18.408609)
            ,JSONB_BUILD_ARRAY(-67.17144,18.4047)
          ))
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
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := 'area_centroid'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
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
      ,15
      ,'test 2.2 - basic catchment count'
   );
   
   -- 2.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-67.17144
      ,'test 2.3 - check longitude passthrough of first ordinate'
   );
   
   -- 2.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,85000100000447::BIGINT
      ,'test 2.4 - check nhdplusid 1'
   );

   -- 2.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'PR'
      ,'test 2.5 - check state code 1'
   );
   
   -- 2.6
   RETURN NEXT tap.is(
       ary_nhdplusid[12]
      ,85000100013695::BIGINT
      ,'test 2.6 - check nhdplusid 1'
   );

   -- 2.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[12]
      ,'PR'
      ,'test 2.7 - check state code 1'
   );
   
   ----------------------------------------------------------------------------
   -- 3
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
       'type'    ,'Feature'
      ,'geometry',JSONB_BUILD_OBJECT(
          'type'       ,'Polygon'
         ,'coordinates',JSONB_BUILD_ARRAY(JSONB_BUILD_ARRAY(
             JSONB_BUILD_ARRAY(-109.037762,37.011874)
            ,JSONB_BUILD_ARRAY(-109.053555,36.995698)
            ,JSONB_BUILD_ARRAY(-109.049606,36.99227)
            ,JSONB_BUILD_ARRAY(-109.03965,36.991036)
            ,JSONB_BUILD_ARRAY(-109.03038,37.002827)
            ,JSONB_BUILD_ARRAY(-109.037762,37.011874)
          ))
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
      ,p_default_line_indexing_method   := NULL
      ,p_default_line_threshold         := NULL
      ,p_default_ring_indexing_method   := NULL
      ,p_default_ring_areacat_threshold := NULL
      ,p_default_ring_areaevt_threshold := NULL
      ,p_default_area_indexing_method   := 'area_centroid'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
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
   
   -- 3.3
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,29
      ,'test 3.2 - basic catchment count'
   );
   
   -- 3.4
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-109.03776
      ,'test 3.4 - check longitude passthrough of first ordinate'
   );
   
   -- 3.5
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,41000600049938::BIGINT
      ,'test 3.5 - check nhdplusid 1'
   );

   -- 3.6
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'AZ'
      ,'test 3.6 - check state code 1'
   );
   
   -- 3.7
   RETURN NEXT tap.is(
       ary_nhdplusid[5]
      ,41000600005775::BIGINT
      ,'test 3.7 - check nhdplusid 1'
   );

   -- 3.8
   RETURN NEXT tap.is(
       ary_catchmentstatecode[5]
      ,'CO'
      ,'test 3.8 - check state code 1'
   );

END;$$;
--******************************--
----- area_centroid_mr.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.area_centroid_mr()
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
                JSONB_BUILD_ARRAY(-89.213104,37.050245)
               ,JSONB_BUILD_ARRAY(-89.202118,37.01626)
               ,JSONB_BUILD_ARRAY(-89.175682,36.96937)
               ,JSONB_BUILD_ARRAY(-89.124184,36.981164)
               ,JSONB_BUILD_ARRAY(-89.160576,37.034625)
               ,JSONB_BUILD_ARRAY(-89.194565,37.052985)
               ,JSONB_BUILD_ARRAY(-89.213104,37.050245)
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
      ,p_default_area_indexing_method   := 'area_centroid'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,-89.21310
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,1841285::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'IL'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[11]
      ,1841291::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[11]
      ,'KY'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[20]
      ,1844789::BIGINT
      ,'test 1 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[20]
      ,'MO'
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
                JSONB_BUILD_ARRAY(-67.17144,18.4047)
               ,JSONB_BUILD_ARRAY(-67.175903,18.393624)
               ,JSONB_BUILD_ARRAY(-67.156677,18.384176)
               ,JSONB_BUILD_ARRAY(-67.145004,18.398185)
               ,JSONB_BUILD_ARRAY(-67.156334,18.408609)
               ,JSONB_BUILD_ARRAY(-67.17144,18.4047)
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
      ,p_default_area_indexing_method   := 'area_centroid'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,15
      ,'test 2 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-67.17144
      ,'test 2 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,800023799::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'PR'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[12]
      ,800038877::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[12]
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
                JSONB_BUILD_ARRAY(-109.037762,37.011874)
               ,JSONB_BUILD_ARRAY(-109.053555,36.995698)
               ,JSONB_BUILD_ARRAY(-109.049606,36.99227)
               ,JSONB_BUILD_ARRAY(-109.03965,36.991036)
               ,JSONB_BUILD_ARRAY(-109.03038,37.002827)
               ,JSONB_BUILD_ARRAY(-109.037762,37.011874)
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
      ,p_default_area_indexing_method   := 'area_centroid'
      ,p_default_areacat_threshold      := 50
      ,p_default_areaevt_threshold      := 1
      ,p_known_region                   := NULL
      ,p_return_indexed_features        := TRUE
      ,p_return_indexed_collection      := FALSE
      ,p_return_catchment_geometry      := FALSE
      ,p_return_indexing_summary        := FALSE
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-109.03776
      ,'test 3 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,1400134::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'AZ'
      ,'test 3 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[5]
      ,1400134::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[5]
      ,'UT'
      ,'test 3 - check state code 1'
   );

END;$$;
--******************************--
----- area_simple_hr.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.area_simple_hr()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   ary_nhdplusid BIGINT[];
   ary_catchmentstatecode VARCHAR[];
   num_dummy     NUMERIC;
   
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
      ,p_nhdplus_version                := 'HR'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,15
      ,'test 1 - basic catchment count'
   );

   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-96.75968
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,65000200000068::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MN'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,65000200011974::BIGINT
      ,'test 1 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'MN'
      ,'test 1 - check state code 2'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[14]
      ,65000200047343::BIGINT
      ,'test 1 - check nhdplusid 3'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[14]
      ,'ND'
      ,'test 1 - check state code 3'
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
      ,p_nhdplus_version                := 'HR'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,38
      ,'test 2 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-65.70030
      ,'test 2 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,85000100000234::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'PR'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[6]
      ,85000100004575::BIGINT
      ,'test 2 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[6]
      ,'PR'
      ,'test 2 - check state code 2'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[30]
      ,95380645218592::BIGINT
      ,'test 2 - check nhdplusid 3'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[8]
      ,'PR'
      ,'test 2 - check state code 3'
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
      ,p_nhdplus_version                := 'HR'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,90000100000135::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'GU'
      ,'test 3 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,90000100000362::BIGINT
      ,'test 3 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'GU'
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
      ,p_nhdplus_version                := 'HR'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,82
      ,'test 4 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0->0)::NUMERIC,5)
      ,-87.77287
      ,'test 4 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,22000800001168::BIGINT
      ,'test 4 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'WI'
      ,'test 4 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[39]
      ,60002800006751::BIGINT
      ,'test 4 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[39]
      ,'WI'
      ,'test 4 - check state code 2'
   );

END;$$;
--******************************--
----- area_simple_mr.sql 

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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
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
--******************************--
----- line_levelpath_hr.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.line_levelpath_hr()
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
             JSONB_BUILD_ARRAY(-91.106186,42.886782)
            ,JSONB_BUILD_ARRAY(-91.099319,42.876467)
            ,JSONB_BUILD_ARRAY(-91.099319,42.86137)
            ,JSONB_BUILD_ARRAY(-91.093483,42.847779)
            ,JSONB_BUILD_ARRAY(-91.08181,42.836954)
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   RETURN NEXT tap.diag(json_input::TEXT);
   
   -- 1.1
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
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
      ,10
      ,'test 1.2 - basic catchment count'
   );
   
   -- 1.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-91.10619
      ,'test 1.3 - check longitude passthrough of first ordinate'
   );
   
   -- 1.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,22000400002754::BIGINT
      ,'test 1.4 - check nhdplusid 1'
   );

   -- 1.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'IA'
      ,'test 1.5 - check state code 1'
   );
   
   -- 1.6
   RETURN NEXT tap.is(
       ary_nhdplusid[10]
      ,22000400022592::BIGINT
      ,'test 1.6 - check nhdplusid 2'
   );

   -- 1.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[10]
      ,'WI'
      ,'test 1.7 - check state code 2'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
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
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 2.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,4
      ,'test 2.2 - basic catchment count'
   );
   
   -- 2.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-82.59298
      ,'test 2.3 - check longitude passthrough of first ordinate'
   );
   
   -- 2.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,15000600198758::BIGINT
      ,'test 2.4 - check nhdplusid 1'
   );

   -- 2.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'GA'
      ,'test 2.5 - check state code 1'
   );
   
   -- 2.6
   RETURN NEXT tap.is(
       ary_nhdplusid[4]
      ,15000600257623::BIGINT
      ,'test 2.6 - check nhdplusid 1'
   );

   -- 2.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[4]
      ,'SC'
      ,'test 2.7 - check state code 1'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
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
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 3.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,4
      ,'test 3.2 - basic catchment count'
   );
   
   -- 3.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-91.86905
      ,'test 3.3 - check longitude passthrough of first ordinate'
   );
   
   -- 3.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,22001300007596::BIGINT
      ,'test 3.4 - check nhdplusid 1'
   );

   -- 3.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'WI'
      ,'test 3.5 - check state code 1'
   );
   
   -- 3.6
   RETURN NEXT tap.is(
       ary_nhdplusid[3]
      ,22001300011102::BIGINT
      ,'test 3.6 - check nhdplusid 2'
   );

   -- 3.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[3]
      ,'WI'
      ,'test 3.7 - check state code 2'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
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
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 4.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,4
      ,'test 4.2 - basic catchment count'
   );
   
   -- 4.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-65.72837
      ,'test 4.3 - check longitude passthrough of first ordinate'
   );
   
   -- 4.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,85000100010380::BIGINT
      ,'test 4.4 - check nhdplusid 1'
   );

   -- 4.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'PR'
      ,'test 4.5 - check state code 1'
   );
   
   -- 4.6
   RETURN NEXT tap.is(
       ary_nhdplusid[4]
      ,85000100016890::BIGINT
      ,'test 4.6 - check nhdplusid 2'
   );

   -- 4.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[4]
      ,'PR'
      ,'test 4.7 - check state code 2'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   RETURN NEXT tap.diag(json_input::TEXT);
   
   -- 5.1
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code '
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
      ,129
      ,'test 5.2 - basic catchment count'
   );
   
   -- 5.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-115.84877
      ,'test 5.3 - check longitude passthrough of first ordinate'
   );
   
   -- 5.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,55001100000045::BIGINT
      ,'test 5.4 - check nhdplusid 1'
   );

   -- 5.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MT'
      ,'test 5.5 - check state code 1'
   );
   
   -- 5.6
   RETURN NEXT tap.is(
       ary_nhdplusid[46]
      ,55001100106215::BIGINT
      ,'test 5.6 - check nhdplusid 2'
   );

   -- 5.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[46]
      ,'MT'
      ,'test 5.7 - check state code 2'
   );

END;$$;
--******************************--
----- line_levelpath_mr.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.line_levelpath_mr()
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
             JSONB_BUILD_ARRAY(-91.106186,42.886782)
            ,JSONB_BUILD_ARRAY(-91.099319,42.876467)
            ,JSONB_BUILD_ARRAY(-91.099319,42.86137)
            ,JSONB_BUILD_ARRAY(-91.093483,42.847779)
            ,JSONB_BUILD_ARRAY(-91.08181,42.836954)
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
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := 'NP21'
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   RETURN NEXT tap.diag(json_input::TEXT);
   
   -- 1.1
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
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
      ,13
      ,'test 1.2 - basic catchment count'
   );
   
   -- 1.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-91.10619
      ,'test 1.3 - check longitude passthrough of first ordinate'
   );
   
   -- 1.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,13326724::BIGINT
      ,'test 1.4 - check nhdplusid 1'
   );

   -- 1.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'IA'
      ,'test 1.5 - check state code 1'
   );
   
   -- 1.6
   RETURN NEXT tap.is(
       ary_nhdplusid[12]
      ,13327004::BIGINT
      ,'test 1.6 - check nhdplusid 2'
   );

   -- 1.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[12]
      ,'WI'
      ,'test 1.7 - check state code 2'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
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
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 2.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,4
      ,'test 2.2 - basic catchment count'
   );
   
   -- 2.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-82.59298
      ,'test 2.3 - check longitude passthrough of first ordinate'
   );
   
   -- 2.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,11737871::BIGINT
      ,'test 2.4 - check nhdplusid 1'
   );

   -- 2.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'GA'
      ,'test 2.5 - check state code 1'
   );
   
   -- 2.6
   RETURN NEXT tap.is(
       ary_nhdplusid[4]
      ,11738899::BIGINT
      ,'test 2.6 - check nhdplusid 1'
   );

   -- 2.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[4]
      ,'SC'
      ,'test 2.7 - check state code 1'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
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
      ,p_return_full_catchments         := FALSE
      ,p_limit_to_us_catchments         := TRUE
   );
   
   RETURN NEXT tap.diag(json_input::TEXT);
   
   -- 3.1
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1- return_code'
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
      ,3
      ,'test 3 - basic catchment count'
   );
   
   -- 3.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-91.86905
      ,'test 3 - check longitude passthrough of first ordinate'
   );
   
   -- 3.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,13107309::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   -- 3.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'WI'
      ,'test 3 - check state code 1'
   );
   
   -- 3.6
   RETURN NEXT tap.is(
       ary_nhdplusid[3]
      ,13107317::BIGINT
      ,'test 3 - check nhdplusid 2'
   );

   -- 3.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[3]
      ,'WI'
      ,'test 3 - check state code 2'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
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
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 4.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,5
      ,'test 4.2 - basic catchment count'
   );
   
   -- 4.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-65.72837
      ,'test 4.3 - check longitude passthrough of first ordinate'
   );
   
   -- 4.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,800030108::BIGINT
      ,'test 4.4 - check nhdplusid 1'
   );

   -- 4.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'PR'
      ,'test 4.5 - check state code 1'
   );
   
   -- 4.6
   RETURN NEXT tap.is(
       ary_nhdplusid[5]
      ,800036551::BIGINT
      ,'test 4.6 - check nhdplusid 2'
   );

   -- 4.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[5]
      ,'PR'
      ,'test 4.7 - check state code 2'
   );
   
   ----------------------------------------------------------------------------
   json_input := JSONB_BUILD_OBJECT(
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
   
   ary_nhdplusid := ARRAY(
      SELECT a.nhdplusid FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   ary_catchmentstatecode := ARRAY(
      SELECT a.catchmentstatecode FROM tmp_cip_out a ORDER BY a.catchmentstatecode,a.nhdplusid
   );
   
   -- 5.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,49
      ,'test 5.2 - basic catchment count'
   );
   
   -- 5.3
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-115.84877
      ,'test 5.3 - check longitude passthrough of first ordinate'
   );
   
   -- 5.4
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,22975990::BIGINT
      ,'test 5.4 - check nhdplusid 1'
   );

   -- 5.5
   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MT'
      ,'test 5.5 - check state code 1'
   );
   
   -- 5.6
   RETURN NEXT tap.is(
       ary_nhdplusid[46]
      ,25073170::BIGINT
      ,'test 5.6 - check nhdplusid 2'
   );

   -- 5.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[46]
      ,'MT'
      ,'test 5.7 - check state code 2'
   );

END;$$;
--******************************--
----- line_simple_hr.sql 

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
--******************************--
----- line_simple_mr.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.line_simple_mr()
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
      ,p_nhdplus_version                := 'MR'
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
      ,'test 1.1 - return code ' || rec.out_return_code
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
      ,7
      ,'test 1.2 - basic catchment count ' || rec.out_catchment_count || ' = 7'
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
      ,16964227::BIGINT
      ,'test 1.4 - check nhdplusid 1: ' || ary_nhdplusid[1] || ' = 16964227'
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
      ,16964235::BIGINT
      ,'test 1.6 - check nhdplusid 6: ' || ary_nhdplusid[6] || ' = 16964235'
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
      ,p_nhdplus_version                := 'MR'
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
      ,'test 2.1 - return code ' || rec.out_return_code
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
      ,4
      ,'test 2.2 - basic catchment count ' || rec.out_catchment_count || ' = 4'
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
      ,10850238::BIGINT
      ,'test 2.4 - check nhdplusid 1: ' || ary_nhdplusid[1] || ' = 10850238'
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
      ,10850242::BIGINT
      ,'test 2.6 - check nhdplusid 2: ' || ary_nhdplusid[2] || ' = 10850242'
   );

   -- 2.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'MI'
      ,'test 2.7 - check state code 2'
   );
   
   -- 2.8
   RETURN NEXT tap.is(
       ary_nhdplusid[3]
      ,10850250::BIGINT
      ,'test 2.8 - check nhdplusid 3: ' || ary_nhdplusid[3] || ' = 10850250'
   );

   -- 2.9
   RETURN NEXT tap.is(
       ary_catchmentstatecode[3]
      ,'MI'
      ,'test 2.9 - check state code 3'
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
      ,p_nhdplus_version                := 'MR'
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
      ,800001779::BIGINT
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
      ,p_nhdplus_version                := 'MR'
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
      ,6333652::BIGINT
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
      ,6333654::BIGINT
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
      ,p_nhdplus_version                := 'MR'
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
      ,239
      ,'test 5.2 - basic catchment count ' || rec.out_catchment_count || ' = 239'
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
      ,7082708::BIGINT
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
      ,7090729::BIGINT
      ,'test 5.6 - check nhdplusid 2'
   );

   -- 5.7
   RETURN NEXT tap.is(
       ary_catchmentstatecode[236]
      ,'MN'
      ,'test 5.7 - check state code 2'
   );

END;$$;
--******************************--
----- point_simple_hr.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.point_simple_hr()
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
      ,p_nhdplus_version                := 'HR'
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
      ,'test 1.1 - return_code' 
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   -- 1.2
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 1.2 - basic catchment count ' || rec.out_catchment_count || ' = 1'
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
      ,10000500005418::BIGINT
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
      ,p_nhdplus_version                := 'HR'
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
      ,'test 2.2 - basic catchment count'
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
      ,20000800014723::BIGINT
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
      ,p_nhdplus_version                := 'HR'
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
      ,'test 3.1 - return code'
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
      ,55000700291103::BIGINT
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
      ,p_nhdplus_version                := 'HR'
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
      ,80000600000634::BIGINT
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
      ,p_nhdplus_version                := 'HR'
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
      ,85000200000517::BIGINT
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
      ,p_nhdplus_version                := 'HR'
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
      ,90000300000563::BIGINT
      ,'test 6.4 - check nhdplusid'
   );

   -- 6.5
   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'AS'
      ,'test 6.5 - check state code'
   );

END;$$;
--******************************--
----- point_simple_mr.sql 

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
--******************************--
----- nav_ut_shorty.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_ut_shorty()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   num_max_distancekm  NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 3840993;
   num_start_measure   := 5.66198;
   num_max_distancekm  := 15;
   
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'UT'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('MR UT SHORTY ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,1::INT
      ,'test 1.2 - flowline_count'
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 30000400017915;
   num_start_measure   := 4.56026;
   num_max_distancekm  := 15;
   
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'UT'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('HR UT SHORTY  ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 2.1 = return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,19::INT
      ,'test 2.2 - flowline_count'
   );

END;$$;
--******************************--
----- nav_ut_350.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_ut_350()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   num_max_distancekm  NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 22814447;
   num_start_measure   := 1.10728;
   num_max_distancekm  := 350;
   
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'UT'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('MR UT ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,125::INT
      ,'test 1.2 - flowline count'
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 20000100000044;
   num_start_measure   := 14.0713;
   num_max_distancekm  := 350;
   
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'UT'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('HR UT ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 2.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,601::INT
      ,'test 2.2 - flowline count'
   );

END;$$;
--******************************--
----- nav_um_350.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_um_350()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   num_max_distancekm  NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 23207768;
   num_start_measure   := 14.0492;
   num_max_distancekm  := 350;
   
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'UM'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('MR UM ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,218::INT
      ,'test 1.2 - flowline count'
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 55000500061680;
   num_start_measure   := 84.56673;
   num_max_distancekm  := 350;
   
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'UM'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('HR UM ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );
   
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,752::INT
      ,'test 1.2 - flowline count'
   );

END;$$;
--******************************--
----- nav_dm_350.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_dm_350()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   num_max_distancekm  NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 17819112;
   num_start_measure   := 14.0492;
   num_max_distancekm  := 350;
   
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'DM'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('MR DM ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );
   
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,224::INT
      ,'test 1.2 - flowline_count'
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 35000600173658;
   num_start_measure   := 67.56459;
   num_max_distancekm  := 350;
   
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'DM'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('HR DM ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 2.1 - return_code'
   );
   
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,441::INT
      ,'test 2.1 - flowline_count'
   );

END;$$;
--******************************--
----- nav_dd_350.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_dd_350()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   num_max_distancekm  NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 19085559;
   num_start_measure   := 22.81061;
   num_max_distancekm  := 350;
   
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'DD'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('MR DD ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,616::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 2000030001763;
   num_start_measure   := 19.18331;
   num_max_distancekm  := 350;
   
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'DD'
      ,p_start_nhdplusid            := 20000300017631
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 19.18331
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := 350
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('HR DD ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,3719::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );

END;$$;
--******************************--
----- nav_pp_short.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_pp_short()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   int_stop_nhdplusid  BIGINT;
   num_stop_measure    NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 6186320;
   num_start_measure   := 42.79978;
   int_stop_nhdplusid  := 6189080;
   num_stop_measure    := 88.4296;
   
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'PP'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := int_stop_nhdplusid
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := num_stop_measure
      ,p_max_distancekm             := NULL
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('MR PP ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,int_stop_nhdplusid,num_stop_measure],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,6::INT
      ,'test 1.2 - flowline_count'
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 10000700016991;
   num_start_measure   := 41.46198;
   int_stop_nhdplusid  := 10000700036090;
   num_stop_measure    := 91.89343;
   
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'PP'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := int_stop_nhdplusid
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := num_stop_measure
      ,p_max_distancekm             := NULL
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('HR PP ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,int_stop_nhdplusid,num_stop_measure],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 2.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,7::INT
      ,'test 2.2 - flowline count'
   );

END;$$;
--******************************--
----- nav_ppall_short.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_ppall_short()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   int_stop_nhdplusid  BIGINT;
   num_stop_measure    NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 2788639;
   num_start_measure   := 27.31578;
   int_stop_nhdplusid  := 1897288;
   num_stop_measure    := 13.76972;
   
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'PPALL'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := int_stop_nhdplusid
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := num_stop_measure
      ,p_max_distancekm             := NULL
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('MR PPALL ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,int_stop_nhdplusid,num_stop_measure],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,135::INT
      ,'test 1.2 - flowline count'
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 50000800141148;
   num_start_measure   := 7.01946;
   int_stop_nhdplusid  := 50000800142389;
   num_stop_measure    := 63.64216;
   
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'PPALL'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := int_stop_nhdplusid
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := num_stop_measure
      ,p_max_distancekm             := NULL
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('HR PPALL ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,int_stop_nhdplusid,num_stop_measure],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 2.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,242::INT
      ,'test 2.2 - flowline count'
   );

END;$$;
--******************************--
----- nav_utnmd.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_utnmd()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   num_max_distancekm  NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 17219402;
   num_start_measure   := 12.85557;
   num_max_distancekm  := 15;
   
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'UTNMD'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('MR UTNMD ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,6::INT
      ,'test 1.2 - flowline count ' || rec.out_flowline_count::VARCHAR
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 23000300000332;
   num_start_measure   := 69.59222;
   num_max_distancekm  := 15;
   
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'UTNMD'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := num_max_distancekm
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('HR UTNMD ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 2.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,11::INT
      ,'test 2.2 - flowline count ' || rec.out_flowline_count::VARCHAR
   );

END;$$;
--******************************--
----- delineate_1.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.delineate_1()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   num_max_distancekm  NUMERIC;
   int_tmp             INTEGER;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 6667867;
   num_start_measure   := 25.636;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_m.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'SPATIAL'
      ,p_split_initial_catchment     := FALSE
      ,p_fill_basin_holes            := FALSE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('MR UT SPATIAL ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 63;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 1.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 6667867;
   num_start_measure   := 25.636;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_m.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'TOPO'
      ,p_split_initial_catchment     := FALSE
      ,p_fill_basin_holes            := FALSE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('MR UT TOPO ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 2.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 63;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 2.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 6667867;
   num_start_measure   := 25.636;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_m.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'NONE'
      ,p_split_initial_catchment     := FALSE
      ,p_fill_basin_holes            := FALSE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('MR UT NONE ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 3.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 63;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 3.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 65000200047343;
   num_start_measure   := 25.41957;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_h.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'SPATIAL'
      ,p_split_initial_catchment     := FALSE
      ,p_fill_basin_holes            := FALSE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('HR UT SPATIAL ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 4.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 208;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 4.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 65000200047343;
   num_start_measure   := 25.41957;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_h.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'TOPO'
      ,p_split_initial_catchment     := FALSE
      ,p_fill_basin_holes            := FALSE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('HR UT TOPO ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 5.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 208;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 5.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 65000200047343;
   num_start_measure   := 25.41957;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_h.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'NONE'
      ,p_split_initial_catchment     := FALSE
      ,p_fill_basin_holes            := FALSE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('HR UT NONE ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 6.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 208;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 6.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 65000200047343;
   num_start_measure   := 25.41957;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_h.delineate(
       p_search_type                 := 'UTNMD'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'NONE'
      ,p_split_initial_catchment     := FALSE
      ,p_fill_basin_holes            := FALSE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('HR UTNMD NONE ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 7.1 - return_code'
   );

   int_tmp := 188;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 7.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );

END;$$;
--******************************--
----- delineate_2.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.delineate_2()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   num_max_distancekm  NUMERIC;
   int_tmp             INTEGER;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 6667867;
   num_start_measure   := 25.636;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_m.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'SPATIAL'
      ,p_split_initial_catchment     := TRUE
      ,p_fill_basin_holes            := TRUE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('MR UT SPATIAL ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 63;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 1.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 6667867;
   num_start_measure   := 25.636;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_m.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'TOPO'
      ,p_split_initial_catchment     := TRUE
      ,p_fill_basin_holes            := TRUE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('MR UT TOPO ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 2.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 63;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 2.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 6667867;
   num_start_measure   := 25.636;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_m.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'NONE'
      ,p_split_initial_catchment     := TRUE
      ,p_fill_basin_holes            := TRUE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('MR UT NONE ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 3.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 63;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 3.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 65000200047343;
   num_start_measure   := 25.41957;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_h.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'SPATIAL'
      ,p_split_initial_catchment     := TRUE
      ,p_fill_basin_holes            := TRUE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('HR UT SPATIAL ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 4.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 208;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 4.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 65000200047343;
   num_start_measure   := 25.41957;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_h.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'TOPO'
      ,p_split_initial_catchment     := TRUE
      ,p_fill_basin_holes            := TRUE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('HR UT TOPO ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 5.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 208;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 5.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 65000200047343;
   num_start_measure   := 25.41957;
   num_max_distancekm  := 55;
   
   rec := cipsrv_nhdplus_h.delineate(
       p_search_type                 := 'UT'
      ,p_start_nhdplusid             := int_start_nhdplusid
      ,p_start_permanent_identifier  := NULL
      ,p_start_reachcode             := NULL
      ,p_start_hydroseq              := NULL
      ,p_start_measure               := num_start_measure
      ,p_stop_nhdplusid              := NULL
      ,p_stop_permanent_identifier   := NULL
      ,p_stop_reachcode              := NULL
      ,p_stop_hydroseq               := NULL
      ,p_stop_measure                := NULL
      ,p_max_distancekm              := num_max_distancekm
      ,p_max_flowtimeday             := NULL
      ,p_aggregation_engine          := 'NONE'
      ,p_split_initial_catchment     := TRUE
      ,p_fill_basin_holes            := TRUE
      ,p_force_no_cache              := FALSE
      ,p_return_delineation_geometry := TRUE
      ,p_return_flowlines            := TRUE
      ,p_return_flowline_details     := TRUE
      ,p_return_flowline_geometry    := TRUE
      ,p_known_region                := NULL
   );
   
   RETURN NEXT tap.diag('HR UT NONE ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 6.1 - return_code ' || rec.out_return_code::VARCHAR
   );

   int_tmp := 208;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 6.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );

END;$$;
--******************************--
----- pointindexing_1.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.pointindexing_1()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   int_tmp       BIGINT;
   
BEGIN
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.pointindexing(
       p_point                        := ST_SETSRID(ST_POINT(-96.735677779,38.699056373),4326)
      ,p_indexing_engine              := 'DISTANCE'
      ,p_fcode_allow                  := NULL
      ,p_fcode_deny                   := NULL
      ,p_distance_max_distkm          := 25
      ,p_raindrop_snap_max_distkm     := NULL
      ,p_raindrop_path_max_distkm     := NULL
      ,p_limit_innetwork              := FALSE
      ,p_limit_navigable              := FALSE
      ,p_fallback_fcode_allow         := NULL
      ,p_fallback_fcode_deny          := NULL
      ,p_fallback_distance_max_distkm := NULL
      ,p_fallback_limit_innetwork     := NULL
      ,p_fallback_limit_navigable     := NULL
      ,p_return_link_path             := TRUE
      ,p_known_region                 := NULL
      ,p_known_catchment_nhdplusid    := NULL 
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
   );
   
   int_tmp := 20928204;
   RETURN NEXT tap.is(
       rec.out_nhdplusid
      ,int_tmp
      ,'test 1.2 - nhdplusid ' || rec.out_nhdplusid || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.pointindexing(
       p_point                        := ST_SETSRID(ST_POINT(-96.735677779,38.699056373),4326)
      ,p_indexing_engine              := 'CATCONSTRAINED'
      ,p_fcode_allow                  := NULL
      ,p_fcode_deny                   := NULL
      ,p_distance_max_distkm          := NULL
      ,p_raindrop_snap_max_distkm     := NULL
      ,p_raindrop_path_max_distkm     := NULL
      ,p_limit_innetwork              := FALSE
      ,p_limit_navigable              := FALSE
      ,p_fallback_fcode_allow         := NULL
      ,p_fallback_fcode_deny          := NULL
      ,p_fallback_distance_max_distkm := NULL
      ,p_fallback_limit_innetwork     := NULL
      ,p_fallback_limit_navigable     := NULL
      ,p_return_link_path             := TRUE
      ,p_known_region                 := NULL
      ,p_known_catchment_nhdplusid    := NULL 
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 2.1 - return_code'
   );
   
   int_tmp := 20928204;
   RETURN NEXT tap.is(
       rec.out_nhdplusid
      ,int_tmp
      ,'test 2.2 - nhdplusid ' || rec.out_nhdplusid || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.pointindexing(
       p_point                        := ST_SETSRID(ST_POINT(-96.735677779,38.699056373),4326)
      ,p_indexing_engine              := 'RAINDROP'
      ,p_fcode_allow                  := NULL
      ,p_fcode_deny                   := NULL
      ,p_distance_max_distkm          := NULL
      ,p_raindrop_snap_max_distkm     := 25
      ,p_raindrop_path_max_distkm     := 0.75
      ,p_limit_innetwork              := FALSE
      ,p_limit_navigable              := FALSE
      ,p_fallback_fcode_allow         := NULL
      ,p_fallback_fcode_deny          := NULL
      ,p_fallback_distance_max_distkm := NULL
      ,p_fallback_limit_innetwork     := NULL
      ,p_fallback_limit_navigable     := NULL
      ,p_return_link_path             := TRUE
      ,p_known_region                 := NULL
      ,p_known_catchment_nhdplusid    := NULL 
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1 - return_code'
   );
   
   int_tmp := 20928204;
   RETURN NEXT tap.is(
       rec.out_nhdplusid
      ,int_tmp
      ,'test 3.2 - nhdplusid ' || rec.out_nhdplusid || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.pointindexing(
       p_point                        := ST_SETSRID(ST_POINT(-96.735677779,38.699056373),4326)
      ,p_indexing_engine              := 'DISTANCE'
      ,p_fcode_allow                  := NULL
      ,p_fcode_deny                   := NULL
      ,p_distance_max_distkm          := 25
      ,p_raindrop_snap_max_distkm     := NULL
      ,p_raindrop_path_max_distkm     := NULL
      ,p_limit_innetwork              := FALSE
      ,p_limit_navigable              := FALSE
      ,p_fallback_fcode_allow         := NULL
      ,p_fallback_fcode_deny          := NULL
      ,p_fallback_distance_max_distkm := NULL
      ,p_fallback_limit_innetwork     := NULL
      ,p_fallback_limit_navigable     := NULL
      ,p_return_link_path             := TRUE
      ,p_known_region                 := NULL
      ,p_known_catchment_nhdplusid    := NULL 
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 4.1 - return_code'
   );
   
   int_tmp := 21001300058689;
   RETURN NEXT tap.is(
       rec.out_nhdplusid
      ,int_tmp
      ,'test 4.2 - nhdplusid ' || rec.out_nhdplusid || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.pointindexing(
       p_point                        := ST_SETSRID(ST_POINT(-96.735677779,38.699056373),4326)
      ,p_indexing_engine              := 'CATCONSTRAINED'
      ,p_fcode_allow                  := NULL
      ,p_fcode_deny                   := NULL
      ,p_distance_max_distkm          := NULL
      ,p_raindrop_snap_max_distkm     := NULL
      ,p_raindrop_path_max_distkm     := NULL
      ,p_limit_innetwork              := FALSE
      ,p_limit_navigable              := FALSE
      ,p_fallback_fcode_allow         := NULL
      ,p_fallback_fcode_deny          := NULL
      ,p_fallback_distance_max_distkm := NULL
      ,p_fallback_limit_innetwork     := NULL
      ,p_fallback_limit_navigable     := NULL
      ,p_return_link_path             := TRUE
      ,p_known_region                 := NULL
      ,p_known_catchment_nhdplusid    := NULL 
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code'
   );
   
   int_tmp := 21001300058689;
   RETURN NEXT tap.is(
       rec.out_nhdplusid
      ,int_tmp
      ,'test 5.2 - nhdplusid ' || rec.out_nhdplusid || ' = ' || int_tmp
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.pointindexing(
       p_point                        := ST_SETSRID(ST_POINT(-96.735677779,38.699056373),4326)
      ,p_indexing_engine              := 'RAINDROP'
      ,p_fcode_allow                  := NULL
      ,p_fcode_deny                   := NULL
      ,p_distance_max_distkm          := NULL
      ,p_raindrop_snap_max_distkm     := 25
      ,p_raindrop_path_max_distkm     := 0.75
      ,p_limit_innetwork              := FALSE
      ,p_limit_navigable              := FALSE
      ,p_fallback_fcode_allow         := NULL
      ,p_fallback_fcode_deny          := NULL
      ,p_fallback_distance_max_distkm := NULL
      ,p_fallback_limit_innetwork     := NULL
      ,p_fallback_limit_navigable     := NULL
      ,p_return_link_path             := TRUE
      ,p_known_region                 := NULL
      ,p_known_catchment_nhdplusid    := NULL 
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 6.1 - return_code'
   );
   
   int_tmp := 21001300058689;
   RETURN NEXT tap.is(
       rec.out_nhdplusid
      ,int_tmp
      ,'test 6.2 - nhdplusid ' || rec.out_nhdplusid || ' = ' || int_tmp
   );

END;$$;
--******************************--
----- updn_1.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.updn_1()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   num_max_distancekm  NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 65000200047343;
   num_start_measure   := 74.0892;
   num_max_distancekm  := 25;
   
   rec := cipsrv_owld.upstreamdownstream(
       p_nhdplus_version               := 'HR'
      ,p_search_type                   := 'UT'
      ,p_start_nhdplusid               := int_start_nhdplusid
      ,p_start_permanent_identifier    := NULL
      ,p_start_reachcode               := NULL
      ,p_start_hydroseq                := NULL
      ,p_start_measure                 := num_start_measure
      ,p_start_source_featureid        := NULL
      ,p_start_source_featureid2       := NULL
      ,p_start_source_originator       := NULL
      ,p_start_source_series           := NULL
      ,p_start_start_date              := NULL
      ,p_start_end_date                := NULL
      ,p_start_permid_joinkey          := NULL
      ,p_start_source_joinkey          := NULL
      ,p_start_cip_joinkey             := NULL
      ,p_start_linked_data_program     := NULL
      ,p_start_search_precision        := NULL
      ,p_start_push_rad_for_permid     := NULL
      ,p_stop_nhdplusid                := NULL
      ,p_stop_permanent_identifier     := NULL
      ,p_stop_reachcode                := NULL
      ,p_stop_hydroseq                 := NULL
      ,p_stop_measure                  := NULL
      ,p_stop_source_featureid         := NULL
      ,p_stop_source_featureid2        := NULL
      ,p_stop_source_originator        := NULL
      ,p_stop_source_series            := NULL
      ,p_stop_start_date               := NULL
      ,p_stop_end_date                 := NULL
      ,p_stop_permid_joinkey           := NULL
      ,p_stop_source_joinkey           := NULL
      ,p_stop_cip_joinkey              := NULL
      ,p_stop_linked_data_program      := NULL
      ,p_stop_search_precision         := NULL
      ,p_stop_push_rad_for_permid      := NULL
      ,p_max_distancekm                := num_max_distancekm
      ,p_max_flowtimeday               := NULL
      ,p_linked_data_search_list       := ARRAY['WQP']
      ,p_search_precision              := 'CATCHMENT'
      ,p_return_flowlines              := TRUE
      ,p_return_flowline_details       := TRUE
      ,p_return_flowline_geometry      := FALSE
      ,p_return_catchments             := TRUE
      ,p_return_linked_data_cip        := FALSE
      ,p_return_linked_data_huc12      := FALSE
      ,p_return_linked_data_source     := FALSE
      ,p_return_linked_data_rad        := FALSE
      ,p_return_linked_data_attributes := FALSE
      ,p_remove_stop_start_sfids       := FALSE
      ,p_push_source_geometry_as_rad   := FALSE
      ,p_known_region                  := NULL
   );
   
   RETURN NEXT tap.diag('UPDN ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));

   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );
   
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,63::INT
      ,'test 1.2 flowline count ' || rec.out_flowline_count || ' = 63 '
   );
   
   RETURN NEXT tap.is(
       rec.out_catchment_count::INT
      ,63::INT
      ,'test 1.3 catchment count ' || rec.out_catchment_count || ' = 63 '
   );
   
END;$$;
--******************************--
----- fdr_flowaccumulation.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.fdr_flowaccumulation()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   sdo_input_geom      GEOMETRY;
   
BEGIN
   
   ----------------------------------------------------------------------------
   sdo_input_geom      := ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[-113.431091,33.440466],[-113.441906,33.44505],[-113.437786,33.431441],[-113.443794,33.38831],[-113.436928,33.379136],[-113.427658,33.38487],[-113.424911,33.437458],[-113.431091,33.440466]]]}');
   
   rec := cipsrv_nhdplus_m.fdr_flowaccumulation(
       p_area_of_interest      := sdo_input_geom
   );
   
   RETURN NEXT tap.diag('MR flow accumulation ');
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_max_accumulation
      ,8321
      ,'test 1.1 - max accumulation'
   );
   
   ----------------------------------------------------------------------------
   sdo_input_geom      := ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[-113.431091,33.440466],[-113.441906,33.44505],[-113.437786,33.431441],[-113.443794,33.38831],[-113.436928,33.379136],[-113.427658,33.38487],[-113.424911,33.437458],[-113.431091,33.440466]]]}');
   
   rec := cipsrv_nhdplus_h.fdr_flowaccumulation(
       p_area_of_interest      := sdo_input_geom
   );
   
   RETURN NEXT tap.diag('HR flow accumulation ');
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 3.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_max_accumulation
      ,73517
      ,'test 3.1 - max accumulation'
   );
   
END;$$;
--******************************--
----- randomnav.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.randomnav()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   int_tmp       BIGINT;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- MR 
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := NULL
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := '5070'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 2.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := '26904'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := '32161'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 4.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := '32655'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := '32702'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 6.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- HR 
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := NULL
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 7.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := '5070'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 8.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := '26904'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 9.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := '32161'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 10.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := '32655'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 11.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := '32702'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 12.1 - return_code'
   );

END;$$;
--******************************--
----- randompoint.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.randompoint()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   int_tmp       BIGINT;
   
BEGIN
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   -- MR
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := NULL
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '5070'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 2.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '26904'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32161'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 4.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32655'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32702'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 6.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   -- HR
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := NULL
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 7.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '5070'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 8.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '26904'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 9.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32161'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 10.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32655'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 11.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32702'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 12.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   -- Extended MR
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := NULL
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 13.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '3338'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 14.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '5070'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 15.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '26904'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 16.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32161'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 17.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32655'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 18.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32702'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 19.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := NULL
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 20.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '3338'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 21.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '5070'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 22.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '26904'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 23.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32161'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 24.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32655'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 25.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32702'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 26.1 - return_code'
   );

END;$$;
--******************************--
----- randomcatchment.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.randomcatchment()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   int_tmp       BIGINT;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- MR
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := NULL
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '5070'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 2.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '26904'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '32161'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 4.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '32655'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '32702'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 6.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- MR Extended
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := NULL
      ,p_include_extended := TRUE
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 7.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '3338'
      ,p_include_extended := TRUE
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 8.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '5070'
      ,p_include_extended := TRUE
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 9.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '26904'
      ,p_include_extended := TRUE
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 10.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '32161'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 11.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '32655'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 12.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := '32702'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 13.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- HR
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := NULL
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 14.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '5070'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 15.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '26904'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 16.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '32161'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 17.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '32655'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 18.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '32702'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 19.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- HR Extended
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := NULL
      ,p_include_extended := TRUE
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 20.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '3338'
      ,p_include_extended := TRUE
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 21.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '5070'
      ,p_include_extended := TRUE
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 22.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '26904'
      ,p_include_extended := TRUE
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 23.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '32161'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 24.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '32655'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 25.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := '32702'
      ,p_include_extended := NULL
	   ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 26.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- MR Extended
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
       p_region           := NULL
      ,p_include_extended := FALSE
	   ,p_return_geometry  := TRUE
      ,p_known_nhdplusid  := 6689021
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 27.1 - return_code'
   );
   
   RETURN NEXT tap.is(
       rec.out_nhdplusid
      ,6689021::BIGINT
      ,'test 27.2 - out_nhdplusid'
   );
   
   ----------------------------------------------------------------------------
   -- HR Extended
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomcatchment(
       p_region           := NULL
      ,p_include_extended := FALSE
	   ,p_return_geometry  := TRUE
      ,p_known_nhdplusid  := 65000200023666
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 28.1 - return_code'
   );
   
   RETURN NEXT tap.is(
       rec.out_nhdplusid
      ,65000200023666::BIGINT
      ,'test 28.2 - out_nhdplusid'
   );
   

END;$$;
--******************************--
----- randomppnav.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.randomppnav()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   int_tmp       BIGINT;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- MR
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomppnav(
       p_region           := NULL
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomppnav(
       p_region           := '5070'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 2.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomppnav(
       p_region           := '26904'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomppnav(
       p_region           := '32161'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 4.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomppnav(
       p_region           := '32655'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomppnav(
       p_region           := '32702'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 6.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- HR
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomppnav(
       p_region           := NULL
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 7.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomppnav(
       p_region           := '5070'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 8.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomppnav(
       p_region           := '26904'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 9.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomppnav(
       p_region           := '32161'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 10.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomppnav(
       p_region           := '32655'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 11.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomppnav(
       p_region           := '32702'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 12.1 - return_code'
   );

END;$$;
--******************************--
----- randomhuc12.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.randomhuc12()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   int_tmp       BIGINT;
   
BEGIN
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := NULL
      ,p_source_dataset  := NULL
      ,p_return_geometry := FALSE
      ,p_known_huc12     := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := NULL
      ,p_source_dataset  := 'NP21'
      ,p_return_geometry := TRUE
      ,p_known_huc12     := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 2.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := NULL
      ,p_source_dataset  := 'NPHR'
      ,p_return_geometry := TRUE
      ,p_known_huc12     := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := NULL
      ,p_source_dataset  := 'F3'
      ,p_return_geometry := TRUE
      ,p_known_huc12     := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 4.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := NULL
      ,p_source_dataset  := 'F3'
      ,p_return_geometry := TRUE
      ,p_known_huc12     := '111401050705'
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code'
   );
   
   RETURN NEXT tap.is(
       rec.out_huc12
      ,'111401050705'
      ,'test 5.2 - huc12 ' || rec.out_huc12
   );
   
   RETURN NEXT tap.is(
       rec.out_huc12_name
      ,'Lower Frazier Creek'
      ,'test 5.3 - huc12 ' || rec.out_huc12_name
   );
   

END;$$;
