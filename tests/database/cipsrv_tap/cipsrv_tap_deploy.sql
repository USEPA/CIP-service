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
      ,26
      ,'test 1 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-119.16218
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,17573033::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'CA'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,17573055::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'CA'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[20]
      ,17573739::BIGINT
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
      ,p_nhdplus_version                := 'MR'
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
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
      ,33
      ,'test 1 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-89.21310
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,22000100008208::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'IL'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[23]
      ,24000100421021::BIGINT
      ,'test 1 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[23]
      ,'KY'
      ,'test 1 - check state code 2'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[31]
      ,22000100008400::BIGINT
      ,'test 1 - check nhdplusid 3'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[31]
      ,'MO'
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
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
      ,85000100000447::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'PR'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[12]
      ,85000100013695::BIGINT
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
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
      ,29
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-109.03776
      ,'test 3 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,41000600049938::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'AZ'
      ,'test 3 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[5]
      ,41000600005775::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[5]
      ,'CO'
      ,'test 3 - check state code 1'
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
      ,p_wbd_version                    := NULL
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
      ,p_wbd_version                    := NULL
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
      ,p_wbd_version                    := NULL
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
      ,p_wbd_version                    := NULL
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
      ,p_wbd_version                    := NULL
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
      ,p_wbd_version                    := NULL
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
      ,p_wbd_version                    := NULL
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
      ,p_wbd_version                    := NULL
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
      ,8
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
      ,p_wbd_version                    := NULL
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
      ,p_wbd_version                    := NULL
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
      ,p_wbd_version                    := NULL
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
      ,p_nhdplus_version                := 'HR'
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
      ,10
      ,'test 1 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-91.10619
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,22000400002754::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'IA'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[10]
      ,22000400022592::BIGINT
      ,'test 1 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[10]
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
      ,p_nhdplus_version                := 'HR'
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
      ,15000600198758::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'GA'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[4]
      ,15000600257623::BIGINT
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
      ,p_nhdplus_version                := 'HR'
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
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-91.86905
      ,'test 3 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,22001300007596::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'WI'
      ,'test 3 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[3]
      ,22001300011102::BIGINT
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
      ,p_nhdplus_version                := 'HR'
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
      ,'test 4 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-65.72837
      ,'test 4 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,85000100010380::BIGINT
      ,'test 4 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'PR'
      ,'test 4 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[4]
      ,85000100016890::BIGINT
      ,'test 4 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[4]
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
      ,p_nhdplus_version                := 'HR'
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
      ,129
      ,'test 5 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-115.84877
      ,'test 5 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,55001100000045::BIGINT
      ,'test 5 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MT'
      ,'test 5 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[46]
      ,55001100106215::BIGINT
      ,'test 5 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[46]
      ,'MT'
      ,'test 5 - check state code 2'
   );

END;$$
--******************************--
----- line_levelpath_mr.sql 

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
--******************************--
----- line_simple_hr.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.line_simple_hr()
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
                JSONB_BUILD_ARRAY(-108.75761,36.99131)
               ,JSONB_BUILD_ARRAY(-108.753147,36.979518)
               ,JSONB_BUILD_ARRAY(-108.740788,36.98706)
               ,JSONB_BUILD_ARRAY(-108.734779,36.981986)
               ,JSONB_BUILD_ARRAY(-108.723621,36.977598)
               ,JSONB_BUILD_ARRAY(-108.717442,36.982398)
             )
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
      ,11
      ,'test 1 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-108.75761
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,41000600117044::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'CO'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[6]
      ,41000600095221::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[6]
      ,'NM'
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
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
      ,-83.08565
      ,'test 2 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,60001500030443::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MI'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,60001500030695::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'MI'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[3]
      ,60001500030697::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[3]
      ,'MI'
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
                JSONB_BUILD_ARRAY(-156.177092,20.628768)
               ,JSONB_BUILD_ARRAY(-156.173573,20.628607)
               ,JSONB_BUILD_ARRAY(-156.170998,20.630615)
               ,JSONB_BUILD_ARRAY(-156.168337,20.629009)
             )
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
      ,1
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-156.17709
      ,'test 3 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,80000200001830::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'HI'
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
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
      ,'test 4 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-84.02936
      ,'test 4 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,15001600047400::BIGINT
      ,'test 4 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'GA'
      ,'test 4 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,15001600124060::BIGINT
      ,'test 4 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'GA'
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
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
      ,930
      ,'test 5 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-95.69092
      ,'test 5 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,65000200000196::BIGINT
      ,'test 5 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MN'
      ,'test 5 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[236]
      ,65000200034219::BIGINT
      ,'test 5 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[236]
      ,'MN'
      ,'test 5 - check state code 2'
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
                JSONB_BUILD_ARRAY(-108.75761,36.99131)
               ,JSONB_BUILD_ARRAY(-108.753147,36.979518)
               ,JSONB_BUILD_ARRAY(-108.740788,36.98706)
               ,JSONB_BUILD_ARRAY(-108.734779,36.981986)
               ,JSONB_BUILD_ARRAY(-108.723621,36.977598)
               ,JSONB_BUILD_ARRAY(-108.717442,36.982398)
             )
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
      ,7
      ,'test 1 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-108.75761
      ,'test 1 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,16964227::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'CO'
      ,'test 1 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[6]
      ,16964235::BIGINT
      ,'test 1 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[6]
      ,'NM'
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
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
      ,'test 2 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-83.08565
      ,'test 2 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,10850238::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MI'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,10850250::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'MI'
      ,'test 2 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[3]
      ,10850310::BIGINT
      ,'test 2 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[3]
      ,'MI'
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
                JSONB_BUILD_ARRAY(-156.177092,20.628768)
               ,JSONB_BUILD_ARRAY(-156.173573,20.628607)
               ,JSONB_BUILD_ARRAY(-156.170998,20.630615)
               ,JSONB_BUILD_ARRAY(-156.168337,20.629009)
             )
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
      ,1
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-156.17709
      ,'test 3 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,800001779::BIGINT
      ,'test 3 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'HI'
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
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
      ,'test 4 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_lines->'features'->0->'geometry'->'coordinates'->0->0)::NUMERIC,5)
      ,-84.02936
      ,'test 4 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,6333652::BIGINT
      ,'test 4 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'GA'
      ,'test 4 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[2]
      ,6333654::BIGINT
      ,'test 4 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[2]
      ,'GA'
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
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
      ,239
      ,'test 5 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_areas->'features'->0->'geometry'->'coordinates'->0->0->0)::NUMERIC,5)
      ,-95.69092
      ,'test 5 - check longitude passthrough of first ordinate'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[1]
      ,7082708::BIGINT
      ,'test 5 - check nhdplusid 1'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[1]
      ,'MN'
      ,'test 5 - check state code 1'
   );
   
   RETURN NEXT tap.is(
       ary_nhdplusid[236]
      ,7090729::BIGINT
      ,'test 5 - check nhdplusid 2'
   );

   RETURN NEXT tap.is(
       ary_catchmentstatecode[236]
      ,'MN'
      ,'test 5 - check state code 2'
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
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-76.98669433593751,38.88595542095899)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := ARRAY['DC']
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 1 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-76.98669
      ,'test 1 - check longitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,10000500005418::BIGINT
      ,'test 1 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'DC'
      ,'test 1 - check state code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-91.58889770507812,31.05646337884346)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 2 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->1)::NUMERIC,5)
      ,31.05646
      ,'test 2 - check latitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,20000800014723::BIGINT
      ,'test 2 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'LA'
      ,'test 2 - check state code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-117.05795288085939,42.07580094787546)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := ARRAY['OR']
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-117.05795
      ,'test 3 - check longitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,55000700291103::BIGINT
      ,'test 3 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'OR'
      ,'test 3 - check state code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-157.80349731445315,21.414719215736195)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 4 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-157.80350
      ,'test 4 - check longitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,80000600000634::BIGINT
      ,'test 4 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'HI'
      ,'test 4 - check state code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-64.95057106018068,18.342631352511194)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 5 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-64.95057
      ,'test 5 - check longitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,85000200000517::BIGINT
      ,'test 5 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'VI'
      ,'test 5 - check state code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-170.72050094604495,-14.32842599932282)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'HR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 6 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-170.72050
      ,'test 6 - check longitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,90000300000563::BIGINT
      ,'test 6 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'AS'
      ,'test 6 - check state code'
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
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-76.98669433593751,38.88595542095899)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := ARRAY['DC']
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 1 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-76.98669
      ,'test 1 - check longitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,22338109::BIGINT
      ,'test 1 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'DC'
      ,'test 1 - check state code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-91.58889770507812,31.05646337884346)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 2 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->1)::NUMERIC,5)
      ,31.05646
      ,'test 2 - check latitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,19406818::BIGINT
      ,'test 2 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'LA'
      ,'test 2 - check state code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-117.05795288085939,42.07580094787546)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := ARRAY['OR']
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 3 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-117.05795
      ,'test 3 - check longitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,23344282::BIGINT
      ,'test 3 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'OR'
      ,'test 3 - check state code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-157.80349731445315,21.414719215736195)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 4 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-157.80350
      ,'test 4 - check longitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,800016810::BIGINT
      ,'test 4 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'HI'
      ,'test 4 - check state code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-64.95057106018068,18.342631352511194)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 5 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-64.95057
      ,'test 5 - check longitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,800041667::BIGINT
      ,'test 5 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'VI'
      ,'test 5 - check state code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := NULL
      ,p_lines                          := NULL
      ,p_areas                          := NULL
      ,p_geometry                       := JSONB_BUILD_OBJECT(
          'type'    ,'Feature'
         ,'geometry',JSONB_BUILD_OBJECT(
             'type'       ,'Point'
            ,'coordinates',JSONB_BUILD_ARRAY(-170.72050094604495,-14.32842599932282)
          )
       )
      ,p_geometry_clip                  := NULL
      ,p_geometry_clip_stage            := NULL
      ,p_catchment_filter               := NULL
      ,p_nhdplus_version                := 'MR'
      ,p_wbd_version                    := NULL
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
   );
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
   );
   
   SELECT a.nhdplusid,a.catchmentstatecode INTO int_nhdplusid,str_catchmentstatecode FROM tmp_cip_out a LIMIT 1;
   
   RETURN NEXT tap.is(
       rec.out_catchment_count
      ,1
      ,'test 6 - basic catchment count'
   );
   
   RETURN NEXT tap.is(
       ROUND((rec.out_indexed_points->'features'->0->'geometry'->'coordinates'->0)::NUMERIC,5)
      ,-170.72050
      ,'test 6 - check longitude passthrough'
   );
   
   RETURN NEXT tap.is(
       int_nhdplusid
      ,810113492::BIGINT
      ,'test 6 - check nhdplusid'
   );

   RETURN NEXT tap.is(
       str_catchmentstatecode
      ,'AS'
      ,'test 6 - check state code'
   );

END;$$;
--******************************--
----- nav_ut_shorty.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_ut_shorty()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   
BEGIN
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'UT'
      ,p_start_nhdplusid            := 3840993
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 5.66198
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := 15
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,1::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'UT'
      ,p_start_nhdplusid            := 30000400017915
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 4.56026
      ,p_stop_nhdplusid             := NULL
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := NULL
      ,p_max_distancekm             := 15
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,19::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );

END;$$;
--******************************--
----- nav_ut_350.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_ut_350()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   
BEGIN
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'UT'
      ,p_start_nhdplusid            := 22814447
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 1.10728
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

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,125::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'UT'
      ,p_start_nhdplusid            := 20000100000044
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 14.0713
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

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,601::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );

END;$$;
--******************************--
----- nav_um_350.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_um_350()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   
BEGIN
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'UM'
      ,p_start_nhdplusid            := 23207768
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 14.0492
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

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,218::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'UM'
      ,p_start_nhdplusid            := 55000500061680
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 84.56673
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

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,752::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );

END;$$;
--******************************--
----- nav_dm_350.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_dm_350()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   
BEGIN
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'DM'
      ,p_start_nhdplusid            := 17819112
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 14.0492
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

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,224::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'DM'
      ,p_start_nhdplusid            := 35000600173658
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 67.56459
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

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,441::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );

END;$$;
--******************************--
----- nav_dd_350.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_dd_350()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   
BEGIN
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'DD'
      ,p_start_nhdplusid            := 19085559
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 22.81061
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

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,616::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );
   
   ----------------------------------------------------------------------------
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
   rec           RECORD;
   
BEGIN
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'PP'
      ,p_start_nhdplusid            := 6186320
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 42.79978
      ,p_stop_nhdplusid             := 6189080
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := 88.4296
      ,p_max_distancekm             := NULL
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,6::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'PP'
      ,p_start_nhdplusid            := 10000700016991
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 41.46198
      ,p_stop_nhdplusid             := 10000700036090
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := 91.89343
      ,p_max_distancekm             := NULL
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,7::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );

END;$$;
--******************************--
----- nav_ppall_short.sql 

CREATE OR REPLACE FUNCTION cipsrv_tap.nav_ppall_short()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   
BEGIN
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'PPALL'
      ,p_start_nhdplusid            := 2788639
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 27.31578
      ,p_stop_nhdplusid             := 1897288
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := 13.76972
      ,p_max_distancekm             := NULL
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,135::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'PPALL'
      ,p_start_nhdplusid            := 50000800141148
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := 7.01946
      ,p_stop_nhdplusid             := 50000800142389
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := 63.64216
      ,p_max_distancekm             := NULL
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,242::INT
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
   );

END;$$;
