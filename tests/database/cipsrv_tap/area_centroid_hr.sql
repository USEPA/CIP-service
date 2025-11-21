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
