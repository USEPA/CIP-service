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
      ,'test 6.1 - return_code'
   );
   
   int_tmp := 21001300058689;
   RETURN NEXT tap.is(
       rec.out_nhdplusid
      ,int_tmp
      ,'test 6.2 - nhdplusid ' || rec.out_nhdplusid || ' = ' || int_tmp
   );

END;$$;
