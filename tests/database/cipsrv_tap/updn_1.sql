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
