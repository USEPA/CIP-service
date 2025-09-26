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
      ,'test 1.1 - return_code'
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
      ,'test 2.1 - return_code'
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
      ,'test 3.1 - return_code'
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
      ,'test 4.1 - return_code'
   );

   int_tmp := 225;
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
      ,'test 5.1 - return_code'
   );

   int_tmp := 225;
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
      ,'test 6.1 - return_code'
   );

   int_tmp := 225;
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,int_tmp
      ,'test 6.2 flowline count ' || rec.out_flowline_count || ' = ' || int_tmp
   );

END;$$;
