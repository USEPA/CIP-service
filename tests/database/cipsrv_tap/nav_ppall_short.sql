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
