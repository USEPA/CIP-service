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
