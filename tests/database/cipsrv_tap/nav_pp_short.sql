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
