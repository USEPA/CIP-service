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
