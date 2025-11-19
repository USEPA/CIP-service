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
