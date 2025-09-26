CREATE OR REPLACE FUNCTION cipsrv_tap.nav_ppall_short()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   int_stop_nhdplusid  BIGINT;
   num_stop_measure    NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 2788639;
   num_start_measure   := 27.31578;
   int_stop_nhdplusid  := 1897288;
   num_stop_measure    := 13.76972;
   
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'PPALL'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := int_stop_nhdplusid
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := num_stop_measure
      ,p_max_distancekm             := NULL
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('MR PPALL ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,int_stop_nhdplusid,num_stop_measure],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,135::INT
      ,'test 1.2 - flowline count'
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 50000800141148;
   num_start_measure   := 7.01946;
   int_stop_nhdplusid  := 50000800142389;
   num_stop_measure    := 63.64216;
   
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'PPALL'
      ,p_start_nhdplusid            := int_start_nhdplusid
      ,p_start_permanent_identifier := NULL
      ,p_start_reachcode            := NULL
      ,p_start_hydroseq             := NULL
      ,p_start_measure              := num_start_measure
      ,p_stop_nhdplusid             := int_stop_nhdplusid
      ,p_stop_permanent_identifier  := NULL
      ,p_stop_reachcode             := NULL
      ,p_stop_hydroseq              := NULL
      ,p_stop_measure               := num_stop_measure
      ,p_max_distancekm             := NULL
      ,p_max_flowtimeday            := NULL
      ,p_return_flowline_details    := TRUE
      ,p_return_flowline_geometry   := TRUE
   );
   
   RETURN NEXT tap.diag('HR PPALL ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,int_stop_nhdplusid,num_stop_measure],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 2.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,242::INT
      ,'test 2.2 - flowline count'
   );

END;$$;
