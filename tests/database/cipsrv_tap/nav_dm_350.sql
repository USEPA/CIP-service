CREATE OR REPLACE FUNCTION cipsrv_tap.nav_dm_350()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   int_start_nhdplusid BIGINT;
   num_start_measure   NUMERIC;
   num_max_distancekm  NUMERIC;
   
BEGIN
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 17819112;
   num_start_measure   := 14.0492;
   num_max_distancekm  := 350;
   
   rec := cipsrv_nhdplus_m.navigate(
       p_search_type                := 'DM'
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
   
   RETURN NEXT tap.diag('MR DM ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );
   
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,224::INT
      ,'test 1.2 - flowline_count'
   );
   
   ----------------------------------------------------------------------------
   int_start_nhdplusid := 35000600173658;
   num_start_measure   := 67.56459;
   num_max_distancekm  := 350;
   
   rec := cipsrv_nhdplus_h.navigate(
       p_search_type                := 'DM'
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
   
   RETURN NEXT tap.diag('HR DM ' || ARRAY_TO_STRING(ARRAY[int_start_nhdplusid,num_start_measure,num_max_distancekm],','));
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 2.1 - return_code'
   );
   
   RETURN NEXT tap.is(
       rec.out_flowline_count::INT
      ,441::INT
      ,'test 2.1 - flowline_count'
   );

END;$$;
