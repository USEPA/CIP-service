CREATE OR REPLACE FUNCTION cipsrv_tap.randomhuc12()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   int_tmp       BIGINT;
   
BEGIN
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := NULL
      ,p_source_dataset  := NULL
      ,p_return_geometry := FALSE
      ,p_known_huc12     := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := NULL
      ,p_source_dataset  := 'NP21'
      ,p_return_geometry := TRUE
      ,p_known_huc12     := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 2.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := NULL
      ,p_source_dataset  := 'NPHR'
      ,p_return_geometry := TRUE
      ,p_known_huc12     := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := NULL
      ,p_source_dataset  := 'F3'
      ,p_return_geometry := TRUE
      ,p_known_huc12     := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 4.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := NULL
      ,p_source_dataset  := 'F3'
      ,p_return_geometry := TRUE
      ,p_known_huc12     := '111401050705'
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code'
   );
   
   RETURN NEXT tap.is(
       rec.out_huc12
      ,'111401050705'
      ,'test 5.2 - huc12 ' || rec.out_huc12
   );
   
   RETURN NEXT tap.is(
       rec.out_huc12_name
      ,'Lower Frazier Creek'
      ,'test 5.3 - huc12 ' || rec.out_huc12_name
   );
   

END;$$;
