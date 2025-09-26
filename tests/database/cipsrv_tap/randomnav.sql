CREATE OR REPLACE FUNCTION cipsrv_tap.randomnav()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   int_tmp       BIGINT;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- MR 
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := NULL
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := '5070'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 2.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := '26904'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := '32161'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 4.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := '32655'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomnav(
       p_region           := '32702'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 6.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- HR 
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := NULL
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 7.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := '5070'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 8.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := '26904'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 9.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := '32161'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 10.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := '32655'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 11.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randomnav(
       p_region           := '32702'
      ,p_return_geometry  := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 12.1 - return_code'
   );

END;$$;
