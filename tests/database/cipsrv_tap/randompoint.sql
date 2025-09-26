CREATE OR REPLACE FUNCTION cipsrv_tap.randompoint()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec           RECORD;
   int_tmp       BIGINT;
   
BEGIN
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   -- MR
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := NULL
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 1.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '5070'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 2.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '26904'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 3.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32161'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 4.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32655'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 5.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32702'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 6.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   -- HR
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := NULL
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 7.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   -- AK placeholder
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '5070'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 8.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '26904'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 9.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32161'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 10.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32655'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 11.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32702'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 12.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   -- Extended MR
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := NULL
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 13.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '3338'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 14.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '5070'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 15.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '26904'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 16.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32161'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 17.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32655'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 18.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randompoint(
       p_region           := '32702'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 19.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := NULL
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 20.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '3338'
      ,p_include_extended := TRUE
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 21.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '5070'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 22.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '26904'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 23.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32161'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 24.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32655'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 25.1 - return_code'
   );
   
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.randompoint(
       p_region           := '32702'
      ,p_include_extended := NULL
   );
   
   RETURN NEXT tap.is(
       rec.out_return_code
      ,0
      ,'test 26.1 - return_code'
   );

END;$$;
