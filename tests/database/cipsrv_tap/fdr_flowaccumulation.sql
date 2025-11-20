CREATE OR REPLACE FUNCTION cipsrv_tap.fdr_flowaccumulation()
RETURNS SETOF TEXT 
LANGUAGE plpgsql
AS $$DECLARE
   rec                 RECORD;
   sdo_input_geom      GEOMETRY;
   
BEGIN
   
   ----------------------------------------------------------------------------
   sdo_input_geom      := ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[-113.431091,33.440466],[-113.441906,33.44505],[-113.437786,33.431441],[-113.443794,33.38831],[-113.436928,33.379136],[-113.427658,33.38487],[-113.424911,33.437458],[-113.431091,33.440466]]]}');
   
   rec := cipsrv_nhdplus_m.fdr_flowaccumulation(
       p_area_of_interest      := sdo_input_geom
   );
   
   RETURN NEXT tap.diag('MR flow accumulation ');
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 1.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_max_accumulation
      ,8321
      ,'test 1.1 - max accumulation'
   );
   
   ----------------------------------------------------------------------------
   sdo_input_geom      := ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[-113.431091,33.440466],[-113.441906,33.44505],[-113.437786,33.431441],[-113.443794,33.38831],[-113.436928,33.379136],[-113.427658,33.38487],[-113.424911,33.437458],[-113.431091,33.440466]]]}');
   
   rec := cipsrv_nhdplus_h.fdr_flowaccumulation(
       p_area_of_interest      := sdo_input_geom
   );
   
   RETURN NEXT tap.diag('HR flow accumulation ');
   
   RETURN NEXT tap.is(
       rec.out_return_code::INT
      ,0::INT
      ,'test 3.1 - return_code'
   );

   RETURN NEXT tap.is(
       rec.out_max_accumulation
      ,73517
      ,'test 3.1 - max accumulation'
   );
   
END;$$;
