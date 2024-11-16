DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.overlay_measures';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.overlay_measures(
    IN  p_geometry1           GEOMETRY
   ,IN  p_geometry2           GEOMETRY
) RETURNS GEOMETRY 
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_input_start   GEOMETRY;
   sdo_input_end     GEOMETRY;
   num_start_meas    NUMERIC;
   num_end_meas      NUMERIC;
   sdo_lrs_output    GEOMETRY;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF ST_GeometryType(p_geometry1)  <> 'ST_LineString'
   THEN
      RAISE EXCEPTION 'geometry 1 must a single linestring';
      
   END IF;
   
   IF ST_GeometryType(p_geometry2) <> 'ST_LineString'
   OR ST_M(ST_StartPoint(p_geometry2)) IS NULL
   THEN
      RAISE EXCEPTION 'geometry 2 must be single LRS linestring';
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Collect the start and end points of the input geometry
   --------------------------------------------------------------------------
   sdo_input_start := ST_StartPoint(p_geometry1);
   sdo_input_end   := ST_EndPoint(p_geometry1);
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Collect the start and end measure of the input geometry on the lrs
   --------------------------------------------------------------------------
   num_start_meas := ST_InterpolatePoint(
       p_geometry2
      ,sdo_input_start
   );
      
   num_end_meas := ST_InterpolatePoint(
       p_geometry2
      ,sdo_input_end
   );
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Build the new LRS string from the measures
   --------------------------------------------------------------------------
   sdo_lrs_output := ST_AddMeasure(
       p_geometry1
      ,num_start_meas
      ,num_end_meas
   );
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Check to see if the geometry is backwards
   --------------------------------------------------------------------------
   IF num_start_meas < num_end_meas
   THEN
      sdo_lrs_output := cipsrv_engine.reverse_linestring(
          p_geometry := sdo_lrs_output
      );
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 60
   -- Return the results
   --------------------------------------------------------------------------
   RETURN sdo_lrs_output;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.overlay_measures(
    GEOMETRY
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.overlay_measures(
    GEOMETRY
   ,GEOMETRY
) TO PUBLIC;
