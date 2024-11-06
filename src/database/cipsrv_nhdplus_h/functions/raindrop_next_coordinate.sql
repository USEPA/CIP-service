DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.raindrop_next_coordinate';
   IF b IS NOT NULL THEN
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.raindrop_next_coordinate(
    IN    p_raster                 RASTER
   ,IN    p_grid_size_km           NUMERIC
   ,IN    p_offset_x               INTEGER
   ,IN    p_offset_y               INTEGER
   ,INOUT inout_column_x           INTEGER
   ,INOUT inout_row_y              INTEGER
   ,OUT   out_distance_km          NUMERIC
   ,OUT   out_direction            INTEGER
   ,OUT   out_return_code          INTEGER
   ,OUT   out_status_message       VARCHAR
) 
IMMUTABLE
AS $BODY$ 
DECLARE
   num_corner_km NUMERIC;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   num_corner_km := p_grid_size_km * SQRT(2);

   --------------------------------------------------------------------------
   -- Step 20
   -- Get the raster value
   --------------------------------------------------------------------------
   out_direction := cipsrv_nhdplus_h.raindrop_st_value(
       p_raster    := p_raster
      ,p_column_x  := inout_column_x
      ,p_row_y     := inout_row_y
      ,p_offset_x  := p_offset_x
      ,p_offset_y  := p_offset_y
   );

   --------------------------------------------------------------------------
   -- Step 30
   -- Walk the grid
   --------------------------------------------------------------------------
   CASE out_direction
   WHEN 1
   THEN
      inout_column_x  := inout_column_x + 1;
      out_distance_km := p_grid_size_km;
      
   WHEN 2
   THEN
      inout_column_x  := inout_column_x + 1;
      inout_row_y     := inout_row_y    + 1;
      out_distance_km := num_corner_km;
      
   WHEN 4
   THEN
      inout_row_y     := inout_row_y    + 1;
      out_distance_km := p_grid_size_km;
      
   WHEN 8
   THEN
      inout_column_x  := inout_column_x - 1;
      inout_row_y     := inout_row_y    + 1;
      out_distance_km := num_corner_km;
      
   WHEN 16
   THEN
      inout_column_x  := inout_column_x - 1;
      out_distance_km := p_grid_size_km;
      
   WHEN 32
   THEN
      inout_column_x  := inout_column_x - 1;
      inout_row_y     := inout_row_y    - 1;
      out_distance_km := num_corner_km;
      
   WHEN 64
   THEN
      inout_row_y     := inout_row_y    - 1;
      out_distance_km := p_grid_size_km;
      
   WHEN 128
   THEN
      inout_column_x  := inout_column_x + 1;
      inout_row_y     := inout_row_y    - 1;
      out_distance_km := num_corner_km;
      
   WHEN 0
   THEN
      out_return_code    := -20011;
      out_status_message := 'Flow ends at sink';
      RETURN;
      
   WHEN 255
   THEN
      out_return_code    := -20010;
      out_status_message := 'Flow Direction Grid Out Of Bounds';
      RETURN;
      
   ELSE
      out_return_code    := -20010;
      out_status_message := 'Flow Direction Grid Out Of Bounds';
      RETURN;
      
   END CASE;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.raindrop_next_coordinate(
    RASTER
   ,NUMERIC
   ,INTEGER
   ,INTEGER
   ,INTEGER
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.raindrop_next_coordinate(
    RASTER
   ,NUMERIC
   ,INTEGER
   ,INTEGER
   ,INTEGER
   ,INTEGER
) TO PUBLIC;
