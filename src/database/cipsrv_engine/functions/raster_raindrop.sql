DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.raster_raindrop';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.raster_raindrop(
    IN  p_raster           RASTER
   ,IN  p_columnX          INTEGER
   ,IN  p_rowY             INTEGER
) RETURNS GEOMETRY
IMMUTABLE
AS
$BODY$
DECLARE
   r             RECORD;
   sdo_output    GEOMETRY;
   sdo_temp      GEOMETRY;
   int_column_x  INTEGER := p_columnX;
   int_row_y     INTEGER := p_rowY;
   int_stop      INTEGER;
   int_value     INTEGER;
   int_width     INTEGER;
   int_height    INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_columnX IS NULL
   OR p_rowY    IS NULL
   OR p_raster  IS NULL
   THEN
      RAISE EXCEPTION 'err';
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- Get the raster size
   ----------------------------------------------------------------------------
   int_width   := ST_Width(p_raster);
   int_height  := ST_Height(p_raster);
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Test the start point on the grid
   ----------------------------------------------------------------------------
   sdo_temp := ST_PixelAsCentroid(
       p_raster
      ,int_column_x
      ,int_row_y
   );
   --RAISE WARNING '% % %', int_column_x,int_row_y,ST_AsEWKT(ST_Transform(sdo_temp,4269));
   
   IF sdo_temp IS NULL
   THEN
      RAISE EXCEPTION 'unable to obtain coordinates from raster with X and Y provided';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Walk the grid downstream
   ----------------------------------------------------------------------------
   int_stop := 10000;
   WHILE int_stop > 0
   LOOP
      sdo_temp := ST_PixelAsCentroid(
          p_raster
         ,int_column_x
         ,int_row_y
      );
      
      int_value := ST_Value(
          rast     := p_raster
         ,band     := 1
         ,x        := int_column_x
         ,y        := int_row_y
         ,exclude_nodata_value := true
      );
      
      CASE int_value
      WHEN 1
      THEN
         int_column_x := int_column_x + 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 2
      THEN
         int_column_x := int_column_x + 1;
         int_row_y    := int_row_y    + 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 4
      THEN
         int_row_y    := int_row_y    + 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 8
      THEN
         int_column_x := int_column_x - 1;
         int_row_y    := int_row_y    + 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 16
      THEN
         int_column_x := int_column_x - 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 32
      THEN
         int_column_x := int_column_x - 1;
         int_row_y    := int_row_y    - 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 64
      THEN
         int_row_y    := int_row_y    - 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 128
      THEN
         int_column_x := int_column_x + 1;
         int_row_y    := int_row_y    - 1;
         
         IF sdo_output IS NULL
         THEN
            sdo_output := sdo_temp;

         ELSE
            sdo_output := ST_MakeLine(
                sdo_output
               ,sdo_temp
            );

         END IF;
         
      WHEN 0
      THEN
         int_stop := 0;
         
      WHEN 255
      THEN
         int_stop := 0;
         
      ELSE
         int_stop := 0;
         
      END CASE;

      IF int_column_x < 1
      OR int_row_y < 1
      OR int_column_x > int_width
      OR int_row_y > int_height
      THEN
         int_stop := 0;
         
      END IF;
      
      int_stop := int_stop - 1;
   
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN sdo_output;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.raster_raindrop(
    RASTER
   ,INTEGER
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.raster_raindrop(
    RASTER
   ,INTEGER
   ,INTEGER
) TO PUBLIC;

