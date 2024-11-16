DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.fdr_upstream_norecursion';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.fdr_upstream_norecursion(
    IN p_column_x             INTEGER
   ,IN p_row_y                INTEGER
   ,IN OUT iout_rast          RASTER
   ,OUT out_depth             INTEGER 
)
IMMUTABLE
AS $BODY$
DECLARE
   boo_continue     BOOLEAN;
   int_depth_charge INTEGER := 100000;
   mat_values       INTEGER[][];
   int_working_x    INTEGER[];
   int_working_y    INTEGER[];
   int_increment_x  INTEGER[];
   int_increment_y  INTEGER[];
   int_index        INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF iout_rast IS NULL
   THEN
      RAISE EXCEPTION 'Input raster is null';
      
   END IF;
   --raise warning 'raster % by %', st_height(iout_rast), st_width(iout_rast);
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Seed the start location
   ----------------------------------------------------------------------------
   int_working_x[1] := p_column_x;
   int_working_y[1] := p_row_y;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Set up outer loop to allow starting over and exit condition
   ----------------------------------------------------------------------------
   out_depth := 1;
   boo_continue := TRUE;
   <<outer_loop>>
   WHILE boo_continue
   LOOP
      
   ----------------------------------------------------------------------------
   -- Step 40
   -- Abend if stuck in ceaseless loop
   ----------------------------------------------------------------------------
      out_depth := out_depth + 1;
      IF out_depth > int_depth_charge
      THEN
         RAISE EXCEPTION 'depth charge';
         
      END IF;
      
   ----------------------------------------------------------------------------
   -- Step 50
   -- Initialize increment queue
   ----------------------------------------------------------------------------
      int_index       := 1;
      int_increment_x := NULL;
      int_increment_y := NULL;
      
   ----------------------------------------------------------------------------
   -- Step 60
   -- Loop through the working stack
   ----------------------------------------------------------------------------
      --raise warning '%', array_length(int_working_x,1);
      FOR i IN 1 .. array_length(int_working_x,1)
      LOOP
         
   ----------------------------------------------------------------------------
   -- Step 70
   -- Pull 3x3 grid
   ----------------------------------------------------------------------------   
         mat_values := ST_Neighborhood(
             iout_rast
            ,1
            ,int_working_x[i]
            ,int_working_y[i]
            ,1
            ,1
         );
         --raise warning '%', mat_values;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Set center grid to be 99
   ----------------------------------------------------------------------------
         IF mat_values[2][2] <> 99
         THEN
            
            iout_rast := ST_SetValue(
                iout_rast
               ,int_working_x[i]
               ,int_working_y[i]
               ,99
            );
            
   ----------------------------------------------------------------------------
   -- Step 90
   -- Examine surrounding cells to define the upstream area
   ----------------------------------------------------------------------------         
            IF mat_values[1][1] = 2
            THEN
               int_increment_x[int_index] := int_working_x[i] - 1;
               int_increment_y[int_index] := int_working_y[i] - 1;
               int_index := int_index + 1;

            END IF;

            IF mat_values[1][2] = 4
            THEN
               int_increment_x[int_index] := int_working_x[i];
               int_increment_y[int_index] := int_working_y[i] - 1;
               int_index := int_index + 1;

            END IF;

            IF mat_values[1][3] = 8
            THEN
               int_increment_x[int_index] := int_working_x[i] + 1;
               int_increment_y[int_index] := int_working_y[i] - 1;
               int_index := int_index + 1;
              
            END IF;

            IF mat_values[2][1] = 1
            THEN
               int_increment_x[int_index] := int_working_x[i] - 1;
               int_increment_y[int_index] := int_working_y[i];
               int_index := int_index + 1;

            END IF;

            IF mat_values[2][3] = 16
            THEN
               int_increment_x[int_index] := int_working_x[i] + 1;
               int_increment_y[int_index] := int_working_y[i];
               int_index := int_index + 1;

            END IF;

            IF mat_values[3][1] = 128
            THEN
               int_increment_x[int_index] := int_working_x[i] - 1;
               int_increment_y[int_index] := int_working_y[i] + 1;
               int_index := int_index + 1;
               
            END IF;

            IF mat_values[3][2] = 64
            THEN
               int_increment_x[int_index] := int_working_x[i];
               int_increment_y[int_index] := int_working_y[i] + 1;
               int_index := int_index + 1;

            END IF;

            IF mat_values[3][3] = 32
            THEN
               int_increment_x[int_index] := int_working_x[i] + 1;
               int_increment_y[int_index] := int_working_y[i] + 1;
               int_index := int_index + 1;
               
            END IF;

         END IF;

      END LOOP;
      
   ----------------------------------------------------------------------------
   -- Step 100
   -- Continue until done
   ----------------------------------------------------------------------------
      IF int_increment_x IS NULL
      OR array_length(int_increment_x,1) = 0
      THEN
         boo_continue := FALSE;
         
      END IF;
      
      int_working_x := int_increment_x;
      int_working_y := int_increment_y;
      
   ----------------------------------------------------------------------------
   -- Step 110
   -- Continue until done
   ----------------------------------------------------------------------------
   END LOOP outer_loop;

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.fdr_upstream_norecursion(
    INTEGER
   ,INTEGER
   ,RASTER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.fdr_upstream_norecursion(
    INTEGER
   ,INTEGER
   ,RASTER
) TO PUBLIC;
