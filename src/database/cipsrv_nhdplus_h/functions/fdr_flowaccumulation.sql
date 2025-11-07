CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.fdr_flowaccumulation(
    p_area_of_interest     IN  GEOMETRY
   ,p_default_weight       IN  INTEGER DEFAULT 1
   ,p_known_region         IN  VARCHAR DEFAULT NULL
   ,out_flow_accumulation  OUT RASTER
   ,out_max_accumulation   OUT INTEGER
   ,out_max_accumulation_x OUT INTEGER
   ,out_max_accumulation_y OUT INTEGER
   ,out_return_code        OUT INTEGER
   ,out_status_message     OUT VARCHAR
)
STABLE
AS
$BODY$
DECLARE
   rec                 RECORD;
   int_depth_charge    INTEGER := 100000;
   int_default_weight  INTEGER := p_default_weight;
   int_raster_srid     INTEGER;
   int_columns         INTEGER;
   int_rows            INTEGER;
   rast_fdr            RASTER;
   int_depth           INTEGER;
   int_nidp_value      INTEGER;
   int_fdr_value       INTEGER;
   int_accu_value      INTEGER;
   int_flow_count      INTEGER;
   int_running_total   INTEGER;
   boo_continue        BOOLEAN;
   int_working_x       INTEGER;
   int_working_y       INTEGER;
   int_index           INTEGER;
   mat_fdr             INTEGER[][];
   mat_nidp            INTEGER[][];
   mat_accum           INTEGER[][];

BEGIN

   -- Zhou, G., Dong, W., and Wei, H.
   -- A fast and simple algorithm for calculating flow accumulation matrices 
   -- from raster digital elevation models
   -- Abstr. Int. Cartogr. Assoc., 
   
   out_return_code := 0;

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   out_max_accumulation := 0;

   IF p_area_of_interest IS NULL
   THEN
      RAISE EXCEPTION 'Input area of interest is null';

   END IF;

   IF int_default_weight IS NULL
   OR int_default_weight = 0
   THEN
      int_default_weight := 1;

   END IF;

   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the grid projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry     := p_area_of_interest
      ,p_known_region := p_known_region
   );
   int_raster_srid    := rec.out_srid;

   IF rec.out_return_code <> 0
   THEN
      RAISE EXCEPTION '%: %', rec.out_return_code,rec.out_status_message;

   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- Pull the FDR grid for the provided AOI
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.fetch_grids_by_geometry(
       p_FDR_input       := p_area_of_interest
      ,p_FAC_input       := NULL
      ,p_known_region    := int_raster_srid::VARCHAR
      ,p_FDR_nodata      := 255
      ,p_FAC_nodata      := NULL
      ,p_crop            := TRUE
   );
   rast_fdr := rec.out_FDR;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Create the nidp and results rasters
   ----------------------------------------------------------------------------
   int_columns := public.ST_Width(rast_fdr);
   int_rows    := public.ST_Height(rast_fdr);

   out_flow_accumulation := public.ST_MakeEmptyRaster(
      rast := rast_fdr
   );

   out_flow_accumulation := public.ST_AddBand(
       rast         := out_flow_accumulation
      ,index        := 1
      ,pixeltype    := '32BSI'::TEXT
      ,initialvalue := -2
      ,nodataval    := -1
   );

   ----------------------------------------------------------------------------
   -- Step 50
   -- Generate the nidp raster set and carve original nodata into output raster
   ----------------------------------------------------------------------------
   mat_fdr  := public.ST_DumpValues(
       rast    := rast_fdr
      ,nband   := 1
      ,exclude_nodata_value := FALSE
   );

   mat_nidp := public.ST_DumpValues(
       rast    := out_flow_accumulation
      ,nband   := 1
      ,exclude_nodata_value := FALSE
   );

   mat_accum := public.ST_DumpValues(
       rast    := out_flow_accumulation
      ,nband   := 1
      ,exclude_nodata_value := FALSE
   );

   FOR i IN 1 .. int_columns
   LOOP
      FOR j IN 1 .. int_rows
      LOOP
         int_flow_count := 0;
         
         IF mat_fdr[j][i] = 255
         THEN
            int_flow_count := -1;
            
         ELSE
            -- [1,1]
            IF j > 1 AND i > 1
            THEN
               IF mat_fdr[j-1][i-1] = 2
               THEN
                  int_flow_count := int_flow_count + 1;

               END IF;
               
            END IF;
            
            -- [1,2]
            IF j > 1
            THEN
               IF mat_fdr[j-1][i] = 4
               THEN
                  int_flow_count := int_flow_count + 1;

               END IF;
               
            END IF;
            
            -- [1,3]
            IF j > 1 AND i < int_columns
            THEN
               IF mat_fdr[j-1][i+1] = 8
               THEN
                  int_flow_count := int_flow_count + 1;
                 
               END IF;
               
            END IF;

            -- [2,1]
            IF i > 1
            THEN
               IF mat_fdr[j][i-1] = 1
               THEN
                  int_flow_count := int_flow_count + 1;

               END IF;
               
            END IF;
            
            -- [2,3]
            IF i < int_columns
            THEN
               IF mat_fdr[j][i+1] = 16
               THEN
                  int_flow_count := int_flow_count + 1;

               END IF;
               
            END IF;
            
            -- [3,1]
            IF j < int_rows AND i > 1
            THEN
               IF mat_fdr[j+1][i-1] = 128
               THEN
                  int_flow_count := int_flow_count + 1;
                  
               END IF;
               
            END IF;

            -- [3,2]
            IF j < int_rows
            THEN
               IF mat_fdr[j+1][i] = 64
               THEN
                  int_flow_count := int_flow_count + 1;

               END IF;
               
            END IF;
            
            -- [3,3]
            IF j < int_rows AND i < int_columns
            THEN
               IF mat_fdr[j+1][i+1] = 32
               THEN
                  int_flow_count := int_flow_count + 1;
                  
               END IF;
               
            END IF;
            
         END IF;
         
         mat_nidp[j][i] := int_flow_count;

         IF int_flow_count = -1
         THEN
            mat_accum[j][i] := -1;

         ELSE
            mat_accum[j][i] := 1;

         END IF;

      END LOOP;

   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Search for source cells and run downstream to first intersection cell
   ----------------------------------------------------------------------------
   FOR i IN 1 .. int_columns
   LOOP
      FOR j IN 1 .. int_rows
      LOOP
         int_nidp_value := mat_nidp[j][i];

         --....................................................................
         IF int_nidp_value = 0
         THEN
            --raise warning 'source found at %, %',i,j;
            int_running_total := 0;
            int_working_x := i;
            int_working_y := j;

            int_depth := 1;
            boo_continue := TRUE;

            <<outer_loop>>
            WHILE boo_continue
            LOOP
               int_depth := int_depth + 1;
               IF int_depth > int_depth_charge
               THEN
                  RAISE EXCEPTION 'depth charge';

               END IF;

               --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
               int_fdr_value := mat_fdr[int_working_y][int_working_x];
               int_accu_value := mat_accum[int_working_y][int_working_x];

               --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
               int_running_total := int_running_total + int_accu_value;
               mat_accum[int_working_y][int_working_x] := int_running_total;
               
               IF int_running_total > out_max_accumulation
               THEN
                  out_max_accumulation   := int_running_total;
                  out_max_accumulation_x := int_working_x;
                  out_max_accumulation_y := int_working_y;
                  
               END IF;

               --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
               int_nidp_value := mat_nidp[int_working_y][int_working_x];

               IF int_nidp_value > 1
               THEN
                  mat_nidp[int_working_y][int_working_x] := int_nidp_value - 1;

               END IF;

               --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
               CASE int_fdr_value
               WHEN 1
               THEN
                  int_working_x := int_working_x + 1;
                  int_working_y := int_working_y;

               WHEN 2
               THEN
                  int_working_x := int_working_x + 1;
                  int_working_y := int_working_y + 1;

               WHEN 4
               THEN
                  int_working_x := int_working_x;
                  int_working_y := int_working_y + 1;

               WHEN 8
               THEN
                  int_working_x := int_working_x - 1;
                  int_working_y := int_working_y + 1;

               WHEN 16
               THEN
                  int_working_x := int_working_x - 1;
                  int_working_y := int_working_y;
   
               WHEN 32
               THEN
                  int_working_x := int_working_x - 1;
                  int_working_y := int_working_y - 1;

               WHEN 64
               THEN
                  int_working_x := int_working_x;
                  int_working_y := int_working_y - 1;

               WHEN 128
               THEN
                  int_working_x := int_working_x + 1;
                  int_working_y := int_working_y - 1;

               WHEN 0
               THEN
                  -- end of the line sink
                  boo_continue := FALSE;

               ELSE
                  --RAISE WARNING 'exiting grid for % at %, %',int_fdr_value,int_working_x,int_working_y;
                  boo_continue := FALSE;

               END CASE;

               --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
               IF int_nidp_value > 1
               THEN
                  --raise warning 'intersection found at %, %',int_working_x,int_working_y;
                  boo_continue := FALSE;

               ELSIF int_working_x < 1 OR int_working_x > int_columns
               OR    int_working_y < 1 OR int_working_y > int_rows
               THEN
                  raise warning 'exiting grid at %, %',int_working_x,int_working_y;
                  boo_continue := FALSE;
               
               END IF;

            END LOOP;

         END IF;

      END LOOP;

   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Return the matrix as raster
   ----------------------------------------------------------------------------
   out_flow_accumulation := public.ST_SetValues(
       rast    := out_flow_accumulation
      ,nband   := 1
      ,x       := 1
      ,y       := 1
      ,newvalueset := mat_accum
   );

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.fdr_flowaccumulation';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;

