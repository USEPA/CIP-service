DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.deepest_cell';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.deepest_cell(
    IN  p_input               GEOMETRY
   ,IN  p_FDR                 RASTER
   ,IN  p_FAC                 RASTER 
   ,OUT out_columnX           INTEGER
   ,OUT out_rowY              INTEGER
)
IMMUTABLE
AS
$BODY$ 
DECLARE
   rec               RECORD;
   sdo_input         GEOMETRY := p_input;
   int_column_x_fdr  INTEGER;
   int_row_y_fdr     INTEGER;
   int_column_x_fac  INTEGER;
   int_row_y_fac     INTEGER;
   int_orig_column_x INTEGER;
   int_orig_row_y    INTEGER;
   mat_values_fdr    INTEGER[][];
   mat_values_fac    INTEGER[][];
   int_largest_accum INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF sdo_input IS NULL
   THEN
      RAISE EXCEPTION 'input point cannot by null';
      
   END IF;
   
   IF p_FDR IS NULL
   OR p_FAC IS NULL
   THEN
      RAISE EXCEPTION 'FDR and FAC rasters required';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Force inputs to match
   ----------------------------------------------------------------------------
   IF ST_SRID(sdo_input) <> ST_SRID(p_FDR)
   THEN
      sdo_input := ST_Transform(sdo_input,ST_SRID(p_FDR));
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Get initial start columnx and rowy from raster fdr
   ----------------------------------------------------------------------------
   rec := ST_WorldToRasterCoord(
       p_FDR
      ,sdo_input
   );
   int_column_x_fdr := rec.columnx;
   int_row_y_fdr    := rec.rowy;
   --RAISE WARNING '% %', int_column_x_fdr, int_row_y_fdr;
   --RAISE WARNING '%',st_astext(st_transform(ST_PixelAsCentroid(p_FDR,int_column_x,int_row_y),4269));

   ----------------------------------------------------------------------------
   -- Step 40
   -- Get initial start columnx and rowy from raster fac
   ----------------------------------------------------------------------------
   rec := ST_WorldToRasterCoord(
       p_FAC
      ,sdo_input
   );
   int_column_x_fac := rec.columnx;
   int_row_y_fac    := rec.rowy;
   --RAISE WARNING '% %', int_column_x_fac, int_row_y_fac;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Pull the fdr 9 cell matrix around this top
   --------------------------------------------------------------------------
   mat_values_fdr := ST_Neighborhood(
       p_FDR
      ,1
      ,int_column_x_fdr
      ,int_row_y_fdr
      ,1
      ,1
   );
   --RAISE WARNING '%', mat_values_fdr;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Pull the fac 9 cell matrix around this top
   --------------------------------------------------------------------------
   mat_values_fac := ST_Neighborhood(
       p_FAC
      ,1
      ,int_column_x_fac
      ,int_row_y_fac
      ,1
      ,1
   );
   --RAISE WARNING '%', mat_values_fac;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Decide whether to move over into neighboring cell that is deeper
   --------------------------------------------------------------------------
   int_orig_column_x := int_column_x_fdr;
   int_orig_row_y    := int_row_y_fdr;
   int_largest_accum := mat_values_fac[2][2];
   
   -- 1,1
   IF  mat_values_fac[1][1] > int_largest_accum
   AND mat_values_fdr[2][2] <> 32
   THEN
      IF mat_values_fdr[2][1] <> 64
      AND mat_values_fdr[1][2] <> 16
      THEN
         int_largest_accum := mat_values_fac[1][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[2][1] = 64
      AND   mat_values_fac[2][1] + 7 < mat_values_fac[1][1]
      THEN
         int_largest_accum := mat_values_fac[1][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[1][2] = 16
      AND   mat_values_fac[1][2] + 7 < mat_values_fac[1][1]
      THEN
         int_largest_accum := mat_values_fac[1][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y - 1;
         
      END IF;
      
   END IF;
   
   -- 1,2
   IF  mat_values_fac[1][2] > int_largest_accum
   AND mat_values_fdr[2][2] <> 64
   THEN
      IF mat_values_fdr[1][1] <> 1
      AND mat_values_fdr[1][3] <> 16
      THEN
         int_largest_accum := mat_values_fac[1][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[1][1] = 1
      AND   mat_values_fac[1][1] + 7 < mat_values_fac[1][2]
      THEN
         int_largest_accum := mat_values_fac[1][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[1][3] = 16
      AND   mat_values_fac[1][3] + 7 < mat_values_fac[1][2]
      THEN
         int_largest_accum := mat_values_fac[1][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y - 1;
         
      END IF;
      
   END IF;
   
   -- 1,3
   IF  mat_values_fac[1][3] > int_largest_accum
   AND mat_values_fdr[2][2] <> 128
   THEN
      IF mat_values_fdr[1][2] <> 1
      AND mat_values_fdr[2][3] <> 64
      THEN
         int_largest_accum := mat_values_fac[1][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[1][2] = 1
      AND   mat_values_fac[1][2] + 7 < mat_values_fac[1][3]
      THEN
         int_largest_accum := mat_values_fac[1][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y - 1;
      
      ELSIF mat_values_fdr[2][3] = 64
      AND   mat_values_fac[2][3] + 7 < mat_values_fac[1][3]
      THEN
         int_largest_accum := mat_values_fac[1][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y - 1;
         
      END IF;
      
   END IF;
   
   -- 2,1
   IF  mat_values_fac[2][1] > int_largest_accum
   AND mat_values_fdr[2][2] <> 16
   THEN
      IF mat_values_fdr[1][1] <> 4
      AND mat_values_fdr[3][1] <> 64
      THEN
         int_largest_accum := mat_values_fac[2][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y;
      
      ELSIF mat_values_fdr[1][1] = 4
      AND   mat_values_fac[1][1] + 7 < mat_values_fac[2][1]
      THEN
         int_largest_accum := mat_values_fac[2][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y;
      
      ELSIF mat_values_fdr[3][1] = 64
      AND   mat_values_fac[3][1] + 7 < mat_values_fac[2][1]
      THEN
         int_largest_accum := mat_values_fac[2][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y;
         
      END IF;
      
   END IF;
   
   -- 2,3
   IF  mat_values_fac[2][3] > int_largest_accum
   AND mat_values_fdr[2][2] <> 1
   THEN
      IF mat_values_fdr[1][3] <> 4
      AND mat_values_fdr[3][3] <> 64
      THEN
         int_largest_accum := mat_values_fac[2][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y;
      
      ELSIF mat_values_fdr[1][3] = 4
      AND   mat_values_fac[1][3] + 7 < mat_values_fac[2][3]
      THEN
         int_largest_accum := mat_values_fac[2][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y;
      
      ELSIF mat_values_fdr[3][3] = 64
      AND   mat_values_fac[3][3] + 7 < mat_values_fac[2][3]
      THEN
         int_largest_accum := mat_values_fac[2][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y;
         
      END IF;
      
   END IF;
   
   -- 3,1
   IF  mat_values_fac[3][1] > int_largest_accum
   AND mat_values_fdr[2][2] <> 8
   THEN
      IF mat_values_fdr[2][1] <> 4
      AND mat_values_fdr[3][2] <> 16
      THEN
         int_largest_accum := mat_values_fac[3][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[2][1] = 4
      AND   mat_values_fac[2][1] + 7 < mat_values_fac[3][1]
      THEN
         int_largest_accum := mat_values_fac[3][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[3][2] = 16
      AND   mat_values_fac[3][2] + 7 < mat_values_fac[3][1]
      THEN
         int_largest_accum := mat_values_fac[3][1];
         int_column_x_fdr  := int_orig_column_x - 1;
         int_row_y_fdr     := int_orig_row_y + 1;
         
      END IF;
      
   END IF;
   
   -- 3,2
   IF  mat_values_fac[3][2] > int_largest_accum
   AND mat_values_fdr[2][2] <> 4
   THEN
      IF  mat_values_fdr[3][1] <> 1
      AND mat_values_fdr[3][3] <> 16
      THEN
         int_largest_accum := mat_values_fac[3][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[3][1] = 1
      AND   mat_values_fac[3][1] + 7 < mat_values_fac[3][2]
      THEN
         int_largest_accum := mat_values_fac[3][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[3][3] = 16
      AND   mat_values_fac[3][3] + 7 < mat_values_fac[3][2]
      THEN
         int_largest_accum := mat_values_fac[3][2];
         int_column_x_fdr  := int_orig_column_x;
         int_row_y_fdr     := int_orig_row_y + 1;
         
      END IF;
      
   END IF;
   
   -- 3,3
   IF  mat_values_fac[3][3] > int_largest_accum
   AND mat_values_fdr[2][2] <> 2
   THEN
      IF mat_values_fdr[3][2] <> 1
      AND mat_values_fdr[2][3] <> 4
      THEN
         int_largest_accum := mat_values_fac[3][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[3][2] = 1
      AND   mat_values_fac[3][2] + 7 < mat_values_fac[3][3]
      THEN
         int_largest_accum := mat_values_fac[3][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y + 1;
      
      ELSIF mat_values_fdr[2][3] = 4
      AND   mat_values_fac[2][3] + 7 < mat_values_fac[3][3]
      THEN
         int_largest_accum := mat_values_fac[3][3];
         int_column_x_fdr  := int_orig_column_x + 1;
         int_row_y_fdr     := int_orig_row_y + 1;
         
      END IF;
      
   END IF;
   --RAISE WARNING '% %', int_column_x_fdr, int_row_y_fdr;
   
   --------------------------------------------------------------------------
   -- Step 80
   -- Return results
   --------------------------------------------------------------------------
   out_columnX := int_column_x_fdr;
   out_rowY    := int_row_y_fdr;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.deepest_cell(
    GEOMETRY
   ,RASTER
   ,RASTER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.deepest_cell(
    GEOMETRY
   ,RASTER
   ,RASTER
) TO PUBLIC;
