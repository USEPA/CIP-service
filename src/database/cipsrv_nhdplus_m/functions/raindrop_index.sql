DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.raindrop_index';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.raindrop_index(
    IN  p_point                        GEOMETRY
   ,IN  p_fcode_allow                  INTEGER[]
   ,IN  p_fcode_deny                   INTEGER[]
   ,IN  p_raindrop_snap_max_distkm     NUMERIC
   ,IN  p_raindrop_path_max_distkm     NUMERIC
   ,IN  p_limit_innetwork              BOOLEAN
   ,IN  p_limit_navigable              BOOLEAN
   ,IN  p_return_link_path             BOOLEAN
   ,IN  p_known_region                 VARCHAR
   ,OUT out_flowlines                  cipsrv_nhdplus_m.snapflowline[]
   ,OUT out_path_distance_km           NUMERIC
   ,OUT out_end_point                  GEOMETRY
   ,OUT out_indexing_line              GEOMETRY
   ,OUT out_region                     VARCHAR
   ,OUT out_nhdplusid                  BIGINT
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                          RECORD;
   num_raindrop_snap_max_distkm NUMERIC  := p_raindrop_snap_max_distkm;
   num_raindrop_path_max_distkm NUMERIC  := p_raindrop_path_max_distkm;
   boo_limit_innetwork          BOOLEAN  := p_limit_innetwork;
   boo_limit_navigable          BOOLEAN  := p_limit_navigable;
   boo_return_link_path         BOOLEAN  := p_return_link_path;
   num_cell_width_km            NUMERIC;
   num_cell_diagonal            NUMERIC;
   int_raster_srid              INTEGER;
   rec_flowline                 RECORD;
   sdo_temporary                GEOMETRY;
   l_point                      GEOMETRY;
   l_nearest_flowline_dist_km   NUMERIC := 0;
   l_traveled_distance_km       NUMERIC := 0;
   l_distance_tmp_km            NUMERIC := 0;
   l_permanent_identifier       BIGINT;
   l_raster                     RASTER;
   l_raster_rid                 INTEGER;
   l_columnX                    INTEGER;
   l_rowY                       INTEGER;
   l_offsetX                    INTEGER;
   l_offsetY                    INTEGER;
   l_current_direction          INTEGER := -1;
   l_last_direction             INTEGER := -2;
   l_before_last_direction      INTEGER := -3;
   l_last_point                 GEOMETRY;
   l_temp_point                 GEOMETRY;
   obj_flowline                 cipsrv_nhdplus_m.snapflowline;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF p_point IS NULL OR ST_IsEmpty(p_point) 
   OR ST_GeometryType(p_point) <> 'ST_Point'
   THEN
      out_return_code    := -99;
      out_status_message := 'Input must be point geometry.';
      RETURN;
      
   END IF;
   
   IF num_raindrop_snap_max_distkm IS NULL
   OR num_raindrop_snap_max_distkm <= 0
   THEN
      num_raindrop_snap_max_distkm := 2;
      
   END IF;
   
   IF boo_limit_innetwork IS NULL
   THEN
      boo_limit_innetwork := FALSE;
      
   END IF;
   
   IF boo_limit_navigable IS NULL
   THEN
      boo_limit_navigable := FALSE;
      
   END IF;
   
   IF boo_return_link_path IS NULL
   THEN
      boo_return_link_path := FALSE;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Bail if rasters are not installed
   --------------------------------------------------------------------------
   IF NOT cipsrv_engine.table_exists(
       p_schema_name := 'cipsrv_nhdplusgrid_h'
      ,p_table_name  := 'fdr_5070_rdt'
   )
   THEN
      out_return_code    := -2;
      out_status_message := 'NHDPlus h raster data not installed.';
      RETURN;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   num_cell_width_km  := rec.out_grid_size / 1000;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   out_region         := rec.out_srid::VARCHAR;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
     
   --------------------------------------------------------------------------
   -- Step 40
   -- Project input point if required
   --------------------------------------------------------------------------
   IF ST_SRID(p_point) = int_raster_srid
   THEN
      l_point := p_point;
      
   ELSE
      l_point := ST_Transform(p_point,int_raster_srid);
      
   END IF;
   
   num_cell_diagonal := num_cell_width_km * SQRT(2);
   
   IF num_raindrop_path_max_distkm IS NULL
   THEN
      num_raindrop_path_max_distkm := num_cell_diagonal;
      
   END IF;
   
   IF boo_return_link_path
   THEN
      out_indexing_line := l_point;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 50
   -- Check if we are not already on top of the result
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.distance_index(
       p_point               := l_point
      ,p_fcode_allow         := p_fcode_allow
      ,p_fcode_deny          := p_fcode_deny
      ,p_distance_max_distkm := num_cell_diagonal
      ,p_limit_innetwork     := boo_limit_innetwork
      ,p_limit_navigable     := boo_limit_navigable
      ,p_return_link_path    := boo_return_link_path
      ,p_known_region        := int_raster_srid::VARCHAR
   );

   IF  rec.out_return_code = 0
   AND rec.out_path_distance_km < num_cell_width_km
   THEN
      out_flowlines        := rec.out_flowlines;
      out_path_distance_km := rec.out_path_distance_km;
      out_end_point        := rec.out_end_point;
      out_indexing_line    := rec.out_indexing_line;
      out_nhdplusid        := rec.out_flowlines[1].nhdplusid;
      out_return_code      := rec.out_return_code;
      out_status_message   := rec.out_status_message;
      RETURN;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Keeps track of total distance
   --------------------------------------------------------------------------
   sdo_temporary := l_point;
   
   rec := cipsrv_nhdplus_m.raindrop_world_to_raster(
       p_point        := l_point
      ,p_known_region := int_raster_srid::VARCHAR
      ,p_preprojected := TRUE
   );
   
   IF rec.out_return_code <> 0
   THEN
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      RETURN;
   
   END IF;
   
   l_raster     := rec.out_raster;
   l_raster_rid := rec.out_rid;
   l_columnX    := rec.out_column_x;
   l_rowY       := rec.out_row_y;
   l_offsetX    := rec.out_offset_x;
   l_offsetY    := rec.out_offset_y;
   
   l_point := cipsrv_nhdplus_m.raindrop_st_pixelascentroid(
       p_raster    := l_raster
      ,p_column_x  := l_columnX
      ,p_row_y     := l_rowY 
      ,p_offset_x  := l_offsetX
      ,p_offset_y  := l_offsetY
   );
   
   out_indexing_line := ST_MakeLine(
       geom1  := out_indexing_line
      ,geom2  := l_point
   );
   
   -- Be certain the grid srid is meter based!
   out_path_distance_km := ST_Distance(
       sdo_temporary
      ,l_point
   ) / 1000;

   --------------------------------------------------------------------------
   -- Step 70
   -- Primary Loop to traverse the flow direction grid
   --------------------------------------------------------------------------
   LOOP

      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 70.10
      -- Determine distance to nearest qualifying NHDPlus flowline
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      rec := cipsrv_nhdplus_m.distance_index_simple(
          p_point               := l_point
         ,p_fcode_allow         := p_fcode_allow
         ,p_fcode_deny          := p_fcode_deny
         ,p_distance_max_distkm := num_raindrop_snap_max_distkm + num_raindrop_path_max_distkm
         ,p_limit_innetwork     := boo_limit_innetwork
         ,p_limit_navigable     := boo_limit_navigable
         ,p_known_region        := int_raster_srid::VARCHAR
      );

      IF rec.out_return_code <> 0
      THEN
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         
         IF out_indexing_line IS NOT NULL
         THEN
            out_indexing_line := ST_Transform(out_indexing_line,4269);
         
         END IF;
               
         RETURN;
      
      END IF;

      l_nearest_flowline_dist_km := rec.out_path_distance_km;
      l_permanent_identifier     := rec.out_nhdplusid;
      --RAISE WARNING '% %', l_nearest_flowline_dist_km, l_permanent_identifier;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.20
      -- Continue checking and traversing until these conditions are met:
      --   * within the proximity value specified
      --   * traversed the maximum number of meters specified
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      --RAISE WARNING '% % % % %', l_nearest_flowline_dist_km, num_cell_diagonal, out_path_distance_km, num_raindrop_path_max_distkm, l_permanent_identifier;
      
      EXIT WHEN l_nearest_flowline_dist_km IS NULL
      OR l_nearest_flowline_dist_km  <= num_cell_diagonal
      OR out_path_distance_km        >  num_raindrop_path_max_distkm;

      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.30
      -- If no raster current then fetch one
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      IF l_raster IS NULL
      THEN
         rec := cipsrv_nhdplus_m.raindrop_world_to_raster(
             p_point        := l_point
            ,p_known_region := int_raster_srid::VARCHAR
            ,p_preprojected := TRUE
         );
         --RAISE WARNING '% %', r.pReturnCode, r.pStatusMessage;
         
         IF rec.out_return_code <> 0
         THEN
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
            IF out_indexing_line IS NOT NULL
            THEN
               out_indexing_line := ST_Transform(out_indexing_line,4269);
            
            END IF;
               
            RETURN;
         
         END IF;
         
         l_raster     := rec.out_raster;
         l_raster_rid := rec.out_rid;
         l_columnX    := rec.out_column_x;
         l_rowY       := rec.out_row_y;
         l_offsetX    := rec.out_offset_x;
         l_offsetY    := rec.out_offset_y;
      
      END IF;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.40
      -- Keeps track of single cell travel distance
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      l_traveled_distance_km := 0;

      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.50
      -- Keeps track of distance traveled this loop only so we know when to
      -- stop and check for the nearest flowline again.
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      l_distance_tmp_km := 0;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.60
      -- Walk the grid
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      WHILE l_distance_tmp_km * 1.7 <= l_nearest_flowline_dist_km
      LOOP

         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         -- Step 60.60.10
         -- Take a step on the raster
         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         rec := cipsrv_nhdplus_m.raindrop_next_coordinate(
             p_raster        := l_raster
            ,p_grid_size_km  := num_cell_width_km
            ,p_offset_x      := l_offsetX
            ,p_offset_y      := l_offsetY
            ,inout_column_x  := l_columnX
            ,inout_row_y     := l_rowY
         );
         
         IF rec.out_return_code <> 0
         THEN
            out_return_code    := rec.out_return_code;
            out_status_message := rec.out_status_message;
            
            IF out_indexing_line IS NOT NULL
            THEN
               out_indexing_line  := ST_Transform(out_indexing_line,4269);
            
            END IF;
               
            RETURN;
         
         END IF;

         l_columnX              := rec.inout_column_x;
         l_rowY                 := rec.inout_row_y;
         l_traveled_distance_km := rec.out_distance_km;
         l_current_direction    := rec.out_direction;
         l_distance_tmp_km      := l_distance_tmp_km  + l_traveled_distance_km;
         out_path_distance_km   := out_path_distance_km + l_traveled_distance_km;

         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         -- Step 60.60.20
         -- Check if we have left the current raster
         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         IF l_columnX <  l_offsetX
         OR l_rowY    <  l_offsetY
         OR l_columnX >= l_offsetX + ST_Width(l_raster)
         OR l_rowY    >= l_offsetY + ST_Height(l_raster)
         THEN
            rec := cipsrv_nhdplus_m.raindrop_coord_to_raster(
                p_column_x := l_columnX
               ,p_row_y    := l_rowY
               ,p_region   := int_raster_srid::VARCHAR
            );
            
            IF rec.out_return_code <> 0
            THEN
               out_return_code    := rec.out_return_code;
               out_status_message := rec.out_status_message;
               
               IF out_indexing_line IS NOT NULL
               THEN
                  out_indexing_line  := ST_Transform(out_indexing_line,4269);
               
               END IF;
               
               RETURN;
            
            END IF;
            
            l_raster_rid := rec.out_rid;
            l_raster     := rec.out_raster;
            l_offsetX    := rec.out_offset_x;
            l_offsetY    := rec.out_offset_y;
         
         END IF;

         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         -- Step 60.60.30
         -- Capture the cell point if so required
         --^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         IF boo_return_link_path
         THEN
            l_point := cipsrv_nhdplus_m.raindrop_st_pixelascentroid(
                p_raster     := l_raster
               ,p_column_x   := l_columnX
               ,p_row_y      := l_rowY 
               ,p_offset_x   := l_offsetX
               ,p_offset_y   := l_offsetY
            );
            
            IF l_current_direction <> l_last_direction
            THEN
               IF l_last_direction = l_before_last_direction
               THEN
                  out_indexing_line := ST_MakeLine(
                      geom1  := out_indexing_line
                     ,geom2  := l_last_point
                  );
                  --RAISE WARNING '%', ST_AsText(ST_Transform(out_indexing_line,4269));
                  
               END IF;
               
               out_indexing_line := ST_MakeLine(
                   geom1  := out_indexing_line
                  ,geom2  := l_point
               );
               
            END IF;
            
            l_before_last_direction := l_last_direction;
            l_last_direction        := l_current_direction;
            l_last_point            := l_point;
         
         END IF;
         
      END LOOP;
      
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- Step 60.70
      -- Convert location back to point 
      --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      l_point := cipsrv_nhdplus_m.raindrop_st_pixelascentroid(
          p_raster     := l_raster
         ,p_column_x   := l_columnX
         ,p_row_y      := l_rowY 
         ,p_offset_x   := l_offsetX
         ,p_offset_y   := l_offsetY
      );

   END LOOP;
   --RAISE WARNING '%',l_nearest_flowline_dist_km;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Check if the path has wandered away from eligible flowlines
   --------------------------------------------------------------------------
   IF l_nearest_flowline_dist_km IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'No results found';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 80
   -- Get the closest eligible flowline that matches snap distance
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.distance_index(
       p_point               := l_point
      ,p_fcode_allow         := p_fcode_allow
      ,p_fcode_deny          := p_fcode_deny
      ,p_distance_max_distkm := num_raindrop_snap_max_distkm
      ,p_limit_innetwork     := boo_limit_innetwork
      ,p_limit_navigable     := boo_limit_navigable
      ,p_return_link_path    := boo_return_link_path
      ,p_known_region        := int_raster_srid::VARCHAR
   );

   IF rec.out_return_code <> 0
   THEN
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      RETURN;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 90
   -- To avoid overshoot, recheck the distance using center of last segment
   --------------------------------------------------------------------------
   
   --------------------------------------------------------------------------
   -- Step 100
   -- Load the output array
   --------------------------------------------------------------------------
   IF boo_return_link_path
   THEN
      out_indexing_line := ST_MakeLine(
          geom1  := out_indexing_line
         ,geom2  := l_point
      );
      --RAISE WARNING '%', ST_AsText(ST_Transform(out_indexing_line,4269));
      
   END IF;
   
   out_flowlines := rec.out_flowlines;

   FOR i IN 1 .. array_length(out_flowlines,1)
   LOOP
      obj_flowline := out_flowlines[i];
      obj_flowline.snap_distancekm := obj_flowline.snap_distancekm + out_path_distance_km;
      out_flowlines[i] := obj_flowline;

   END LOOP;
   
   --------------------------------------------------------------------------
   -- Step 110
   -- Clean up the output products
   --------------------------------------------------------------------------
   out_end_point        := rec.out_end_point;
   sdo_temporary        := ST_Transform(out_end_point,int_raster_srid);
   out_path_distance_km := out_path_distance_km + ST_Distance(
       l_point
      ,sdo_temporary
   ) / 1000;
   out_nhdplusid        := out_flowlines[1].nhdplusid;
   
   IF boo_return_link_path
   AND out_path_distance_km > 0.00005
   THEN
      out_indexing_line := ST_MakeLine(
          geom1  := out_indexing_line
         ,geom2  := sdo_temporary
      );
      
      out_indexing_line := ST_Transform(out_indexing_line,4269);

   ELSE
      out_indexing_line := NULL;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.raindrop_index';
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

