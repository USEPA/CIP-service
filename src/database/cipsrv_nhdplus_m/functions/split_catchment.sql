DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.split_catchment';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.split_catchment(
    IN  p_split_point  GEOMETRY
   ,IN  p_nhdplusid    BIGINT   DEFAULT NULL
   ,IN  p_known_region VARCHAR  DEFAULT NULL
) RETURNS GEOMETRY
STABLE
AS $BODY$
DECLARE 
   rec               RECORD;
   int_nhdplusid     BIGINT       := p_nhdplusid;
   int_original_srid INTEGER;
   int_raster_srid   INTEGER;
   sdo_results       GEOMETRY;
   sdo_results2      GEOMETRY;
   sdo_point         GEOMETRY;
   sdo_catchment     GEOMETRY;
   sdo_catchmentsp   GEOMETRY;
   sdo_temp          GEOMETRY;
   sdo_temp2         GEOMETRY;
   sdo_top_point     GEOMETRY;
   sdo_downstream    GEOMETRY;
   sdo_grid_point    GEOMETRY;
   sdo_catch_buffer  GEOMETRY;
   int_depth         INTEGER := 1;
   rast              RASTER;
   rast_fac          RASTER;
   int_column_x      INTEGER;
   int_row_y         INTEGER;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters and set parameters
   --------------------------------------------------------------------------
   IF p_split_point IS NULL
   THEN
      RETURN NULL;
      
   END IF;    
   
   IF ST_GeometryType(p_split_point) <> 'ST_Point'
   THEN
      RAISE EXCEPTION 'split point geometry must be single point';

   END IF;

   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection 
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_split_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   
   IF rec.out_return_code != 0
   THEN
      RAISE EXCEPTION '%: %',rec.out_return_code,rec.out_status_message;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 30
   -- Project initial point
   --------------------------------------------------------------------------
   int_original_srid := ST_Srid(p_split_point);

   IF int_original_srid <> int_raster_srid
   THEN
      sdo_point := ST_Transform(p_split_point,int_raster_srid);
      
   ELSE
      sdo_point := p_split_point;

   END IF;

   --------------------------------------------------------------------------
   -- Step 40
   -- Fetch the catchment outline to clip with
   --------------------------------------------------------------------------
   IF int_nhdplusid IS NOT NULL
   THEN
      IF int_raster_srid = 5070
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_m.catchment_5070_full a
         WHERE
         a.nhdplusid = int_nhdplusid;
         
      ELSIF int_raster_srid = 3338
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_m.catchment_3338_full a
         WHERE
         a.nhdplusid = int_nhdplusid;

      ELSIF int_raster_srid = 26904
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_m.catchment_26904_full a
         WHERE
         a.nhdplusid = int_nhdplusid;

      ELSIF int_raster_srid = 32161
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_m.catchment_32161_full a
         WHERE
         a.nhdplusid = int_nhdplusid;

      ELSIF int_raster_srid = 32655
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_m.catchment_32655_full a
         WHERE
         a.nhdplusid = int_nhdplusid;

      ELSIF int_raster_srid = 32702
      THEN
         SELECT
         a.shape
         INTO 
         sdo_catchment
         FROM
         cipsrv_nhdplus_m.catchment_32702_full a
         WHERE
         a.nhdplusid = int_nhdplusid;

      ELSE
         RAISE EXCEPTION 'err';
         
      END IF;
   
   ELSE
      IF int_raster_srid = 5070
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_m.catchment_5070_full a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSIF int_raster_srid = 3338
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_m.catchment_3338_full a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSIF int_raster_srid = 26904
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_m.catchment_26904_full a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSIF int_raster_srid = 32161
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_m.catchment_32161_full a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSIF int_raster_srid = 32655
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_m.catchment_32655_full a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSIF int_raster_srid = 32702
      THEN
         SELECT
          ST_SnapToGrid(a.shape,0.05)
         ,a.nhdplusid
         INTO 
          sdo_catchment
         ,int_nhdplusid
         FROM
         cipsrv_nhdplus_m.catchment_32702_full a
         WHERE
         ST_INTERSECTS(a.shape,sdo_point) LIMIT 1; 

      ELSE
         RAISE EXCEPTION 'err';
         
      END IF;

   END IF;

   IF sdo_catchment IS NULL
   THEN
      RETURN NULL;

   END IF;
  
   --------------------------------------------------------------------------
   -- Step 50
   -- Buffer the catchment to get a slightly larger fdr raster 
   -- This size value may need tuning
   --------------------------------------------------------------------------
   sdo_catch_buffer := ST_Buffer(
       sdo_catchment
      ,50
   );
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Fetch the catchment rasters
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.fetch_grids_by_geometry(
       p_FDR_input        := sdo_catch_buffer
      ,p_FAC_input        := sdo_catchment
      ,p_known_region     := int_raster_srid::VARCHAR
      ,p_FDR_nodata       := 255
      ,p_FAC_nodata       := 2147483647
      ,p_crop             := TRUE
   );
   rast     := rec.out_FDR;
   rast_fac := rec.out_FAC;
   
   IF rast IS NULL
   THEN
      RAISE EXCEPTION 'No results for FDR raster';
      
   ELSIF rast_fac IS NULL
   THEN
      RAISE EXCEPTION 'No results for FAC raster';
   
   END IF;
   --RAISE WARNING 'inc area %',ST_AREA(sdo_catchment);
   --RAISE WARNING 'fdr area % sum %',ST_AREA(ST_MinConvexHull(rast)),ST_Summary(rast);
   --RAISE WARNING 'fac area % sum %',ST_AREA(ST_MinConvexHull(rast_fac)),ST_Summary(rast_fac);

   --------------------------------------------------------------------------
   -- Step 70
   -- Determine what we think is the top of the flowline
   --------------------------------------------------------------------------
   sdo_top_point := cipsrv_nhdplus_m.top_of_flow(
       p_nhdplusid            := int_nhdplusid
      ,p_permanent_identifier := NULL
      ,p_reachcode            := NULL 
      ,p_known_region         := int_raster_srid::VARCHAR
      ,p_2d_flag              := TRUE
      ,p_polygon_mask         := ST_Buffer(sdo_catchment,-1)
   );
   --RAISE WARNING '%',ST_ASEWKT(ST_Transform(ST_Collect(sdo_top_point,sdo_catchment),4269));

   --------------------------------------------------------------------------
   -- Step 80
   -- Get the nearest cell on the flow grid
   --------------------------------------------------------------------------   
   rec := cipsrv_engine.deepest_cell(
       p_input               := sdo_top_point
      ,p_FDR                 := rast
      ,p_FAC                 := rast_fac 
   );
   int_column_x := rec.out_columnX;
   int_row_y    := rec.out_rowY;
   --RAISE WARNING '% %',int_column_x,int_row_y;
   --RAISE WARNING 'deepest cell %',ST_AsEWKT(ST_Transform(ST_PixelAsPoint(rast,int_column_x,int_row_y),4269));

   --------------------------------------------------------------------------
   -- Step 90
   -- Generate the downstream path
   --------------------------------------------------------------------------
   sdo_downstream := cipsrv_engine.raster_raindrop(
       p_raster       := rast
      ,p_columnX      := int_column_x
      ,p_rowY         := int_row_y
   );
   --RAISE WARNING '%',ST_AsEWKT(ST_Transform(sdo_downstream,4269));

   --------------------------------------------------------------------------
   -- Step 100
   -- Convert downstream path into multipoint and get closest point to input
   --------------------------------------------------------------------------
   SELECT 
   ST_Collect(geom) 
   INTO 
   sdo_downstream
   FROM
   ST_DumpPoints(sdo_downstream);
   
   sdo_grid_point := ST_ClosestPoint(
       sdo_downstream
      ,sdo_point
   );
   --RAISE WARNING '%',ST_AsEWKT(ST_Transform(sdo_catchment,4269));
   --RAISE WARNING '%',ST_AsEWKT(ST_Transform(sdo_grid_point,4269));
   
   --------------------------------------------------------------------------
   -- Step 110
   -- Use new grid point to split catchment
   --------------------------------------------------------------------------
   rec := ST_WorldToRasterCoord(
       rast
      ,sdo_grid_point
   );
   IF rec IS NULL
   THEN
      RAISE EXCEPTION 'point not on raster error';
      
   END IF;
   
   int_column_x := rec.columnx;
   int_row_y    := rec.rowy;
   --RAISE WARNING '% %',int_column_x,int_row_y;
   --RAISE WARNING 'split point %',ST_AsEWKT(ST_Transform(ST_PixelAsPoint(rast,int_column_x,int_row_y),4269));
   --RAISE WARNING '% %',ST_Width(rast),ST_Height(rast);
   
   --------------------------------------------------------------------------
   -- Step 120
   -- Recursively delineation the upstream cells
   --------------------------------------------------------------------------
   rec := cipsrv_engine.fdr_upstream_norecursion(
       p_column_x := int_column_x
      ,p_row_y    := int_row_y
      ,iout_rast  := rast
   );
   --RAISE WARNING '%',ST_AsEWKT(ST_Transform(ST_MinConvexHull(rec.iout_rast),4269));
   --RAISE WARNING '% %',ST_Width(rec.iout_rast),ST_Height(rec.iout_rast);
   
   --------------------------------------------------------------------------
   -- Step 130
   -- Blank out all but the delineated cells (make nodata 0 for ST_Polygon)
   --------------------------------------------------------------------------
   rast := ST_Reclass(
       rec.iout_rast
      ,1
      ,'[0-98]:0,99:99,[100-255]:0'
      ,'8BUI'
      ,0
   );
   --RAISE WARNING 'reclassed % %',ST_AREA(ST_MinConvexHull(rast)),ST_Summary(rast);
   
   --------------------------------------------------------------------------
   -- Step 140
   -- Build a polygon from the remaining cells with value 99
   -- Clip the results down the original catchment size to remove padding
   --------------------------------------------------------------------------
   sdo_results := ST_Polygon(
       ST_Clip(
           rast
          ,sdo_catchment
          ,255
          ,TRUE
       )
      ,1
   );
   --RAISE WARNING '%',ST_Area(sdo_catchment);
   --RAISE WARNING '% %',ST_Area(sdo_results),ST_AsEWKT(ST_Transform(sdo_results,4269));
   
   --------------------------------------------------------------------------
   -- Step 150
   -- Reduce to single polygon if possible
   --------------------------------------------------------------------------
   IF ST_NumGeometries(sdo_results) = 1
   THEN
      sdo_results := ST_GeometryN(sdo_results,1);

   END IF;
   
   --------------------------------------------------------------------------
   -- Step 180
   -- Project to match input point
   --------------------------------------------------------------------------
   IF int_original_srid <> int_raster_srid
   THEN
      sdo_results := ST_Transform(
          sdo_results
         ,int_original_srid
      );
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 190
   -- Return what we got
   --------------------------------------------------------------------------
   RETURN sdo_results;

END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.split_catchment(
    GEOMETRY
   ,BIGINT
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.split_catchment(
    GEOMETRY
   ,BIGINT
   ,VARCHAR
) TO PUBLIC;
