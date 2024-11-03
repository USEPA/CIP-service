DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.raindrop_world_to_raster';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.raindrop_world_to_raster(
    IN  p_point                  GEOMETRY
   ,IN  p_known_region           VARCHAR
   ,IN  p_preprojected           BOOLEAN DEFAULT FALSE
   ,OUT out_column_x             INTEGER
   ,OUT out_row_y                INTEGER
   ,OUT out_rid                  INTEGER
   ,OUT out_raster               RASTER
   ,OUT out_offset_x             INTEGER
   ,OUT out_offset_y             INTEGER
   ,OUT out_return_code          INTEGER
   ,OUT out_status_message       VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                     RECORD;
   int_raster_srid         INTEGER;
   sdo_input               GEOMETRY;
   
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
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
     
   --------------------------------------------------------------------------
   -- Step 30
   -- Project input point if required
   --------------------------------------------------------------------------
   IF p_preprojected
   OR ST_SRID(p_point) = int_raster_srid
   THEN
      sdo_input := p_point;
      
   ELSE
      sdo_input := ST_Transform(p_point,int_raster_srid);
      
   END IF;

   -------------------------------------------------------------------------
   -- Step 40
   -- Query the grid as needed
   --------------------------------------------------------------------------
   IF int_raster_srid = 5070
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_5070_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   ELSIF int_raster_srid = 3338
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_3338_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   ELSIF int_raster_srid = 26904
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_26904_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   ELSIF int_raster_srid = 32161
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y 
      FROM
      cipsrv_nhdplusgrid_h.fdr_32161_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   ELSIF int_raster_srid = 32655
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y 
      FROM
      cipsrv_nhdplusgrid_h.fdr_32655_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   ELSIF int_raster_srid = 32702
   THEN
      SELECT
       (ST_WorldToRasterCoord(
           rast := a.rast
          ,pt   := sdo_input
       )).*
      ,a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO
       out_column_x
      ,out_row_y
      ,out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_32702_rdt a
      WHERE 
      ST_Intersects(a.rast,sdo_input);
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Check output is okay
   --------------------------------------------------------------------------
   IF out_column_x IS NULL
   OR out_row_y IS NULL
   THEN
      out_return_code    := -201;
      out_status_message := 'Unable to obtain X,Y location from input point on ' 
                     || int_raster_srid || ' raster grid';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Add the offsets
   --------------------------------------------------------------------------
   out_column_x := out_column_x + out_offset_x - 1;
   out_row_y    := out_row_y    + out_offset_y - 1;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.raindrop_world_to_raster(
    GEOMETRY
   ,VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.raindrop_world_to_raster(
    GEOMETRY
   ,VARCHAR
   ,BOOLEAN
) TO PUBLIC;
