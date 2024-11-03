DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.raindrop_coord_to_raster';
   IF b IS NOT NULL THEN
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.raindrop_coord_to_raster(
    IN    p_column_x              INTEGER
   ,IN    p_row_y                 INTEGER
   ,IN    p_region                VARCHAR
   ,OUT   out_rid                 INTEGER
   ,OUT   out_raster              RASTER
   ,OUT   out_offset_x            INTEGER
   ,OUT   out_offset_y            INTEGER
   ,OUT   out_return_code         INTEGER
   ,OUT   out_status_message      VARCHAR
) 
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF p_column_x IS NULL
   OR p_row_y IS NULL
   THEN
      RETURN;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 20
   -- Query the grid as needed
   --------------------------------------------------------------------------
   IF UPPER(p_region) IN ('CONUS','5070')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_5070_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSIF UPPER(p_region) IN ('AK','3338')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_3338_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSIF UPPER(p_region) IN ('HI','26904')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_26904_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSIF UPPER(p_region) IN ('PRVI','32161')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_32161_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSIF UPPER(p_region) IN ('GUMP','32655')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_32655_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSIF UPPER(p_region) IN ('SAMOA','32702')
   THEN
      SELECT
       a.rid
      ,a.rast
      ,a.offset_x
      ,a.offset_y
      INTO STRICT
       out_rid
      ,out_raster
      ,out_offset_x
      ,out_offset_y
      FROM
      cipsrv_nhdplusgrid_h.fdr_32702_rdt a
      WHERE 
      ST_SetSRID(
         ST_MakeBox2D(
           ST_Point(a.offset_x,a.offset_y),
           ST_Point(a.offset_x + ST_Width(a.rast) - 1,a.offset_y + ST_Height(a.rast) - 1)
         ),0
      ) && ST_Point(p_column_x,p_row_y);
      
   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 30
   -- Check output is okay
   --------------------------------------------------------------------------
   IF p_column_x IS NULL
   OR p_row_y IS NULL
   THEN
      out_return_code    := -201;
      out_status_message := 'Unable to obtain X,Y location from input point on ' 
                     || p_region || ' raster grid';
      RETURN;
      
   END IF;

EXCEPTION
   WHEN NO_DATA_FOUND
   THEN
      out_return_code    := -201;
      out_status_message := 'Unable to obtain X,Y location from input point on ' 
                     || p_region || ' raster grid';
      RETURN;
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.raindrop_coord_to_raster(
    INTEGER
   ,INTEGER
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.raindrop_coord_to_raster(
    INTEGER
   ,INTEGER
   ,VARCHAR
) TO PUBLIC;
