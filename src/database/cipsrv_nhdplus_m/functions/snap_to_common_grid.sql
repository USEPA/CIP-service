DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.snap_to_common_grid';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.snap_to_common_grid(
    IN  p_geometry          GEOMETRY
   ,IN  p_known_region      VARCHAR
   ,IN  p_grid_size         NUMERIC
) RETURNS GEOMETRY
IMMUTABLE
AS $BODY$ 
DECLARE
   rec                RECORD;
   sdo_incoming       GEOMETRY;
   int_raster_srid    INTEGER;
   int_return_code    INTEGER;
   str_status_message VARCHAR;
   geom_grid          GEOMETRY;
   num_lower_x        NUMERIC;
   num_lower_y        NUMERIC;
   incoming_srid      INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  p_geometry IS NULL
   OR  ST_ISEMPTY(p_geometry)
   THEN
      RETURN NULL;
      
   END IF;
   
   incoming_srid := ST_SRID(p_geometry);
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection if needed
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   int_return_code    := rec.out_return_code;
   str_status_message := rec.out_status_message;
   
   IF int_return_code != 0
   THEN
      RAISE EXCEPTION 'err %: %',int_return_code,str_status_message;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Project input geometry if required
   --------------------------------------------------------------------------
   IF ST_SRID(p_geometry) = int_raster_srid
   THEN
      sdo_incoming := p_geometry;
      
   ELSE
      sdo_incoming := ST_Transform(p_geometry,int_raster_srid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Get the lower point of the common grid space
   ----------------------------------------------------------------------------
   geom_grid   := cipsrv_nhdplus_m.generic_common_mbr(int_raster_srid::VARCHAR);
   num_lower_x := ST_XMIN(geom_grid);
   num_lower_y := ST_YMIN(geom_grid);
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return results
   ----------------------------------------------------------------------------
   RETURN ST_SNAPTOGRID(
       sdo_incoming
      ,num_lower_x
      ,num_lower_y
      ,p_grid_size
      ,p_grid_size
   );
   
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.snap_to_common_grid';
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

