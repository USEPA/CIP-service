DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_wbd.best_spatial_fit';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_wbd.best_spatial_fit(
    IN  p_geometry                     GEOMETRY
   ,IN  p_objectid                     INTEGER
   ,IN  p_nhdplusid                    BIGINT
   ,IN  p_vpuid                        VARCHAR
   ,IN  p_wbd_version                  VARCHAR
   ,IN  p_percent_threshold            NUMERIC DEFAULT 1
   ,IN  p_grid_system                  VARCHAR DEFAULT 'nhdplus_m'
   ,IN  p_known_region                 VARCHAR DEFAULT NULL
) RETURNS TABLE(
    out_objectid                   INTEGER
   ,out_xwalk_huc12                VARCHAR
   ,out_xwalk_method               VARCHAR
   ,out_xwalk_huc12_version        VARCHAR
   ,out_nhdplusid                  BIGINT
   ,out_catchment_areasqkm         NUMERIC
   ,out_overlap_areasqkm           NUMERIC
   ,out_vpuid                      VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                     RECORD;
   sdo_input               GEOMETRY;
   sdo_intersection        GEOMETRY;
   int_raster_srid         INTEGER;
   int_return_code         INTEGER;
   str_status_message      VARCHAR;
   str_wbd_table           VARCHAR;
   str_huc12               VARCHAR;
   num_overlap             NUMERIC;
   num_running_best        NUMERIC;
   str_running_huc12       VARCHAR;
   num_huc12_area          NUMERIC;
   num_input_area          NUMERIC;
   num_percent_overlap     NUMERIC;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection
   --------------------------------------------------------------------------
   IF p_grid_system IN ('nhdplus_h','HR')
   THEN
      rec := cipsrv_nhdplus_h.determine_grid_srid(
          p_geometry       := p_geometry
         ,p_known_region   := p_known_region
      );
      int_raster_srid    := rec.out_srid;
      int_return_code    := rec.out_return_code;
      str_status_message := rec.out_status_message;
   
   ELSE
      rec := cipsrv_nhdplus_m.determine_grid_srid(
          p_geometry       := p_geometry
         ,p_known_region   := p_known_region
      );
      int_raster_srid    := rec.out_srid;
      int_return_code    := rec.out_return_code;
      str_status_message := rec.out_status_message;
   
   END IF;
   
   IF int_return_code != 0
   THEN
      RETURN QUERY
         SELECT 
          p_objectid
         ,NULL::VARCHAR
         ,NULL::VARCHAR
         ,NULL::VARCHAR
         ,p_nhdplusid::BIGINT
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,p_vpuid;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Project input geometry if required
   --------------------------------------------------------------------------
   IF ST_SRID(p_geometry) = int_raster_srid
   THEN
      sdo_input := p_geometry;
      
   ELSE
      sdo_input := ST_Transform(p_geometry,int_raster_srid);
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Determine query table
   --------------------------------------------------------------------------
   str_wbd_table := 'wbd_hu12_' || LOWER(p_wbd_version) || '_' || int_raster_srid::VARCHAR;
   
   IF NOT cipsrv_engine.resource_exists('cipsrv_wbd',str_wbd_table)
   THEN
      RAISE EXCEPTION 'wbd table not found - cipsrv_wbd.%',str_wbd_table;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Search for matches
   --------------------------------------------------------------------------
   str_huc12         := NULL;
   num_overlap       := NULL;
   num_running_best  := 0;
   str_running_huc12 := NULL;
   
   FOR rec IN EXECUTE
   'SELECT a.huc12,a.shape FROM cipsrv_wbd.' || str_wbd_table || ' a ' ||
   'WHERE ST_INTERSECTS(a.shape,$1) '
   USING sdo_input
   LOOP
      sdo_intersection := ST_COLLECTIONEXTRACT(ST_INTERSECTION(rec.shape,sdo_input),3);
      IF sdo_intersection IS NOT NULL AND NOT ST_ISEMPTY(sdo_intersection)
      THEN
         num_overlap := ST_AREA(ST_TRANSFORM(sdo_intersection,4326)::GEOGRAPHY)::NUMERIC / 1000000;

         IF num_overlap > 0.00000005
         THEN
            IF num_overlap > num_running_best
            THEN
               num_running_best  := num_overlap;
               str_running_huc12 := rec.huc12;
               
            END IF;
         
         END IF;
      
      END IF;      
 
   END LOOP;
   
   IF num_running_best = 0
   THEN
      RETURN QUERY
         SELECT 
          p_objectid
         ,NULL::VARCHAR
         ,NULL::VARCHAR
         ,NULL::VARCHAR
         ,p_nhdplusid::BIGINT
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,p_vpuid;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Check percentage is viable
   --------------------------------------------------------------------------
   num_input_area := ST_AREA(ST_TRANSFORM(sdo_input,4326)::GEOGRAPHY)::NUMERIC / 1000000;
   num_percent_overlap := (num_running_best / num_input_area) * 100;

   IF num_percent_overlap < p_percent_threshold
   THEN
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Return results
   --------------------------------------------------------------------------
   RETURN QUERY
      SELECT 
       p_objectid
      ,str_running_huc12
      ,'SFIT'::VARCHAR
      ,p_wbd_version
      ,p_nhdplusid::BIGINT
      ,num_input_area
      ,num_running_best
      ,p_vpuid;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_wbd.best_spatial_fit';
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

