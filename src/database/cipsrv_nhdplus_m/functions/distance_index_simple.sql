DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.distance_index_simple';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.distance_index_simple(
    IN  p_point                     GEOMETRY
   ,IN  p_fcode_allow               INTEGER[]
   ,IN  p_fcode_deny                INTEGER[]
   ,IN  p_distance_max_distkm       NUMERIC
   ,IN  p_limit_innetwork           BOOLEAN
   ,IN  p_limit_navigable           BOOLEAN
   ,IN  p_known_region              VARCHAR
   ,OUT out_path_distance_km        NUMERIC
   ,OUT out_region                  VARCHAR
   ,OUT out_nhdplusid               BIGINT
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                     RECORD;
   curs_candidates         REFCURSOR;
   num_distance_max_distkm NUMERIC  := p_distance_max_distkm;
   boo_limit_innetwork     BOOLEAN  := p_limit_innetwork;
   boo_limit_navigable     BOOLEAN  := p_limit_navigable;
   int_raster_srid         INTEGER;
   sdo_input               GEOMETRY;
   rec_flowline            RECORD;
   int_counter             INTEGER;
   boo_check_fcode_allow   BOOLEAN := FALSE;
   boo_check_fcode_deny    BOOLEAN := FALSE;
   
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
   
   IF num_distance_max_distkm IS NULL
   OR num_distance_max_distkm <= 0
   THEN
      num_distance_max_distkm := 99999;
      
   END IF;
   
   IF p_fcode_allow IS NOT NULL
   AND array_length(p_fcode_allow,1) > 0
   THEN
      boo_check_fcode_allow := TRUE;
   
   END IF;
   
   IF p_fcode_deny IS NOT NULL
   AND array_length(p_fcode_deny,1) > 0
   THEN
      boo_check_fcode_deny := TRUE;
   
   END IF;
   
   IF boo_limit_innetwork IS NULL
   THEN
      boo_limit_innetwork := FALSE;
      
   END IF;
   
   IF boo_limit_navigable IS NULL
   THEN
      boo_limit_navigable := FALSE;
      
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
   IF ST_SRID(p_point) = int_raster_srid
   THEN
      sdo_input := p_point;
      
   ELSE
      sdo_input := ST_Transform(p_point,int_raster_srid);
      
   END IF;

   -------------------------------------------------------------------------
   -- Step 40
   -- Open the cursor required
   -------------------------------------------------------------------------
   IF int_raster_srid = 5070
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_5070 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSIF int_raster_srid = 3338
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_3338 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSIF int_raster_srid = 26904
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_26904 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSIF int_raster_srid = 32161
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_32161 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSIF int_raster_srid = 32655
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_32655 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSIF int_raster_srid = 32702
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.nhdplusid
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.nhdplusid
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_32702 aa
         WHERE
         (  boo_check_fcode_allow IS FALSE
            OR
            aa.fcode = ANY(p_fcode_allow)
         )
         AND
         (  boo_check_fcode_deny IS FALSE
            OR
            aa.fcode != ALL(p_fcode_deny)
         )
         AND
         (  boo_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  boo_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT 1
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm;
      
   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;
   
   -------------------------------------------------------------------------
   -- Step 50
   -- Iterate the cursor into array of output type
   --------------------------------------------------------------------------
   int_counter := 0;
   LOOP 
      FETCH curs_candidates INTO rec_flowline; 
      EXIT WHEN NOT FOUND; 
      
      int_counter          := int_counter + 1;
      out_nhdplusid        := rec_flowline.nhdplusid;
      out_path_distance_km := rec_flowline.snap_distancekm;
      
   END LOOP; 
   
   CLOSE curs_candidates; 
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Error out if no results
   --------------------------------------------------------------------------
   IF int_counter = 0
   OR out_nhdplusid IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'No results found';
      RETURN;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.distance_index_simple(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.distance_index_simple(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) TO PUBLIC;
