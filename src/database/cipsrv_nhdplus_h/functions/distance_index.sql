DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.distance_index';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.distance_index(
    IN  p_point                     GEOMETRY
   ,IN  p_fcode_allow               INTEGER[]
   ,IN  p_fcode_deny                INTEGER[]
   ,IN  p_distance_max_distkm       NUMERIC
   ,IN  p_limit_innetwork           BOOLEAN
   ,IN  p_limit_navigable           BOOLEAN
   ,IN  p_return_link_path          BOOLEAN
   ,IN  p_known_region              VARCHAR
   ,OUT out_flowlines               cipsrv_nhdplus_h.snapflowline[]
   ,OUT out_path_distance_km        NUMERIC
   ,OUT out_end_point               GEOMETRY
   ,OUT out_indexing_line           GEOMETRY
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
   boo_return_link_path    BOOLEAN  := p_return_link_path;
   int_raster_srid         INTEGER;
   sdo_input               GEOMETRY;
   sdo_temp                GEOMETRY;
   rec_flowline            RECORD;
   rec_candidate           cipsrv_nhdplus_h.snapflowline;
   int_counter             INTEGER;
   num_nearest             NUMERIC;
   boo_check_fcode_allow   BOOLEAN := FALSE;
   boo_check_fcode_deny    BOOLEAN := FALSE;
   int_limit               INTEGER := 16;
   
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
   
   IF boo_return_link_path IS NULL
   THEN
      boo_return_link_path := FALSE;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   out_region         := rec.out_srid::VARCHAR;
   
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
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_5070 aa
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
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
      
   ELSIF int_raster_srid = 3338
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_3338 aa
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
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
   
   ELSIF int_raster_srid = 26904
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_26904 aa
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
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
      
   ELSIF int_raster_srid = 32161
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32161 aa
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
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
      
   ELSIF int_raster_srid = 32655
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32655 aa
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
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
      
   ELSIF int_raster_srid = 32702
   THEN
      OPEN curs_candidates FOR
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.enabled
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,a.snap_distancekm
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.reachcode
         ,aa.flowdir
         ,aa.wbarea_permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.mainpath
         ,aa.innetwork
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,aa.vpuid
         ,aa.enabled
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_h.nhdflowline_32702 aa
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
         (  p_limit_innetwork IS FALSE
            OR
            aa.hasvaa
         )
         AND
         (  p_limit_navigable IS FALSE
            OR
            aa.isnavigable
         )
         ORDER BY 
         aa.shape <-> sdo_input 
         LIMIT int_limit
      ) a
      WHERE
      a.snap_distancekm <= num_distance_max_distkm
      ORDER BY 
      a.snap_distancekm ASC;
      
   ELSE
      RAISE EXCEPTION 'err';
   
   END IF;

   -------------------------------------------------------------------------
   -- Step 50
   -- Iterate the cursor into array of output type
   --------------------------------------------------------------------------
   num_nearest := 0;
   int_counter := 1;
   FETCH NEXT FROM curs_candidates INTO rec_flowline;
   WHILE (FOUND) 
   LOOP 
      rec_candidate.permanent_identifier        := rec_flowline.permanent_identifier;
      rec_candidate.fdate                       := rec_flowline.fdate;
      rec_candidate.resolution                  := rec_flowline.resolution::integer;
      rec_candidate.gnis_id                     := rec_flowline.gnis_id;
      rec_candidate.gnis_name                   := rec_flowline.gnis_name;
      rec_candidate.lengthkm                    := rec_flowline.lengthkm;
      rec_candidate.reachcode                   := rec_flowline.reachcode;
      rec_candidate.flowdir                     := rec_flowline.flowdir;
      rec_candidate.wbarea_permanent_identifier := rec_flowline.wbarea_permanent_identifier;
      rec_candidate.ftype                       := rec_flowline.ftype;
      rec_candidate.fcode                       := rec_flowline.fcode;
      rec_candidate.mainpath                    := rec_flowline.mainpath; 
      rec_candidate.innetwork                   := rec_flowline.innetwork;
      rec_candidate.visibilityfilter            := rec_flowline.visibilityfilter;
      rec_candidate.nhdplusid                   := rec_flowline.nhdplusid;
      rec_candidate.vpuid                       := rec_flowline.vpuid;
      rec_candidate.enabled                     := rec_flowline.enabled;
      rec_candidate.fmeasure                    := rec_flowline.fmeasure;
      rec_candidate.tmeasure                    := rec_flowline.tmeasure;
      rec_candidate.hydroseq                    := rec_flowline.hydroseq;
      rec_candidate.shape                       := ST_Transform(rec_flowline.shape,4269);
      
      rec_candidate.snap_measure := ROUND(ST_InterpolatePoint(
          rec_flowline.shape
         ,sdo_input
      )::NUMERIC,5);
      
      rec_candidate.snap_distancekm             := rec_flowline.snap_distancekm;
      
      sdo_temp := ST_GeometryN(ST_LocateAlong(
          rec_flowline.shape
         ,rec_candidate.snap_measure
      ),1);
      
      -- This is some weird numeric nuttiness which is corrected by reifying as text 
      IF sdo_temp IS NULL
      OR ST_IsEmpty(sdo_temp)
      THEN
         sdo_temp := ST_GeometryN(ST_LocateAlong(
             ST_GeomFromEWKT(ST_AsEWKT(rec_flowline.shape))
            ,rec_candidate.snap_measure
         ),1);
         
      END IF;
 
      rec_candidate.snap_point := ST_Transform(ST_Force2D(
         sdo_temp
      ),4269);

      IF num_nearest = 0
      THEN
         num_nearest = rec_candidate.snap_distancekm;
         
      END IF;
      
      EXIT WHEN rec_candidate.snap_distancekm > num_nearest;
      
      out_flowlines[int_counter] := rec_candidate;
      int_counter := int_counter + 1;
      
      FETCH NEXT FROM curs_candidates INTO rec_flowline;
      
   END LOOP; 
   
   CLOSE curs_candidates;

   --------------------------------------------------------------------------
   -- Step 60
   -- Bail if no results
   --------------------------------------------------------------------------
   IF int_counter = 1
   OR out_flowlines IS NULL
   OR array_length(out_flowlines,1) = 0
   THEN
      out_return_code    := -1;
      out_status_message := 'No results found';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Finalize the output
   --------------------------------------------------------------------------
   out_path_distance_km := out_flowlines[1].snap_distancekm;
   out_end_point        := out_flowlines[1].snap_point;
   out_nhdplusid        := out_flowlines[1].nhdplusid;
   
   IF p_return_link_path
   AND out_path_distance_km > 0.00005
   THEN
      out_indexing_line := ST_MakeLine(
          ST_Transform(sdo_input,4269)
         ,out_end_point
      );
  
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.distance_index(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.distance_index(
    GEOMETRY
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) TO PUBLIC;
