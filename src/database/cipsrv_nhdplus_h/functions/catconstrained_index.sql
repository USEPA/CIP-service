DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.catconstrained_index';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.catconstrained_index(
    IN  p_point                        public.GEOMETRY
   ,IN  p_return_link_path             BOOLEAN
   ,IN  p_known_region                 VARCHAR
   ,IN  p_known_catchment_nhdplusid    BIGINT DEFAULT NULL
   ,OUT out_flowlines                  cipsrv_nhdplus_h.snapflowline[]
   ,OUT out_path_distance_km           NUMERIC
   ,OUT out_end_point                  public.GEOMETRY
   ,OUT out_indexing_line              public.GEOMETRY
   ,OUT out_region                     VARCHAR
   ,OUT out_nhdplusid                  BIGINT
   ,OUT out_reachcode                  VARCHAR
   ,OUT out_hydroseq                   BIGINT
   ,OUT out_snap_measure               NUMERIC
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                     RECORD;
   boo_return_link_path    BOOLEAN  := p_return_link_path;
   int_raster_srid         INTEGER;
   sdo_input               public.GEOMETRY;
   rec_candidate           cipsrv_nhdplus_h.snapflowline;
   int_nhdplusid           BIGINT;
   boo_issink              BOOLEAN;
   boo_isocean             BOOLEAN;
   boo_isalaskan           BOOLEAN;
   str_source_table        VARCHAR;
   str_srid                VARCHAR;
   str_sql                 VARCHAR;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF p_point IS NULL OR public.ST_ISEMPTY(p_point) 
   OR public.ST_GEOMETRYTYPE(p_point) <> 'ST_Point'
   THEN
      out_return_code    := -99;
      out_status_message := 'Input must be point geometry.';
      RETURN;
      
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
   IF public.ST_SRID(p_point) = int_raster_srid
   THEN
      sdo_input := p_point;
      
   ELSE
      sdo_input := public.ST_TRANSFORM(p_point,int_raster_srid);
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 40
   -- Determine the catchment if not provided
   --------------------------------------------------------------------------
   IF p_known_catchment_nhdplusid IS NOT NULL
   THEN
      int_nhdplusid := p_known_catchment_nhdplusid;
      
   ELSE
      IF int_raster_srid = 5070
      THEN
         str_source_table := 'cipsrv_nhdplus_h.catchment_5070';

      ELSIF int_raster_srid = 3338
      THEN
         str_source_table := 'cipsrv_nhdplus_h.catchment_3338';
         
      ELSIF int_raster_srid = 26904
      THEN
         str_source_table := 'cipsrv_nhdplus_h.catchment_26904';
         
      ELSIF int_raster_srid = 32161
      THEN
         str_source_table := 'cipsrv_nhdplus_h.catchment_32161';
         
      ELSIF int_raster_srid = 32655
      THEN
         str_source_table := 'cipsrv_nhdplus_h.catchment_32655';
         
      ELSIF int_raster_srid = 32702
      THEN
         str_source_table := 'cipsrv_nhdplus_h.catchment_32702';
         
      ELSE
         RAISE EXCEPTION 'err';
      
      END IF;
      
      str_sql := '
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         FROM
         ' || str_source_table || ' a
         WHERE
         public.ST_INTERSECTS(
             a.shape
            ,$1
         )
         LIMIT 1
      ';
      
      EXECUTE str_sql
      INTO 
       int_nhdplusid
      ,boo_issink
      ,boo_isocean
      ,boo_isalaskan
      USING
      sdo_input;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Bail if no results
   --------------------------------------------------------------------------
   IF int_nhdplusid IS NULL
   THEN
      out_return_code    := -2;
      out_status_message := 'no results found';
      RETURN;
   
   END IF;
   
   IF boo_issink
   OR boo_isocean
   OR boo_isalaskan
   THEN
      IF boo_issink
      THEN
         out_status_message := 'sink catchment without flowline';
      
      ELSIF boo_isocean
      THEN
         out_status_message := 'ocean catchment without flowline';
      
      ELSIF boo_isalaskan
      THEN
         out_status_message := 'alaskan catchment without flowline';
      
      END IF;
      
      out_return_code    := -3;
      out_nhdplusid      := int_nhdplusid;
      RETURN;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Pull the matching flowline
   --------------------------------------------------------------------------
   IF int_raster_srid = 5070
   THEN
      str_source_table := 'cipsrv_nhdplus_h.nhdflowline_5070';

   ELSIF int_raster_srid = 3338
   THEN
      str_source_table := 'cipsrv_nhdplus_h.nhdflowline_3338';
   
   ELSIF int_raster_srid = 26904
   THEN
      str_source_table := 'cipsrv_nhdplus_h.nhdflowline_26904';
      
   ELSIF int_raster_srid = 32161
   THEN
      str_source_table := 'cipsrv_nhdplus_h.nhdflowline_32161';
      
   ELSIF int_raster_srid = 32655
   THEN
      str_source_table := 'cipsrv_nhdplus_h.nhdflowline_32655';
      
   ELSIF int_raster_srid = 32702
   THEN
      str_source_table := 'cipsrv_nhdplus_h.nhdflowline_32702';
   
   END IF;
   
   str_sql := '
      SELECT 
       a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.totma
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
      ,NULL
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydroseq
      ,a.shape
      ,ROUND(
           a.snap_measure::NUMERIC
          ,5
       ) AS snap_measure
      ,a.snap_distancekm
      ,public.ST_TRANSFORM(public.ST_FORCE2D(
          public.ST_GEOMETRYN(
             public.ST_LOCATEALONG(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.fdate
         ,aa.resolution
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.lengthkm
         ,aa.totma
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
         ,NULL
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,public.ST_INTERPOLATEPOINT(
              aa.shape
             ,$1
          ) AS snap_measure
         ,public.ST_DISTANCE(
              public.ST_TRANSFORM(aa.shape,4326)::public.GEOGRAPHY
             ,public.ST_TRANSFORM($2,4326)::public.GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         ' || str_source_table || ' aa
         WHERE
         aa.nhdplusid = $3
      ) a
   ';
   
   EXECUTE str_sql
   INTO
    rec_candidate.permanent_identifier
   ,rec_candidate.fdate
   ,rec_candidate.resolution
   ,rec_candidate.gnis_id
   ,rec_candidate.gnis_name
   ,rec_candidate.lengthkm
   ,rec_candidate.totma
   ,rec_candidate.reachcode
   ,rec_candidate.flowdir
   ,rec_candidate.wbarea_permanent_identifier
   ,rec_candidate.ftype
   ,rec_candidate.fcode
   ,rec_candidate.mainpath
   ,rec_candidate.innetwork
   ,rec_candidate.visibilityfilter
   ,rec_candidate.nhdplusid
   ,rec_candidate.vpuid
   ,rec_candidate.enabled
   ,rec_candidate.fmeasure
   ,rec_candidate.tmeasure
   ,rec_candidate.hydroseq
   ,rec_candidate.shape
   ,rec_candidate.snap_measure
   ,rec_candidate.snap_distancekm
   ,rec_candidate.snap_point
   USING
    sdo_input
   ,sdo_input
   ,int_nhdplusid;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Generate snap path if requested
   --------------------------------------------------------------------------
   out_flowlines[1]     := rec_candidate;
   out_path_distance_km := out_flowlines[1].snap_distancekm;
   out_end_point        := out_flowlines[1].snap_point;
   out_nhdplusid        := out_flowlines[1].nhdplusid;
   out_reachcode        := out_flowlines[1].reachcode;
   out_hydroseq         := out_flowlines[1].hydroseq;
   out_snap_measure     := out_flowlines[1].snap_measure;
   
   IF p_return_link_path
   AND out_path_distance_km > 0.00005
   THEN
      out_indexing_line := public.ST_MAKELINE(
          public.ST_TRANSFORM(sdo_input,4269)
         ,out_end_point
      );
  
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 90
   -- Check for problems and mismatches
   --------------------------------------------------------------------------
   IF out_nhdplusid IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'Error matching catchment to flowline <<' || int_nhdplusid::VARCHAR || '>>';
      RETURN;
      
   END IF;


END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.catconstrained_index';
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

