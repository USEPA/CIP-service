DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.catconstrained_index';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.catconstrained_index(
    IN  p_point                     GEOMETRY
   ,IN  p_return_link_path          BOOLEAN
   ,IN  p_known_region              VARCHAR
   ,IN  p_known_catchment_nhdplusid BIGINT DEFAULT NULL
   ,OUT out_flowlines               cipsrv_nhdplus_m.snapflowline[]
   ,OUT out_path_distance_km        NUMERIC
   ,OUT out_end_point               GEOMETRY
   ,OUT out_indexing_line           GEOMETRY
   ,OUT out_region                  VARCHAR
   ,OUT out_nhdplusid               BIGINT
   ,OUT out_reachcode               VARCHAR
   ,OUT out_snap_measure            NUMERIC
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                     RECORD;
   boo_return_link_path    BOOLEAN  := p_return_link_path;
   int_raster_srid         INTEGER;
   sdo_input               GEOMETRY;
   rec_candidate           cipsrv_nhdplus_m.snapflowline;
   int_nhdplusid           BIGINT;
   boo_issink              BOOLEAN;
   boo_isocean             BOOLEAN;
   boo_isalaskan           BOOLEAN;
   
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
   
   IF boo_return_link_path IS NULL
   THEN
      boo_return_link_path := FALSE;
      
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
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          int_nhdplusid
         ,boo_issink
         ,boo_isocean
         ,boo_isalaskan
         FROM
         cipsrv_nhdplus_m.catchment_5070 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSIF int_raster_srid = 3338
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          int_nhdplusid
         ,boo_issink
         ,boo_isocean
         ,boo_isalaskan
         FROM
         cipsrv_nhdplus_m.catchment_3338 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSIF int_raster_srid = 26904
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          int_nhdplusid
         ,boo_issink
         ,boo_isocean
         ,boo_isalaskan
         FROM
         cipsrv_nhdplus_m.catchment_26904 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSIF int_raster_srid = 32161
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          int_nhdplusid
         ,boo_issink
         ,boo_isocean
         ,boo_isalaskan
         FROM
         cipsrv_nhdplus_m.catchment_32161 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSIF int_raster_srid = 32655
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
          int_nhdplusid
         ,boo_issink
         ,boo_isocean
         ,boo_isalaskan
         FROM
         cipsrv_nhdplus_m.catchment_32655 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSIF int_raster_srid = 32702
      THEN
         SELECT
          a.nhdplusid
         ,a.issink
         ,a.isocean
         ,a.isalaskan
         INTO 
         int_nhdplusid
         FROM
         cipsrv_nhdplus_m.catchment_32702 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_input
         )
         LIMIT 1;
         
      ELSE
         RAISE EXCEPTION 'err %',int_raster_srid;
      
      END IF;
   
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
      out_return_code    := -3;
      out_status_message := 'catchment without flowline for indexing';
      out_nhdplusid      := int_nhdplusid;
      RETURN;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 60
   -- Pull the matching flowline
   --------------------------------------------------------------------------
   IF int_raster_srid = 5070
   THEN
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
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
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
         ,NULL
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_5070 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
   
   ELSIF int_raster_srid = 3338
   THEN
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
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
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
         ,NULL
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_3338 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
   
   ELSIF int_raster_srid = 26904
   THEN
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
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
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
         ,NULL
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_26904 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32161
   THEN
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
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
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
         ,NULL
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_32161 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32655
   THEN
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
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
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
         ,NULL
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_32655 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32702
   THEN
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
      ,ST_Transform(ST_Force2D(
          ST_GeometryN(
             ST_LocateAlong(
                 a.shape
                ,a.snap_measure
             )
            ,1
          )
       ),4269) AS snap_point
      INTO
       rec_candidate.permanent_identifier
      ,rec_candidate.fdate
      ,rec_candidate.resolution
      ,rec_candidate.gnis_id
      ,rec_candidate.gnis_name
      ,rec_candidate.lengthkm
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
         ,NULL
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.hydroseq
         ,aa.shape
         ,ST_InterpolatePoint(
              aa.shape
             ,sdo_input
          ) AS snap_measure
         ,ST_Distance(
              ST_Transform(aa.shape,4326)::GEOGRAPHY
             ,ST_Transform(sdo_input,4326)::GEOGRAPHY
          ) / 1000 AS snap_distancekm
         FROM
         cipsrv_nhdplus_m.nhdflowline_32702 aa
         WHERE
         aa.nhdplusid = int_nhdplusid
      ) a;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Generate snap path if requested
   --------------------------------------------------------------------------
   out_flowlines[1]     := rec_candidate;
   out_path_distance_km := out_flowlines[1].snap_distancekm;
   out_end_point        := out_flowlines[1].snap_point;
   out_nhdplusid        := out_flowlines[1].nhdplusid;
   out_reachcode        := out_flowlines[1].reachcode;
   out_snap_measure     := out_flowlines[1].snap_measure;
   
   IF p_return_link_path
   AND out_path_distance_km > 0.00005
   THEN
      out_indexing_line := ST_MakeLine(
          ST_Transform(sdo_input,4269)
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

ALTER FUNCTION cipsrv_nhdplus_m.catconstrained_index(
    GEOMETRY
   ,BOOLEAN
   ,VARCHAR
   ,BIGINT
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.catconstrained_index(
    GEOMETRY
   ,BOOLEAN
   ,VARCHAR
   ,BIGINT
) TO PUBLIC;
