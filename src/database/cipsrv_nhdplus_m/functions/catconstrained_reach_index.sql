CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.catconstrained_reach_index(
    IN  p_geometry               GEOMETRY
   ,IN  p_catchment_nhdplusid    NUMERIC
   ,IN  p_known_region           VARCHAR
   ,OUT out_permanent_identifier VARCHAR
   ,OUT out_nhdplusid            NUMERIC
   ,OUT out_fdate                DATE
   ,OUT out_resolution           INTEGER
   ,OUT out_reachcode            VARCHAR
   ,OUT out_flowdir              INTEGER
   ,OUT out_gnis_id              VARCHAR
   ,OUT out_gnis_name            VARCHAR
   ,OUT out_wbarea_permanent_identifier VARCHAR
   ,OUT out_ftype                INTEGER
   ,OUT out_fcode                INTEGER
   ,OUT out_vpuid                VARCHAR
   ,OUT out_snap_measure         NUMERIC
   ,OUT out_snap_distancekm      NUMERIC
   ,OUT out_snap_point           GEOMETRY
   ,OUT out_return_code          INTEGER
   ,OUT out_status_message       VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                  RECORD;
   int_raster_srid      INTEGER;
   sdo_input            GEOMETRY;
   
BEGIN

   out_return_code := 0;
   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters and set parameters
   --------------------------------------------------------------------------
   IF p_geometry IS NULL
   OR p_catchment_nhdplusid IS NULL
   THEN
      RAISE EXCEPTION 'point and catchment nhdplusid required.';
      
   END IF;
   
   IF ST_GeometryType(p_geometry) <> 'ST_Point'
   THEN
      out_return_code    := -99;
      out_status_message := 'Input must be point geometry';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the projection
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_geometry
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
   IF ST_SRID(p_geometry) = int_raster_srid
   THEN
      sdo_input := p_geometry;
      
   ELSE
      sdo_input := ST_Transform(p_geometry,int_raster_srid);
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Pull the matching flowline
   --------------------------------------------------------------------------
   IF int_raster_srid = 5070
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
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
         ,aa.vpuid
         ,aa.fmeasure
         ,aa.tmeasure
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
         aa.nhdplusid = p_catchment_nhdplusid
      ) a;
   
   ELSIF int_raster_srid = 26904
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
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
         ,aa.vpuid
         ,aa.fmeasure
         ,aa.tmeasure
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
         aa.nhdplusid = p_catchment_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32161
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
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
         ,aa.vpuid
         ,aa.fmeasure
         ,aa.tmeasure
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
         aa.nhdplusid = p_catchment_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32655
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
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
         ,aa.vpuid
         ,aa.fmeasure
         ,aa.tmeasure
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
         aa.nhdplusid = p_catchment_nhdplusid
      ) a;
      
   ELSIF int_raster_srid = 32702
   THEN
      SELECT 
       a.permanent_identifier
      ,a.nhdplusid
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.vpuid
      ,ROUND(
           a.snap_measure::numeric
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
       out_permanent_identifier
      ,out_nhdplusid
      ,out_fdate
      ,out_resolution
      ,out_gnis_id
      ,out_gnis_name
      ,out_reachcode
      ,out_flowdir
      ,out_wbarea_permanent_identifier
      ,out_ftype
      ,out_fcode
      ,out_vpuid
      ,out_snap_measure
      ,out_snap_distancekm
      ,out_snap_point
      FROM (
         SELECT 
          aa.permanent_identifier
         ,aa.nhdplusid
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
         ,aa.vpuid
         ,aa.fmeasure
         ,aa.tmeasure
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
         aa.nhdplusid = p_catchment_nhdplusid
      ) a;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Check for problems and mismatches
   --------------------------------------------------------------------------
   IF out_nhdplusid IS NULL
   OR out_permanent_identifier IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'Error matching catchment to flowline <<' || p_catchment_nhdplusid::VARCHAR || '>>';
      RETURN;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.catconstrained_reach_index(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.catconstrained_reach_index(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
) TO PUBLIC;

