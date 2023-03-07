CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_single(
    IN  str_search_type           VARCHAR
   ,IN  obj_start_flowline        cipsrv_nhdplus_h.flowline
   ,IN  obj_stop_flowline         cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distance_km   NUMERIC
   ,IN  num_maximum_flowtime_day  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   
   num_init_meas_total      NUMERIC;
   num_init_fmeasure        NUMERIC;
   num_init_tmeasure        NUMERIC;
   num_init_lengthkm        NUMERIC;
   num_init_flowtimeday     NUMERIC;
   int_navtermination_flag  INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Calculate the single flowline navigation
   ----------------------------------------------------------------------------
   IF obj_start_flowline.comid = obj_stop_flowline.comid
   THEN
      num_init_meas_total  := ABS(obj_stop_flowline.out_measure - obj_start_flowline.out_measure);
      num_init_lengthkm    := num_init_meas_total * obj_start_flowline.lengthkm_ratio;
      num_init_flowtimeday := num_init_meas_total * obj_start_flowline.flowtimeday_ratio;

      IF obj_start_flowline.out_measure < obj_stop_flowline.out_measure
      THEN
         num_init_fmeasure := obj_start_flowline.out_measure;
         num_init_tmeasure := obj_stop_flowline.out_measure;

      ELSE
         num_init_fmeasure := obj_stop_flowline.out_measure;
         num_init_tmeasure := obj_start_flowline.out_measure;

      END IF;
      
   ELSIF num_maximum_distance_km < obj_start_flowline.out_lengthkm
   THEN
      IF str_search_type IN ('UM','UT')
      THEN
         num_init_fmeasure := obj_start_flowline.out_measure;
         num_init_tmeasure := obj_start_flowline.out_measure + ROUND(num_maximum_distance_km / obj_start_flowline.lengthkm_ratio,5);
         
      ELSE
         num_init_fmeasure := obj_start_flowline.out_measure - ROUND(num_maximum_distance_km / obj_start_flowline.lengthkm_ratio,5);
         num_init_tmeasure := obj_start_flowline.out_measure;

      END IF;

      num_init_lengthkm    := num_maximum_distance_km;
      num_init_flowtimeday := (num_init_tmeasure - num_init_fmeasure) * obj_start_flowline.flowtimeday_ratio;

   ELSIF num_maximum_flowtime_day < obj_start_flowline.out_flowtimeday
   THEN
      IF str_search_type IN ('UM','UT')
      THEN
         num_init_fmeasure := obj_start_flowline.out_measure;
         num_init_tmeasure := obj_start_flowline.out_measure + ROUND(num_maximum_flowtime_day / obj_start_flowline.flowtimeday_ratio,5);
         
      ELSE
         num_init_fmeasure := obj_start_flowline.out_measure - ROUND(num_maximum_flowtime_day / obj_start_flowline.flowtimeday_ratio,5);
         num_init_tmeasure := obj_start_flowline.out_measure;

      END IF;

      num_init_lengthkm    := (num_init_tmeasure - num_init_fmeasure) * obj_start_flowline.lengthkm_ratio;
      num_init_flowtimeday := num_maximum_flowtime_day;

   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Test just in case the results are exactly the same as original flowline
   ----------------------------------------------------------------------------
   IF obj_start_flowline.tmeasure - obj_start_flowline.fmeasure = num_init_tmeasure - num_init_fmeasure
   THEN
      int_navtermination_flag := 1;
      
   ELSE
      int_navtermination_flag := 2;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Insert the results
   ----------------------------------------------------------------------------
   INSERT INTO tmp_navigation_working30(
       nhdplusid
      ,hydroseq
      ,fmeasure
      ,tmeasure
      ,lengthkm
      ,flowtimeday
      ,network_distancekm
      ,network_flowtimeday
      ,levelpathi
      ,terminalpa
      ,uphydroseq
      ,dnhydroseq
      ,navtermination_flag
      ,nav_order
      ,selected
   ) VALUES (
       obj_start_flowline.nhdplusid
      ,obj_start_flowline.hydroseq
      ,num_init_fmeasure
      ,num_init_tmeasure
      ,num_init_lengthkm
      ,num_init_flowtimeday
      ,num_init_lengthkm
      ,num_init_flowtimeday
      ,obj_start_flowline.levelpathi
      ,obj_start_flowline.terminalpa
      ,obj_start_flowline.uphydroseq
      ,obj_start_flowline.dnhydroseq
      ,int_navtermination_flag
      ,0
      ,TRUE
   );

   ----------------------------------------------------------------------------
   -- Step 40
   -- Insert the initial flowline and tag the running counts
   ----------------------------------------------------------------------------
   RETURN 1;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.nav_single(
    VARCHAR
   ,cipsrv_nhdplus_h.flowline
   ,cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.nav_single(
    VARCHAR
   ,cipsrv_nhdplus_h.flowline
   ,cipsrv_nhdplus_h.flowline
   ,NUMERIC
   ,NUMERIC
)  TO PUBLIC;

