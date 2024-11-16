DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.delineate';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.delineate(
    IN  p_search_type                  VARCHAR
   ,IN  p_start_nhdplusid              BIGINT
   ,IN  p_start_permanent_identifier   VARCHAR
   ,IN  p_start_reachcode              VARCHAR
   ,IN  p_start_hydroseq               BIGINT
   ,IN  p_start_measure                NUMERIC
   ,IN  p_stop_nhdplusid               BIGINT
   ,IN  p_stop_permanent_identifier    VARCHAR
   ,IN  p_stop_reachcode               VARCHAR
   ,IN  p_stop_hydroseq                BIGINT
   ,IN  p_stop_measure                 NUMERIC
   ,IN  p_max_distancekm               NUMERIC
   ,IN  p_max_flowtimeday              NUMERIC
   ,IN  p_aggregation_engine           VARCHAR
   ,IN  p_split_initial_catchment      BOOLEAN
   ,IN  p_fill_basin_holes             BOOLEAN
   ,IN  p_force_no_cache               BOOLEAN
   ,IN  p_return_delineation_geometry  BOOLEAN
   ,IN  p_return_flowlines             BOOLEAN   
   ,IN  p_return_flowline_details      BOOLEAN
   ,IN  p_return_flowline_geometry     BOOLEAN
   
   ,OUT out_aggregation_used           VARCHAR
   ,OUT out_return_flowlines           BOOLEAN
   ,OUT out_start_nhdplusid            BIGINT
   ,OUT out_start_permanent_identifier VARCHAR
   ,OUT out_start_measure              NUMERIC
   ,OUT out_grid_srid                  INTEGER
   ,OUT out_stop_nhdplusid             BIGINT
   ,OUT out_stop_measure               NUMERIC
   ,OUT out_flowline_count             INTEGER
   ,OUT out_return_code                NUMERIC
   ,OUT out_status_message             VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                             RECORD;
   str_search_type                 VARCHAR(16) := UPPER(p_search_type); 
   obj_start_flowline              cipsrv_nhdplus_h.flowline;
   obj_stop_flowline               cipsrv_nhdplus_h.flowline;
   num_maximum_distancekm          NUMERIC     := p_max_distancekm;
   num_maximum_flowtimeday         NUMERIC     := p_max_flowtimeday;
   int_counter                     INTEGER;
   int_check                       INTEGER;
   int_grid_srid                   INTEGER;
   int_start_nhdplusid             BIGINT;
   str_start_permanent_identifier  VARCHAR(40);
   num_start_measure               NUMERIC;
   int_stop_nhdplusid              BIGINT;
   num_stop_measure                NUMERIC;
   int_flowlines                   INTEGER;
   int_catchments                  INTEGER;
   sdo_splitpoint                  GEOMETRY;
   sdo_split_catchment             GEOMETRY;
   sdo_topo_output                 GEOMETRY;
   sdo_output                      GEOMETRY;
   num_area                        NUMERIC;
   str_aggregation_engine          VARCHAR := UPPER(p_aggregation_engine);
   boo_aggregation_flag            BOOLEAN;
   boo_topology_flag               BOOLEAN;
   boo_split_initial_catchment     BOOLEAN;
   boo_fill_basin_holes            BOOLEAN;
   boo_return_delineation_geometry BOOLEAN;
   boo_return_flowlines            BOOLEAN;
   boo_return_flowline_details     BOOLEAN;
   boo_return_flowline_geometry    BOOLEAN;
   boo_zero_length_delin           BOOLEAN;   
   boo_force_no_cache              BOOLEAN;
   boo_cached_watershed            BOOLEAN;

BEGIN

   out_return_code := cipsrv_engine.create_delineation_temp_tables();

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   out_return_code    := 0;
   obj_start_flowline := NULL::cipsrv_nhdplus_h.flowline;
   obj_stop_flowline  := NULL::cipsrv_nhdplus_h.flowline;
   
   IF str_search_type IN ('PP','POINT TO POINT','POINT-TO-POINT')
   THEN
      str_search_type := 'PP';
      
   ELSIF str_search_type IN ('PPALL')
   THEN
      str_search_type := 'PPALL';
   
   ELSIF str_search_type IN ('UT','UPSTREAM WITH TRIBUTARIES')
   THEN
      str_search_type := 'UT';
   
   ELSIF str_search_type IN ('UM','UPSTREAM MAIN PATH ONLY')
   THEN
      str_search_type := 'UM';
   
   ELSIF str_search_type IN ('DD','DOWNSTREAM WITH DIVERGENCES')
   THEN
      str_search_type := 'DD';
   
   ELSIF str_search_type IN ('DM','DOWNSTREAM MAIN PATH ONLY')
   THEN
      str_search_type := 'DM';
      
   ELSE
      out_return_code    := -1;
      out_status_message := 'Valid SearchType codes are UM, UT, DM, DD and PP.';

   END IF;

   IF str_search_type IN ('PP','PPALL')
   THEN
      num_maximum_distancekm  := NULL;
      num_maximum_flowtimeday := NULL;

   END IF;

   IF  num_maximum_distancekm  IS NOT NULL
   AND num_maximum_flowtimeday IS NOT NULL
   THEN
      num_maximum_flowtimeday := NULL;

   END IF;
   
   IF str_aggregation_engine IN ('SPATIAL')
   THEN
      str_aggregation_engine := 'SPATIAL';
      boo_aggregation_flag   := TRUE;
      boo_topology_flag      := FALSE;
      
   ELSIF str_aggregation_engine IN ('TOPO')
   THEN
      str_aggregation_engine := 'TOPO';
      boo_aggregation_flag   := TRUE;
      boo_topology_flag      := TRUE;
   
   ELSIF str_aggregation_engine IN ('NONE')
   THEN
      str_aggregation_engine := 'NONE';
      boo_aggregation_flag   := FALSE;
      boo_topology_flag      := FALSE;
   
   ELSE
      str_aggregation_engine := 'TOPO';
      boo_aggregation_flag   := TRUE;
      boo_topology_flag      := TRUE;
      
   END IF;
   
   boo_split_initial_catchment := p_split_initial_catchment;
   IF boo_split_initial_catchment IS NULL
   THEN
      boo_split_initial_catchment := TRUE;
      
   END IF;
   
   boo_fill_basin_holes := p_fill_basin_holes;
   IF boo_fill_basin_holes IS NULL
   THEN
      boo_fill_basin_holes := TRUE;
      
   END IF;
   
   boo_force_no_cache := p_force_no_cache;
   IF boo_force_no_cache IS NULL
   THEN
      boo_force_no_cache := FALSE;
      
   END IF;
   
   boo_return_delineation_geometry := p_return_delineation_geometry;
   IF boo_return_delineation_geometry IS NULL
   THEN
      boo_return_delineation_geometry := TRUE;
      
   END IF;
   
   boo_return_flowlines := p_return_flowlines;
   IF boo_return_flowlines IS NULL
   THEN
      boo_return_flowlines := FALSE;
      
   END IF;
   
   boo_return_flowline_details := p_return_flowline_details;
   IF boo_return_flowline_details IS NULL
   THEN
      boo_return_flowline_details := TRUE;
      
   END IF;
   
   boo_return_flowline_geometry := p_return_flowline_geometry;
   IF boo_return_flowline_geometry IS NULL
   THEN
      boo_return_flowline_geometry := TRUE;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine special conditions
   ----------------------------------------------------------------------------
   IF  num_maximum_distancekm IS NULL
   AND num_maximum_flowtimeday IS NULL
   AND NOT boo_split_initial_catchment
   AND NOT boo_return_flowlines
   AND str_aggregation_engine != 'NONE'
   THEN
      boo_cached_watershed := TRUE;
      
   ELSIF num_maximum_distancekm  = 0
   OR    num_maximum_flowtimeday = 0
   THEN
      boo_zero_length_delin := TRUE;
      
   END IF;
   
   IF boo_force_no_cache
   THEN
      boo_cached_watershed := FALSE;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- For cached watershed, verify that nhdplusid matches actual catchment
   ----------------------------------------------------------------------------
   IF boo_cached_watershed
   OR boo_zero_length_delin
   THEN
      rec := cipsrv_nhdplus_h.get_flowline(
          p_direction            := str_search_type
         ,p_nhdplusid            := p_start_nhdplusid
         ,p_permanent_identifier := p_start_permanent_identifier
         ,p_reachcode            := p_start_reachcode
         ,p_hydroseq             := p_start_hydroseq
         ,p_measure              := p_start_measure
      );
      out_return_code        := rec.out_return_code;
      out_status_message     := rec.out_status_message;
      
      IF out_return_code <> 0
      THEN
         IF out_return_code = -10
         THEN
            out_status_message := 'Flowline ' || COALESCE(
                p_start_nhdplusid::VARCHAR
               ,p_start_permanent_identifier
               ,p_start_reachcode
               ,p_start_hydroseq::VARCHAR
               ,'err'
            );
            
            IF p_start_measure IS NOT NULL
            THEN
               out_status_message := out_status_message || ' at measure ' || p_start_measure::VARCHAR;
               
            END IF;
            
            out_status_message := out_status_message || ' not found in NHDPlus stream network.';
            
         END IF;
         
         RETURN;
         
      END IF;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Run the Navigation Service
   ----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   THEN
      rec := cipsrv_nhdplus_h.navigate(
          p_search_type                := str_search_type
         ,p_start_nhdplusid            := p_start_nhdplusid
         ,p_start_permanent_identifier := p_start_permanent_identifier
         ,p_start_reachcode            := p_start_reachcode
         ,p_start_hydroseq             := p_start_hydroseq
         ,p_start_measure              := p_start_measure
         ,p_stop_nhdplusid             := p_stop_nhdplusid
         ,p_stop_permanent_identifier  := p_stop_permanent_identifier
         ,p_stop_reachcode             := p_stop_reachcode
         ,p_stop_hydroseq              := p_stop_hydroseq
         ,p_stop_measure               := p_stop_measure
         ,p_max_distancekm             := num_maximum_distancekm
         ,p_max_flowtimeday            := num_maximum_flowtimeday
      );
      
      out_return_code                := rec.out_return_code;
      out_status_message             := rec.out_status_message;
      int_start_nhdplusid            := rec.out_start_nhdplusid;
      num_start_measure              := rec.out_start_measure;
      str_start_permanent_identifier := rec.out_start_permanent_identifier;
      int_grid_srid                  := rec.out_grid_srid;   
      int_flowlines                  := rec.out_flowline_count;
      int_stop_nhdplusid                 := rec.out_stop_nhdplusid;
      num_stop_measure               := rec.out_stop_measure;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Examine the navigation results
   ----------------------------------------------------------------------------
   IF  out_return_code = -56600
   AND str_search_type <> 'PP'
   THEN
       -- When directional navigation hits the coastal response, flip to zero length
      out_return_code := 0;
      out_status_message := NULL;
      boo_zero_length_delin := TRUE;
      
      rec := cipsrv_nhdplus_h.get_flowline(
          p_direction            := str_search_type
         ,p_nhdplusid            := p_start_nhdplusid
         ,p_permanent_identifier := p_start_permanent_identifier
         ,p_reachcode            := p_start_reachcode
         ,p_hydroseq             := p_start_hydroseq
         ,p_measure              := p_start_measure
      );
      out_return_code        := rec.out_return_code;
      out_status_message     := rec.out_status_message;
      
      IF out_return_code <> 0
      THEN
         IF out_return_code = -10
         THEN
            out_status_message := 'Flowline ' || COALESCE(
                p_start_nhdplusid::VARCHAR
               ,p_start_permanent_identifier
               ,p_start_reachcode
               ,p_start_hydroseq::VARCHAR
               ,'err'
            );
            
            IF p_start_measure IS NOT NULL
            THEN
               out_status_message := out_status_message || ' at measure ' || p_start_measure::VARCHAR;
               
            END IF;
            
            out_status_message := out_status_message || ' not found in NHDPlus stream network.';
            
         END IF;
         
         RETURN;
         
      END IF;
      
   END IF;
   
   IF out_return_code <> 0
   THEN
      IF out_return_code = 302
      THEN
         -- Message is per Tommy Dewald.  Do not change without checking with Tommy first
         out_status_message := 'The selected flowline (stream segment) is not connected to the NHDPlus stream network';
         
      END IF;
      
      RETURN;
      
   END IF;

   -----------------------------------------------------------------------------
   -- Step 70
   -- Run Catchment Delineation Preprocessing 
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   AND NOT boo_return_flowlines
   THEN
      rec := cipsrv_nhdplus_h.delineation_preprocessing(
          p_aggregation_flag             := boo_aggregation_flag
         ,p_known_grid                   := int_grid_srid::VARCHAR
         ,p_return_delineation_geometry  := boo_return_delineation_geometry
         ,p_return_topology_results      := FALSE
         ,p_extra_catchments             := NULL
      );
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      int_catchments     := rec.out_records_inserted;

      IF out_return_code <> 0
      THEN
         RETURN;
      
      END IF;
      
   END IF;

   -----------------------------------------------------------------------------
   -- Step 80
   -- Fetch the split catchment geometry if required
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   AND boo_split_catchment
   THEN
      IF str_search_type <> 'UT'
      THEN
         out_return_code := -700;
         out_status_message := 'Catchment split only supported for upstream with tributary navigation';
         
         RETURN;
         
      END IF;
   
      -- This might need to be adjusted to be a bit further downstream, dunno
      sdo_splitpoint := cipsrv_nhdplus_h.point_at_measure(
          p_nhdplusid            := int_start_nhdplusid
         ,p_permanent_identifier := NULL
         ,p_reachcode            := NULL 
         ,p_measure              := num_start_measure
         ,p_2d_flag              := TRUE
      );

      sdo_split_catchment := cipsrv_nhdplus_h.split_catchment(
          p_split_point  := ST_Transform(sdo_splitpoint,int_grid_srid)
         ,p_nhdplusid    := int_start_nhdplusid
         ,p_known_region := int_grid_srid::VARCHAR
      );
      --raise warning '% %',int_catchments, st_astext(st_transform(st_collect(sdo_splitpoint,NULL),4269));
      
      IF sdo_split_catchment IS NULL
      THEN
         out_return_code := -702;
         out_status_message := 'Catchment split process failed';
         
         RETURN;
         
      END IF;
   
   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 90
   -- Swap in the split catchment for catchment and geometric aggregation
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   AND boo_split_catchment
   THEN
      IF boo_topology_flag
      THEN
         DELETE FROM tmp_catchments a
         WHERE
         a.nhdplusid  = int_start_nhdplusid;
         
      ELSE
         IF int_grid_srid = 5070
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_5070  = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = int_start_nhdplusid;
            
         ELSIF int_grid_srid = 3338
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_3338  = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = int_start_nhdplusid;
            
         ELSIF int_grid_srid = 26904
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_26904 = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = int_start_nhdplusid;
            
         ELSIF int_grid_srid = 32161
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_32161 = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = int_start_nhdplusid;
            
         ELSIF int_grid_srid = 32655
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_32655 = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = int_start_nhdplusid;
            
         ELSIF int_grid_srid = 32702
         THEN
            UPDATE tmp_catchments a
            SET
             shape       = ST_Transform(sdo_split_catchment,4269)
            ,shape_32702 = sdo_split_catchment
            ,areasqkm    = ROUND(ST_Area(sdo_split_catchment)::NUMERIC * 0.000001,5)
            WHERE
            a.nhdplusid = int_start_nhdplusid;
            
         END IF;

      END IF;
      
   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 100
   -- Branch based on aggregation decision - return aggregated catchment poly
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   THEN
      IF boo_topology_flag
      THEN
         rec := cipsrv_nhdplus_h.load_topo_catchment(
             p_grid_srid       := int_grid_srid
            ,p_catchment_count := int_catchments
         );
         out_return_code     := rec.out_return_code;
         out_status_message  := rec.out_status_message;
         sdo_topo_output     := rec.out_geometry;
      
         IF out_return_code <> 0
         THEN
            RETURN;
         
         END IF;
         
      ELSE
         rec := cipsrv_nhdplus_h.load_aggregated_catchment(
             p_grid_srid       := int_grid_srid
         );
         out_return_code     := rec.out_return_code;
         out_status_message  := rec.out_status_message;
      
         IF out_return_code <> 0
         THEN
            RETURN;
         
         END IF;         
         
      END IF;
   
   END IF;
   
    -----------------------------------------------------------------------------
   -- Step 110
   -- Add split catchment to topo aggregation if requested
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   AND boo_split_catchment
   AND boo_topology_flag
   THEN
      SELECT
      ST_Union(ST_SnaptoGrid(a.geom,0.05))
      INTO sdo_topo_output
      FROM (
         SELECT 
         (ST_Dump(
            ST_Collect(sdo_topo_output,ST_Buffer(sdo_split_catchment,0.01))
         )).*
      ) a;      
      
      UPDATE tmp_catchments a
      SET
       shape    = ST_Transform(sdo_topo_output,4269)
      ,areasqkm = ROUND(ST_Area(sdo_topo_output)::NUMERIC * 0.000001,5)
      WHERE
      a.nhdplusid  = -9999999;

   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 120
   -- Migrate catchment geometry if requested
   -----------------------------------------------------------------------------
   IF  NOT boo_cached_watershed
   AND NOT boo_zero_length_delin
   AND boo_aggregation_flag
   THEN
      IF int_grid_srid = 5070
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_5070,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      ELSIF int_grid_srid = 3338
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_3338,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      ELSIF int_grid_srid = 26904
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_26904,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      ELSIF int_grid_srid = 32161
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_32161,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      ELSIF int_grid_srid = 32655
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_32655,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      ELSIF int_grid_srid = 32702
      THEN
         UPDATE tmp_catchments a
         SET
         shape = ST_Transform(shape_32702,4269)
         WHERE
         a.sourcefc <> 'AGGR';
         
      END IF;
   
   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 130
   -- Fetch cached watershed if requested
   -----------------------------------------------------------------------------
   IF boo_cached_watershed
   THEN
      INSERT INTO tmp_catchments(
          nhdplusid
         ,sourcefc
         ,gridcode
         ,areasqkm
         ,shape
      )
      SELECT
       -9999999
      ,'AGGR'
      ,-9999
      ,a.areasqkm
      ,a.shape
      FROM
      cipsrv_nhdpluswshed_m.catchment_watershed a
      WHERE
      a.nhdplusid = int_catchment_nhdplusid;

   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 140
   -- Fetch zero distance catchment
   -----------------------------------------------------------------------------
   IF boo_zero_length_delin
   THEN
      IF boo_return_flowlines
      THEN
         INSERT INTO tmp_navigation_results(
             nhdplusid
            ,hydroseq
            ,fmeasure
            ,tmeasure
            ,levelpathi
            ,terminalpa
            ,uphydroseq
            ,dnhydroseq
            ,lengthkm
            ,flowtimeday
            /* ++++++++++ */
            ,network_distancekm
            ,network_flowtimeday
            /* ++++++++++ */
            ,permanent_identifier
            ,reachcode
            ,fcode
            ,gnis_id
            ,gnis_name
            ,wbarea_permanent_identifier
            /* ++++++++++ */
            ,navtermination_flag
            ,shape
            ,nav_order
         )
         SELECT
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,a.lengthkm
         ,a.flowtimeday
         /* ++++++++++ */
         ,a.lengthkm    AS network_distancekm
         ,a.flowtimeday AS network_flowtimeday
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         ,a.gnis_id
         ,a.gnis_name
         ,a.wbarea_permanent_identifier
         /* ++++++++++ */
         ,NULL
         ,a.shape
         ,1
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_h.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE
         a.nhdplusid = int_start_nhdplusid;
         
      END IF;
      
      IF boo_split_catchment
      THEN
         sdo_splitpoint := cipsrv_nhdplus_h.point_at_measure(
             p_nhdplusid             := int_start_nhdplusid
            ,p_permanent_identifier  := NULL
            ,p_reachcode             := NULL 
            ,p_measure               := num_start_measure
            ,p_2d_flag               := 'TRUE'
         );

         sdo_split_catchment := cipsrv_nhdplus_h.split_catchment(
             p_split_point  := ST_Transform(sdo_splitpoint,int_grid_srid)
            ,p_nhdplusid    := int_catchment_nhdplusid
            ,p_known_region := int_grid_srid::VARCHAR
         );

         IF sdo_split_catchment IS NULL
         THEN
            out_return_code := -702;
            out_status_message := 'Catchment split process failed';
            
            RETURN;
            
         END IF;
         
         num_area := ROUND(
             ST_Area(ST_Transform(sdo_split_catchment,4326)::GEOGRAPHY)::NUMERIC * 0.000001
            ,5
         );
         
         sdo_output := ST_Transform(sdo_split_catchment,4269);
         
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,gridcode
            ,areasqkm
            ,shape
         ) VALUES (
             -9999999
            ,'AGGR'
            ,-9999
            ,num_area
            ,sdo_output
         );
  
      END IF;

   END IF;
   
   -----------------------------------------------------------------------------
   -- Step 150
   -- Remove holes if requested
   -----------------------------------------------------------------------------
   IF boo_fill_holes
   THEN
      UPDATE tmp_catchments a
      SET
      shape = cipsrv_engine.remove_holes(a.shape)
      WHERE
      a.sourcefc = 'AGGR';
      
      UPDATE tmp_catchments a
      SET
      areasqkm = ROUND(ST_Area(ST_Transform(a.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001,5)
      WHERE
      a.sourcefc = 'AGGR';

   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.delineate(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,VARCHAR
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.delineate(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,VARCHAR
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
)  TO PUBLIC;

