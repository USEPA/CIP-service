DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.navigate';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.navigate(
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
   ,IN  p_return_flowline_details      BOOLEAN
   ,IN  p_return_flowline_geometry     BOOLEAN
   ,IN  p_known_region                 VARCHAR DEFAULT NULL
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
   rec                            RECORD;
   str_search_type                VARCHAR(16) := UPPER(p_search_type); 
   obj_start_flowline             cipsrv_nhdplus_m.flowline;
   obj_stop_flowline              cipsrv_nhdplus_m.flowline;
   num_maximum_distancekm         NUMERIC     := p_max_distancekm;
   num_maximum_flowtimeday        NUMERIC     := p_max_flowtimeday;
   int_counter                    INTEGER;
   int_check                      INTEGER;
   boo_return_flowline_details    BOOLEAN;
   boo_return_flowline_geometry   BOOLEAN;

BEGIN

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
   
   IF num_maximum_distancekm = 0
   OR num_maximum_flowtimeday = 0
   THEN
      out_return_code    := -3;
      out_status_message := 'navigation for zero distance or flowtime is not valid.';
   
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
   -- Flush or create the temp tables
   ----------------------------------------------------------------------------
   int_check := cipsrv_engine.create_navigation_temp_tables();

   ----------------------------------------------------------------------------
   -- Step 40
   -- Get the start flowline
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.get_flowline(
       p_direction            := str_search_type
      ,p_nhdplusid            := p_start_nhdplusid
      ,p_permanent_identifier := p_start_permanent_identifier
      ,p_reachcode            := p_start_reachcode
      ,p_hydroseq             := p_start_hydroseq
      ,p_measure              := p_start_measure
      ,p_known_region         := p_known_region
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

   IF rec.out_flowline IS NULL
   THEN
      RAISE EXCEPTION 'start get flowline returned no results';
   
   END IF;
   
   obj_start_flowline := rec.out_flowline;
   out_grid_srid := obj_start_flowline.out_grid_srid;
   
   IF obj_start_flowline.hydroseq IS NULL
   THEN
      out_return_code    := -22;
      out_status_message := 'Start flowline is not part of the NHDPlus network.';
      
      RETURN;
   
   ELSIF num_maximum_flowtimeday IS NOT NULL
   AND   obj_start_flowline.flowtimeday IS NULL
   THEN
      out_return_code    := -23;
      out_status_message := 'Start flowline is tidal without flow time information.';
      
      RETURN;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Get the stop flowline
   ----------------------------------------------------------------------------
   IF str_search_type IN ('PP','PPALL')
   THEN
      rec := cipsrv_nhdplus_m.get_flowline(
          p_direction            := 'U'
         ,p_nhdplusid            := p_stop_nhdplusid
         ,p_permanent_identifier := p_stop_permanent_identifier
         ,p_reachcode            := p_stop_reachcode
         ,p_hydroseq             := p_stop_hydroseq
         ,p_measure              := p_stop_measure
         ,p_known_region         := p_known_region
      );
      out_return_code       := rec.out_return_code;
      out_status_message    := rec.out_status_message;

      IF out_return_code <> 0
      THEN
         RETURN;
         
      END IF;
   
      IF rec.out_flowline IS NULL
      THEN
         RAISE EXCEPTION 'stop get flowline returned no results';
      
      END IF;
      
      obj_stop_flowline := rec.out_flowline;

      IF obj_stop_flowline.hydroseq IS NULL
      THEN
         out_return_code    := -22;
         out_status_message := 'Stop flowline is not part of the NHDPlus network.';
      
         RETURN;
      
      ELSIF num_maximum_flowtimeday IS NOT NULL
      AND   obj_stop_flowline.flowtimeday IS NULL
      THEN
         out_return_code    := -23;
         out_status_message := 'Stop flowline is tidal without flow time information.';
         
         RETURN;
         
      END IF;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Turn PP around if stop above start
   ----------------------------------------------------------------------------
   IF obj_stop_flowline.hydroseq > obj_start_flowline.hydroseq
   THEN
      rec := cipsrv_nhdplus_m.get_flowline(
          p_direction            := str_search_type
         ,p_nhdplusid            := p_stop_nhdplusid
         ,p_permanent_identifier := p_stop_permanent_identifier
         ,p_reachcode            := p_stop_reachcode
         ,p_hydroseq             := p_stop_hydroseq
         ,p_measure              := p_stop_measure
         ,p_known_region         := p_known_region
      );
      out_return_code        := rec.out_return_code;
      out_status_message     := rec.out_status_message;
      obj_start_flowline     := rec.out_flowline;
      
      rec := cipsrv_nhdplus_m.get_flowline(
          p_direction            := 'U'
         ,p_nhdplusid            := p_start_nhdplusid
         ,p_permanent_identifier := p_start_permanent_identifier
         ,p_reachcode            := p_start_reachcode
         ,p_hydroseq             := p_start_hydroseq
         ,p_measure              := p_start_measure
         ,p_known_region         := p_known_region
      );
      out_return_code       := rec.out_return_code;
      out_status_message    := rec.out_status_message;
      obj_stop_flowline     := rec.out_flowline;

   END IF;
   
   out_start_nhdplusid            := obj_start_flowline.nhdplusid;
   out_start_permanent_identifier := obj_start_flowline.permanent_identifier;
   out_start_measure              := obj_start_flowline.out_measure;
   out_stop_nhdplusid             := obj_stop_flowline.nhdplusid;
   out_stop_measure               := obj_stop_flowline.out_measure;

   ----------------------------------------------------------------------------
   -- Step 70
   -- Abend if start or stop is coastal
   ----------------------------------------------------------------------------
   IF obj_start_flowline.fcode = 56600
   OR obj_stop_flowline.fcode  = 56600
   THEN
      out_return_code      := -56600;
      out_status_message   := 'Navigation from or to coastal flowlines is not valid.';
      
      RETURN;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Create the initial flowline and deal with single flowline search
   ----------------------------------------------------------------------------
   IF obj_start_flowline.nhdplusid = obj_stop_flowline.nhdplusid
   OR num_maximum_distancekm < obj_start_flowline.out_lengthkm
   OR num_maximum_flowtimeday < obj_start_flowline.out_flowtimeday
   THEN
      int_counter := cipsrv_nhdplus_m.nav_single(
          str_search_type          := str_search_type
         ,obj_start_flowline       := obj_start_flowline
         ,obj_stop_flowline        := obj_stop_flowline
         ,num_maximum_distancekm   := num_maximum_distancekm
         ,num_maximum_flowtimeday  := num_maximum_flowtimeday
      );

   ELSE
   
   ----------------------------------------------------------------------------
   -- Step 90
   -- Do Point to Point
   ----------------------------------------------------------------------------
      IF str_search_type = 'PP'
      THEN
         int_counter := cipsrv_nhdplus_m.nav_pp(
             obj_start_flowline       := obj_start_flowline
            ,obj_stop_flowline        := obj_stop_flowline
         );
         
      ELSIF str_search_type = 'PPALL'
      THEN
         int_counter := cipsrv_nhdplus_m.nav_ppall(
             obj_start_flowline       := obj_start_flowline
            ,obj_stop_flowline        := obj_stop_flowline
         );
         
      ELSE
   ----------------------------------------------------------------------------
   -- Step 100
   -- Do upstream search with tributaries
   ----------------------------------------------------------------------------
         IF str_search_type = 'UT'
         THEN 
            IF (
                   num_maximum_distancekm  IS NULL
               AND num_maximum_flowtimeday IS NULL
               AND obj_start_flowline.arbolatesu > 500
            ) OR (
                   num_maximum_distancekm  IS NOT NULL
               AND num_maximum_distancekm > 200
               AND obj_start_flowline.arbolatesu > 200
            ) OR (
                   num_maximum_flowtimeday  IS NOT NULL
               AND num_maximum_flowtimeday > 3
               AND obj_start_flowline.arbolatesu > 200
            )
            THEN
               int_counter := cipsrv_nhdplus_m.nav_ut_extended(
                   obj_start_flowline      := obj_start_flowline
                  ,num_maximum_distancekm  := num_maximum_distancekm
                  ,num_maximum_flowtimeday := num_maximum_flowtimeday
               );

            ELSE   
               int_counter := cipsrv_nhdplus_m.nav_ut_concise(
                   obj_start_flowline      := obj_start_flowline
                  ,num_maximum_distancekm  := num_maximum_distancekm
                  ,num_maximum_flowtimeday := num_maximum_flowtimeday
               );

            END IF;
                 
   ----------------------------------------------------------------------------
   -- Step 110
   -- Do upstream search main line
   ----------------------------------------------------------------------------
         ELSIF str_search_type = 'UM'
         THEN
            int_counter := cipsrv_nhdplus_m.nav_um(
                obj_start_flowline      := obj_start_flowline
               ,num_maximum_distancekm  := num_maximum_distancekm
               ,num_maximum_flowtimeday := num_maximum_flowtimeday
            );

   ----------------------------------------------------------------------------
   -- Step 120
   -- Do downstream search main line
   ----------------------------------------------------------------------------
         ELSIF str_search_type = 'DM'
         THEN
            int_counter := cipsrv_nhdplus_m.nav_dm(
                obj_start_flowline      := obj_start_flowline
               ,num_maximum_distancekm  := num_maximum_distancekm
               ,num_maximum_flowtimeday := num_maximum_flowtimeday
            );

   ----------------------------------------------------------------------------
   -- Step 130
   -- Do downstream with divergences 
   -------------------------------------------------------------------
         ELSIF str_search_type = 'DD'
         THEN
            int_counter := cipsrv_nhdplus_m.nav_dd(
                obj_start_flowline      := obj_start_flowline
               ,num_maximum_distancekm  := num_maximum_distancekm
               ,num_maximum_flowtimeday := num_maximum_flowtimeday
            );

         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;

   ----------------------------------------------------------------------------
   -- Step 140
   -- Trim endings and mark partial flowline termination flags
   ----------------------------------------------------------------------------
         IF num_maximum_distancekm IS NOT NULL
         THEN
            UPDATE tmp_navigation_working30 a
            SET (
                fmeasure
               ,tmeasure
               ,lengthkm
               ,flowtimeday
               ,network_distancekm
               ,network_flowtimeday
               ,navtermination_flag
            ) = (
               SELECT
                aa.fmeasure
               ,aa.tmeasure
               ,aa.lengthkm
               ,aa.flowtimeday
               ,aa.network_distancekm
               ,aa.network_flowtimeday
               ,2
               FROM
               cipsrv_nhdplus_m.nav_trim_temp(
                   p_search_type          := str_search_type
                  ,p_fmeasure             := a.fmeasure
                  ,p_tmeasure             := a.tmeasure
                  ,p_lengthkm             := a.lengthkm
                  ,p_flowtimeday          := a.flowtimeday
                  ,p_network_distancekm   := a.network_distancekm
                  ,p_network_flowtimeday  := a.network_flowtimeday
                  ,p_maximum_distancekm   := num_maximum_distancekm
                  ,p_maximum_flowtimeday  := num_maximum_flowtimeday
               ) aa
            )
            WHERE
                a.selected IS TRUE
            AND a.network_distancekm > num_maximum_distancekm;

         ELSIF num_maximum_flowtimeday IS NOT NULL
         THEN
            UPDATE tmp_navigation_working30 a
            SET (
                fmeasure
               ,tmeasure
               ,lengthkm
               ,flowtimeday
               ,network_distancekm
               ,network_flowtimeday
               ,navtermination_flag
            ) = (
               SELECT
                aa.fmeasure
               ,aa.tmeasure
               ,aa.lengthkm
               ,aa.flowtimeday
               ,aa.network_distancekm
               ,aa.network_flowtimeday
               ,2
               FROM
               cipsrv_nhdplus_m.nav_trim_temp(
                   p_search_type          := str_search_type
                  ,p_fmeasure             := a.fmeasure
                  ,p_tmeasure             := a.tmeasure
                  ,p_lengthkm             := a.lengthkm
                  ,p_flowtimeday          := a.flowtimeday
                  ,p_network_distancekm   := a.network_distancekm
                  ,p_network_flowtimeday  := a.network_flowtimeday
                  ,p_maximum_distancekm   := num_maximum_distancekm
                  ,p_maximum_flowtimeday  := num_maximum_flowtimeday
               ) aa
            )
            WHERE
                a.selected IS TRUE
            AND a.network_flowtimeday > num_maximum_flowtimeday;

         END IF;
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 160
   -- Load the final table
   ----------------------------------------------------------------------------
   IF boo_return_flowline_details
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
      ,a.hydroseq
      ,a.fmeasure
      ,a.tmeasure
      ,a.levelpathi
      ,a.terminalpa
      ,a.uphydroseq
      ,a.dnhydroseq
      ,a.lengthkm
      ,a.flowtimeday
      /* ++++++++++ */
      ,a.network_distancekm
      ,a.network_flowtimeday
      /* ++++++++++ */
      ,a.permanent_identifier
      ,a.reachcode
      ,a.fcode
      ,a.gnis_id
      ,a.gnis_name
      ,a.wbarea_permanent_identifier
      /* ++++++++++ */
      ,a.navtermination_flag
      ,a.shape
      ,a.nav_order
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.hydroseq
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.levelpathi
         ,aa.terminalpa
         ,aa.uphydroseq
         ,aa.dnhydroseq
         ,aa.lengthkm
         ,aa.flowtimeday
         /* ++++++++++ */
         ,aa.network_distancekm
         ,aa.network_flowtimeday
         /* ++++++++++ */
         ,bb.permanent_identifier
         ,bb.reachcode
         ,bb.fcode
         ,bb.gnis_id
         ,bb.gnis_name
         ,bb.wbarea_permanent_identifier
         /* ++++++++++ */
         ,aa.navtermination_flag
         ,CASE
          WHEN boo_return_flowline_geometry
          THEN
            CASE
            WHEN aa.fmeasure <> bb.frommeas
            OR   aa.tmeasure <> bb.frommeas
            THEN
               ST_GeometryN(
                   ST_LocateBetween(bb.shape,aa.fmeasure,aa.tmeasure)
                  ,1
               )
            ELSE
               bb.shape
            END
          ELSE
            NULL::GEOMETRY
          END AS shape
         ,aa.nav_order
         FROM
         tmp_navigation_working30 aa
         JOIN
         cipsrv_nhdplus_m.networknhdflowline bb
         ON
         aa.nhdplusid = bb.nhdplusid
         WHERE
             aa.selected IS TRUE
         AND aa.fmeasure <> aa.tmeasure
         AND aa.fmeasure >= 0 AND aa.fmeasure <= 100
         AND aa.tmeasure >= 0 AND aa.tmeasure <= 100
         AND aa.lengthkm > 0
      ) a
      ORDER BY
       a.nav_order
      ,a.network_distancekm;
      
   ELSE
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
         ,network_distancekm
         ,network_flowtimeday
         ,navtermination_flag
         ,nav_order
      )
      SELECT
       a.nhdplusid
      ,a.hydroseq
      ,a.fmeasure
      ,a.tmeasure
      ,a.levelpathi
      ,a.terminalpa
      ,a.uphydroseq
      ,a.dnhydroseq
      ,a.lengthkm
      ,a.flowtimeday
      ,a.network_distancekm
      ,a.network_flowtimeday
      ,a.navtermination_flag
      ,a.nav_order
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.hydroseq
         ,aa.fmeasure
         ,aa.tmeasure
         ,aa.levelpathi
         ,aa.terminalpa
         ,aa.uphydroseq
         ,aa.dnhydroseq
         ,aa.lengthkm
         ,aa.flowtimeday
         ,aa.network_distancekm
         ,aa.network_flowtimeday
         ,aa.navtermination_flag
         ,aa.nav_order
         FROM
         tmp_navigation_working30 aa
         WHERE
             aa.selected IS TRUE
         AND aa.fmeasure <> aa.tmeasure
         AND aa.fmeasure >= 0 AND aa.fmeasure <= 100
         AND aa.tmeasure >= 0 AND aa.tmeasure <= 100
         AND aa.lengthkm > 0
      ) a
      ORDER BY
       a.nav_order
      ,a.network_distancekm;
   
   END IF;
   
   GET DIAGNOSTICS out_flowline_count = ROW_COUNT;
   
   IF out_flowline_count = 0
   THEN
      out_return_code    := -1;
      out_status_message := 'No results found.';
   
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.navigate(
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
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.navigate(
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
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
)  TO PUBLIC;

