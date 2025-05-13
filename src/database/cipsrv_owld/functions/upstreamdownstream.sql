DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.upstreamdownstream';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_owld.upstreamdownstream(
    IN  p_nhdplus_version               VARCHAR
   ,IN  p_search_type                   VARCHAR
   
   ,IN  p_start_nhdplusid               BIGINT
   ,IN  p_start_permanent_identifier    VARCHAR
   ,IN  p_start_reachcode               VARCHAR
   ,IN  p_start_hydroseq                BIGINT
   ,IN  p_start_measure                 NUMERIC
   ,IN  p_start_source_featureid        VARCHAR
   ,IN  p_start_source_featureid2       VARCHAR
   ,IN  p_start_source_originator       VARCHAR
   ,IN  p_start_source_series           VARCHAR
   ,IN  p_start_start_date              DATE
   ,IN  p_start_end_date                DATE
   ,IN  p_start_permid_joinkey          VARCHAR
   ,IN  p_start_source_joinkey          VARCHAR
   ,IN  p_start_cip_joinkey             VARCHAR
   ,IN  p_start_linked_data_program     VARCHAR
   ,IN  p_start_search_precision        VARCHAR
   ,IN  p_start_push_rad_for_permid     BOOLEAN
      
   ,IN  p_stop_nhdplusid                BIGINT
   ,IN  p_stop_permanent_identifier     VARCHAR
   ,IN  p_stop_reachcode                VARCHAR
   ,IN  p_stop_hydroseq                 BIGINT
   ,IN  p_stop_measure                  NUMERIC
   ,IN  p_stop_source_featureid         VARCHAR
   ,IN  p_stop_source_featureid2        VARCHAR
   ,IN  p_stop_source_originator        VARCHAR
   ,IN  p_stop_source_series            VARCHAR
   ,IN  p_stop_start_date               DATE
   ,IN  p_stop_end_date                 DATE
   ,IN  p_stop_permid_joinkey           VARCHAR
   ,IN  p_stop_source_joinkey           VARCHAR
   ,IN  p_stop_cip_joinkey              VARCHAR
   ,IN  p_stop_linked_data_program      VARCHAR
   ,IN  p_stop_search_precision         VARCHAR
   ,IN  p_stop_push_rad_for_permid      BOOLEAN
   
   ,IN  p_max_distancekm                NUMERIC
   ,IN  p_max_flowtimeday               NUMERIC
   ,IN  p_linked_data_search_list       VARCHAR[]
   ,IN  p_search_precision              VARCHAR
   
   ,IN  p_return_flowlines              BOOLEAN   
   ,IN  p_return_flowline_details       BOOLEAN
   ,IN  p_return_flowline_geometry      BOOLEAN
   ,IN  p_return_catchments             BOOLEAN
   ,IN  p_return_linked_data_cip        BOOLEAN
   ,IN  p_return_linked_data_huc12      BOOLEAN
   ,IN  p_return_linked_data_source     BOOLEAN
   ,IN  p_return_linked_data_rad        BOOLEAN
   ,IN  p_return_linked_data_attributes BOOLEAN
   ,IN  p_remove_stop_start_sfids       BOOLEAN
   ,IN  p_push_source_geometry_as_rad   BOOLEAN
   
   ,IN  p_known_region                  VARCHAR DEFAULT NULL
   
   ,OUT out_start_nhdplusid             BIGINT
   ,OUT out_start_permanent_identifier  VARCHAR
   ,OUT out_start_measure               NUMERIC
   ,OUT out_start_linked_data_program   VARCHAR
   ,OUT out_grid_srid                   INTEGER
   ,OUT out_stop_nhdplusid              BIGINT
   ,OUT out_stop_measure                NUMERIC
   ,OUT out_stop_linked_data_program    VARCHAR
   ,OUT out_flowline_count              INTEGER
   ,OUT out_catchment_count             INTEGER
   ,OUT out_cip_found_count             INTEGER
   ,OUT out_huc12_found_count           INTEGER
   ,OUT out_rad_found_count             INTEGER
   ,OUT out_sfid_found_count            INTEGER
   ,OUT out_src_found_count             INTEGER
   ,OUT out_return_flowlines            BOOLEAN
   ,OUT out_return_code                 NUMERIC
   ,OUT out_status_message              VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                            RECORD;
   str_nhdplus_version            VARCHAR := p_nhdplus_version;
   str_search_type                VARCHAR := p_search_type;
   num_max_distancekm             NUMERIC := p_max_distancekm;
   num_max_flowtimeday            NUMERIC := p_max_flowtimeday;
   boo_return_flowline_details    BOOLEAN := p_return_flowline_details;
   boo_return_flowline_geometry   BOOLEAN := p_return_flowline_geometry;
   str_known_region               VARCHAR := p_known_region;

   int_start_nhdplusid            BIGINT  := p_start_nhdplusid;
   str_start_permanent_identifier VARCHAR := p_start_permanent_identifier;
   int_start_hydroseq             BIGINT  := p_start_hydroseq;
   str_start_reachcode            VARCHAR := p_start_reachcode;
   int_start_catnhdplusid         BIGINT;
   num_start_measure              NUMERIC := p_start_measure;
   sdo_start_shape                GEOMETRY;
   str_start_source_featureid     VARCHAR;
   str_start_source_featureid2    VARCHAR;
   str_start_source_originator    VARCHAR;
   str_start_source_series        VARCHAR;
   str_start_source_subdivision   VARCHAR;
   dat_start_start_date           DATE;
   dat_start_end_date             DATE;
   str_start_nhdplus_version      VARCHAR;
   str_start_source_joinkey       VARCHAR;
   str_start_permid_joinkey       VARCHAR;
   str_start_cip_joinkey          VARCHAR;
   str_start_search_precision     VARCHAR := p_start_search_precision;
   str_start_linked_data_program  VARCHAR;
   
   int_stop_nhdplusid             BIGINT  := p_stop_nhdplusid;
   str_stop_permanent_identifier  VARCHAR := p_stop_permanent_identifier;
   int_stop_hydroseq              BIGINT  := p_stop_hydroseq;
   str_stop_reachcode             VARCHAR := p_stop_reachcode;
   int_stop_catnhdplusid          BIGINT;
   num_stop_measure               NUMERIC := p_stop_measure;
   sdo_stop_shape                 GEOMETRY;
   str_stop_source_featureid      VARCHAR;
   str_stop_source_featureid2     VARCHAR;
   str_stop_source_originator     VARCHAR;
   str_stop_source_series         VARCHAR;
   str_stop_source_subdivision    VARCHAR;
   dat_stop_start_date            DATE;
   dat_stop_end_date              DATE;
   str_stop_nhdplus_version       VARCHAR;
   str_stop_source_joinkey        VARCHAR;
   str_stop_permid_joinkey        VARCHAR;
   str_stop_cip_joinkey           VARCHAR;
   str_stop_search_precision      VARCHAR := p_stop_search_precision;
   str_stop_linked_data_program   VARCHAR;
   
   str_search_precision           VARCHAR := UPPER(p_search_precision);   
   int_attains_eventtype          INTEGER := 10033;
   int_frspub_eventtype           INTEGER := 10028;
   int_npdes_eventtype            INTEGER := 10015;
   int_wqp_eventtype              INTEGER := 10032;
   boo_remove_stop_start_sfids    BOOLEAN;
   str_remove_start_permid        VARCHAR(40);
   str_remove_stop_permid         VARCHAR(40);
   str_resolution_abbrev          VARCHAR(2);
   str_owld                       VARCHAR(64);
   int_count                      INTEGER;
   int_owld                       INTEGER;
   boo_catchment_okay             BOOLEAN;
   boo_reach_okay                 BOOLEAN;
   str_joinkey_fix                VARCHAR;
      
BEGIN

   out_return_code := cipsrv_engine.create_navigation_temp_tables();
   out_return_code := cipsrv_engine.create_delineation_temp_tables();
   out_return_code := cipsrv_owld.create_updn_temp_tables();

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   out_return_code    := 0;
   
   IF str_nhdplus_version IS NULL
   OR str_nhdplus_version IN ('MR','nhdplus_m')
   THEN
      str_nhdplus_version := 'MR';
      
   ELSIF str_nhdplus_version IN ('HR','nhdplus_h')
   THEN
      str_nhdplus_version := 'HR';
      
   ELSE
      out_return_code    := -10;
      out_status_message := 'Invalid resolution value ' || str_nhdplus_version || '.';
   
   END IF;
   
   IF str_search_type IS NULL
   THEN
      str_search_type := 'UT';
      
   END IF;
   
   IF str_search_precision IS NULL
   THEN
      str_search_precision := 'CATCHMENT';
      
   END IF;
   
   IF p_remove_stop_start_sfids IS NULL
   THEN
      boo_remove_stop_start_sfids := TRUE;
      
   ELSE
      boo_remove_stop_start_sfids := p_remove_stop_start_sfids;
      
   END IF;
   
   out_flowline_count  := 0;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Convert any program locations to network locations
   ----------------------------------------------------------------------------
   IF p_start_linked_data_program IS NOT NULL
   OR p_start_permid_joinkey      IS NOT NULL
   OR p_start_source_joinkey      IS NOT NULL
   OR p_start_cip_joinkey         IS NOT NULL
   THEN
   
      rec := cipsrv_owld.owld_locator(
          p_nhdplus_version               := str_nhdplus_version
         ,p_source_featureid              := p_start_source_featureid
         ,p_source_featureid2             := p_start_source_featureid2
         ,p_source_originator             := p_start_source_originator
         ,p_source_series                 := p_start_source_series
         ,p_start_date                    := p_start_start_date
         ,p_end_date                      := p_start_end_date
         ,p_linked_data_program           := p_start_linked_data_program
         ,p_source_joinkey                := p_start_source_joinkey
         ,p_permid_joinkey                := p_start_permid_joinkey
         ,p_cip_joinkey                   := p_start_cip_joinkey
         ,p_search_direction              := str_search_type
         ,p_reference_catchment_nhdplusid := NULL
         ,p_reference_reachcode           := NULL
         ,p_reference_flowline_nhdplusid  := NULL
         ,p_reference_hydroseq            := NULL
         ,p_reference_measure             := NULL
         ,p_reference_point               := NULL
         ,p_search_precision              := str_start_search_precision
         ,p_known_region                  := str_known_region
         ,p_push_rad_for_permid           := p_start_push_rad_for_permid
      );
      int_start_catnhdplusid         := rec.out_catchment_nhdplusid;
      str_start_permanent_identifier := rec.out_permanent_identifier;
      str_start_reachcode            := rec.out_reachcode;
      str_start_nhdplus_version      := rec.out_nhdplus_version;
      
      IF rec.out_flowline_nhdplusid IS NOT NULL
      THEN
         int_start_nhdplusid         := rec.out_flowline_nhdplusid;
         
      ELSIF rec.out_catchment_nhdplusid IS NOT NULL
      THEN
         int_start_nhdplusid         := rec.out_catchment_nhdplusid;
      
      ELSE
         IF rec.out_return_code != 0
         THEN
            out_return_code          := rec.out_return_code;
            out_status_message       := rec.out_status_message;
     
         ELSE
            out_return_code          := -20;
            out_status_message       := 'Unable to determine network location for start event';

         END IF;
         
         RETURN;
         
      END IF;
      
      str_start_linked_data_program  := rec.out_linked_data_program;
      int_start_hydroseq             := rec.out_hydroseq;
      num_start_measure              := rec.out_measure;
      sdo_start_shape                := rec.out_shape;
      str_start_source_featureid     := rec.out_source_featureid;
      str_start_source_featureid2    := rec.out_source_featureid2;
      str_start_source_originator    := rec.out_source_originator;
      str_start_source_series        := rec.out_source_series;
      str_start_source_subdivision   := rec.out_source_subdivision;
      dat_start_start_date           := rec.out_start_date;
      dat_start_end_date             := rec.out_end_date;
      str_start_source_joinkey       := rec.out_source_joinkey;
      str_start_permid_joinkey       := rec.out_permid_joinkey;
      str_start_cip_joinkey          := rec.out_cip_joinkey;
      out_return_code                := rec.out_return_code;
      out_status_message             := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
         
      END IF;
      
      IF boo_remove_stop_start_sfids
      THEN
         str_remove_start_permid := str_start_permid_joinkey;
      
      END IF;
   
   END IF;
   
   IF p_stop_linked_data_program IS NOT NULL
   OR p_stop_permid_joinkey      IS NOT NULL
   OR p_stop_source_joinkey      IS NOT NULL
   OR p_stop_cip_joinkey         IS NOT NULL
   THEN
   
      rec := cipsrv_owld.owld_locator(
          p_nhdplus_version               := str_nhdplus_version
         ,p_source_featureid              := p_stop_source_featureid
         ,p_source_featureid2             := p_stop_source_featureid2
         ,p_source_originator             := p_stop_source_originator
         ,p_source_series                 := p_stop_source_series
         ,p_start_date                    := p_stop_start_date
         ,p_end_date                      := p_stop_end_date
         ,p_linked_data_program           := p_stop_linked_data_program
         ,p_source_joinkey                := p_stop_source_joinkey
         ,p_permid_joinkey                := p_stop_permid_joinkey
         ,p_cip_joinkey                   := p_stop_cip_joinkey
         ,p_search_direction              := str_search_type
         ,p_reference_catchment_nhdplusid := NULL
         ,p_reference_reachcode           := NULL
         ,p_reference_flowline_nhdplusid  := NULL
         ,p_reference_hydroseq            := NULL
         ,p_reference_measure             := NULL
         ,p_reference_point               := NULL
         ,p_search_precision              := str_stop_search_precision
         ,p_known_region                  := str_known_region
         ,p_push_rad_for_permid           := TRUE
      );
      int_stop_catnhdplusid          := rec.out_catchment_nhdplusid;
      str_stop_permanent_identifier  := rec.out_permanent_identifier;
      str_stop_reachcode             := rec.out_reachcode;
      str_stop_nhdplus_version       := rec.out_nhdplus_version;
      
      IF rec.out_flowline_nhdplusid IS NOT NULL
      THEN
         int_stop_nhdplusid          := rec.out_flowline_nhdplusid;
         
      ELSIF rec.out_catchment_nhdplusid IS NOT NULL
      THEN
         int_stop_nhdplusid          := rec.out_catchment_nhdplusid;
      
      ELSE
         IF rec.out_return_code != 0
         THEN
            out_return_code          := rec.out_return_code;
            out_status_message       := rec.out_status_message;
     
         ELSE
            out_return_code          := -20;
            out_status_message       := 'Unable to determine network location for stop event';

         END IF;
         
         RETURN;
         
      END IF;
      
      str_stop_linked_data_program   := rec.out_linked_data_program;
      int_stop_hydroseq              := rec.out_hydroseq;
      num_stop_measure               := rec.out_measure;
      sdo_stop_shape                 := rec.out_shape;
      str_stop_source_featureid      := rec.out_source_featureid;
      str_stop_source_featureid2     := rec.out_source_featureid2;
      str_stop_source_originator     := rec.out_source_originator;
      str_stop_source_series         := rec.out_source_series;
      str_stop_source_subdivision    := rec.out_source_subdivision;
      dat_stop_start_date            := rec.out_start_date;
      dat_stop_end_date              := rec.out_end_date;
      str_stop_source_joinkey        := rec.out_source_joinkey;
      str_stop_permid_joinkey        := rec.out_permid_joinkey;
      str_stop_cip_joinkey           := rec.out_cip_joinkey;
      out_return_code                := rec.out_return_code;
      out_status_message             := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
         
      END IF;
      
      IF boo_remove_stop_start_sfids
      THEN
         str_remove_stop_permid := str_stop_permid_joinkey;
      
      END IF;
   
   END IF;
   
   out_start_nhdplusid            := int_start_nhdplusid;
   out_start_measure              := num_start_measure;
   out_start_permanent_identifier := str_start_permanent_identifier;
   out_start_linked_data_program  := str_start_linked_data_program;
   out_stop_nhdplusid             := int_stop_nhdplusid;
   out_stop_measure               := num_stop_measure;
   out_stop_linked_data_program   := str_stop_linked_data_program;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Call the navigation engine
   ----------------------------------------------------------------------------
   IF str_nhdplus_version IN ('MR','nhdplus_m')
   THEN
      str_resolution_abbrev := 'MR';
      
      rec := cipsrv_nhdplus_m.navigate(
          p_search_type                 := str_search_type
         ,p_start_nhdplusid             := int_start_nhdplusid
         ,p_start_permanent_identifier  := str_start_permanent_identifier
         ,p_start_reachcode             := str_start_reachcode
         ,p_start_hydroseq              := int_start_hydroseq
         ,p_start_measure               := num_start_measure
         ,p_stop_nhdplusid              := int_stop_nhdplusid
         ,p_stop_permanent_identifier   := str_stop_permanent_identifier
         ,p_stop_reachcode              := str_stop_reachcode
         ,p_stop_hydroseq               := int_stop_hydroseq
         ,p_stop_measure                := num_stop_measure
         ,p_max_distancekm              := num_max_distancekm
         ,p_max_flowtimeday             := num_max_flowtimeday
         ,p_return_flowline_details     := TRUE
         ,p_return_flowline_geometry    := boo_return_flowline_geometry
         ,p_known_region                := str_known_region
      );
      int_start_nhdplusid := rec.out_start_nhdplusid;
      str_start_permanent_identifier := rec.out_start_permanent_identifier;
      num_start_measure   := rec.out_start_measure;
      out_grid_srid       := rec.out_grid_srid;
      int_stop_nhdplusid  := rec.out_stop_nhdplusid;
      num_stop_measure    := rec.out_stop_measure;
      out_flowline_count  := rec.out_flowline_count;
      out_return_code     := rec.out_return_code;
      out_status_message  := rec.out_status_message;
      
      IF p_return_catchments
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,areasqkm
            ,orderingkey
            ,shape
         )
         SELECT
          a.nhdplusid
         ,a.sourcefc
         ,a.areasqkm
         ,b.network_distancekm
         ,a.shape
         FROM
         cipsrv_nhdplus_m.nhdpluscatchment a
         JOIN
         tmp_navigation_results b
         ON
         a.nhdplusid = b.nhdplusid;
         
         GET DIAGNOSTICS int_count = ROW_COUNT;
         
         IF out_catchment_count IS NULL
         THEN
            out_catchment_count := int_count;
            
         ELSE
            out_catchment_count := out_catchment_count + int_count;
         
         END IF;
      
      END IF;
      
   ELSIF str_nhdplus_version IN ('HR','nhdplus_h')
   THEN
      str_resolution_abbrev := 'HR';
      
      rec := cipsrv_nhdplus_h.navigate(
          p_search_type                 := str_search_type
         ,p_start_nhdplusid             := int_start_nhdplusid
         ,p_start_permanent_identifier  := str_start_permanent_identifier
         ,p_start_reachcode             := str_start_reachcode
         ,p_start_hydroseq              := int_start_hydroseq
         ,p_start_measure               := num_start_measure
         ,p_stop_nhdplusid              := int_stop_nhdplusid
         ,p_stop_permanent_identifier   := str_stop_permanent_identifier
         ,p_stop_reachcode              := str_stop_reachcode
         ,p_stop_hydroseq               := int_stop_hydroseq
         ,p_stop_measure                := num_stop_measure
         ,p_max_distancekm              := num_max_distancekm
         ,p_max_flowtimeday             := num_max_flowtimeday
         ,p_return_flowline_details     := TRUE
         ,p_return_flowline_geometry    := boo_return_flowline_geometry
         ,p_known_region                := str_known_region
      );
      int_start_nhdplusid := rec.out_start_nhdplusid;
      str_start_permanent_identifier := rec.out_start_permanent_identifier;
      num_start_measure   := rec.out_start_measure;
      out_grid_srid       := rec.out_grid_srid;
      int_stop_nhdplusid  := rec.out_stop_nhdplusid;
      num_stop_measure    := rec.out_stop_measure;
      out_flowline_count  := rec.out_flowline_count;
      out_return_code     := rec.out_return_code;
      out_status_message  := rec.out_status_message;
      
      IF p_return_catchments
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,areasqkm
            ,orderingkey
            ,shape
         )
         SELECT
          a.nhdplusid
         ,a.sourcefc
         ,a.areasqkm
         ,b.network_distancekm
         ,a.shape
         FROM
         cipsrv_nhdplus_h.nhdpluscatchment a
         JOIN
         tmp_navigation_results b
         ON
         a.nhdplusid = b.nhdplusid;
         
         GET DIAGNOSTICS int_count = ROW_COUNT;
         
         IF out_catchment_count IS NULL
         THEN
            out_catchment_count := int_count;
            
         ELSE
            out_catchment_count := out_catchment_count + int_count;
         
         END IF;
         
      END IF;
   
   ELSE
      RAISE EXCEPTION 'err %',str_nhdplus_version;
      
   END IF;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- Search programs for results
   ----------------------------------------------------------------------------
   IF  p_linked_data_search_list IS NOT NULL
   AND array_length(p_linked_data_search_list,1) > 0
   THEN

      FOR i IN 1 .. array_length(p_linked_data_search_list,1)
      LOOP
      
         boo_catchment_okay := FALSE;
         boo_reach_okay     := FALSE;
         
         IF p_linked_data_search_list[i] IN (int_frspub_eventtype::VARCHAR,'FRSPUB')
         THEN
            str_owld := 'cipsrv_owld.frspub';
            int_owld := int_frspub_eventtype;
            
            boo_catchment_okay := TRUE;
            boo_reach_okay     := TRUE;
            str_joinkey_fix    := 'permid_joinkey';
            
         ELSIF p_linked_data_search_list[i] IN (int_npdes_eventtype::VARCHAR,'NPDES')
         THEN
            str_owld := 'cipsrv_owld.npdes';
            int_owld := int_npdes_eventtype;
            
            boo_catchment_okay := TRUE;
            boo_reach_okay     := TRUE;
            str_joinkey_fix    := 'permid_joinkey';
            
         ELSIF p_linked_data_search_list[i] IN (int_wqp_eventtype::VARCHAR,'WQP')
         THEN
            str_owld := 'cipsrv_owld.wqp';
            int_owld := int_wqp_eventtype;
            
            boo_catchment_okay := TRUE;
            boo_reach_okay     := TRUE;
            str_joinkey_fix    := 'permid_joinkey';
            
         ELSIF p_linked_data_search_list[i] IN (int_attains_eventtype::VARCHAR,'ATTAINS')
         THEN
            str_owld := 'cipsrv_owld.attains';
            int_owld := int_attains_eventtype;
            
            boo_catchment_okay := TRUE;
            boo_reach_okay     := FALSE;
            str_joinkey_fix    := 'source_joinkey';
            
         ELSE
            CONTINUE;
         
         END IF;

         --##################################################################--
         IF  str_search_precision IN ('REACH','REACHED') 
         AND boo_reach_okay
         THEN
            -------------------------------------------------------------------
            EXECUTE '
               INSERT INTO tmp_rad_points(
                   eventtype
                  ,permanent_identifier
                  ,eventdate
                  ,reachcode
                  ,reachsmdate
                  ,reachresolution
                  ,feature_permanent_identifier
                  ,featureclassref
                  ,source_originator
                  ,source_featureid
                  ,source_featureid2
                  ,source_datadesc
                  ,source_series
                  ,source_subdivision
                  ,source_joinkey
                  ,permid_joinkey
                  ,start_date
                  ,end_date
                  ,featuredetailurl
                  ,measure
                  ,eventoffset
                  ,geogstate
                  ,xwalk_huc12
                  ,isnavigable
                  ,network_distancekm
                  ,network_flowtimeday
                  ,shape
               )
               SELECT
                $1
               ,b.permanent_identifier
               ,b.eventdate
               ,b.reachcode
               ,b.reachsmdate
               ,$2 AS reachresolution
               ,b.feature_permanent_identifier
               ,b.featureclassref
               ,b.source_originator
               ,b.source_featureid
               ,b.source_featureid2
               ,b.source_datadesc
               ,b.source_series
               ,b.source_subdivision
               ,b.source_joinkey
               ,b.permid_joinkey
               ,b.start_date
               ,b.end_date
               ,b.featuredetailurl
               ,b.measure
               ,b.eventoffset
               ,b.geogstate
               ,b.xwalk_huc12
               ,b.isnavigable
               ,cipsrv_owld.adjust_point_extent(
                   p_extent_value      => a.network_distancekm
                  ,p_direction         => $3
                  ,p_flowline_amount   => a.lengthkm
                  ,p_flowline_fmeasure => a.fmeasure
                  ,p_flowline_tmeasure => a.tmeasure
                  ,p_event_measure     => b.measure::NUMERIC
                ) AS network_distancekm
               ,cipsrv_owld.adjust_point_extent(
                   p_extent_value      => a.network_flowtimeday
                  ,p_direction         => $4
                  ,p_flowline_amount   => a.flowtimeday
                  ,p_flowline_fmeasure => a.fmeasure
                  ,p_flowline_tmeasure => a.tmeasure
                  ,p_event_measure     => b.measure::NUMERIC
                ) AS network_flowtimeday
               ,CASE
                WHEN NOT $5
                THEN
                  NULL::GEOMETRY
                ELSE
                  b.shape
                END AS shape
               FROM
               tmp_navigation_results a
               JOIN
               ' || str_owld || '_rad_p b
               ON
                   b.reachcode  = a.reachcode
               AND b.measure   >= a.fmeasure
               AND b.measure   <= a.tmeasure
               WHERE
                   b.reachresolution = $6
               AND ($7 IS NULL OR b.permid_joinkey != $8)
               AND ($9 IS NULL OR b.permid_joinkey != $10);
            ' 
            USING
             int_owld
            ,str_resolution_abbrev
            ,p_search_type
            ,p_search_type
            ,p_return_linked_data_rad
            ,str_resolution_abbrev
            ,str_remove_start_permid
            ,str_remove_start_permid
            ,str_remove_stop_permid
            ,str_remove_stop_permid;

            GET DIAGNOSTICS int_count = ROW_COUNT;           

            IF out_rad_found_count IS NULL
            OR out_rad_found_count = 0
            THEN
               out_rad_found_count := int_count;
               
            ELSE
               out_rad_found_count := out_rad_found_count + int_count;
            
            END IF;

            -------------------------------------------------------------------
            IF p_push_source_geometry_as_rad
            AND int_count > 0
            THEN
               EXECUTE '
                  UPDATE tmp_rad_points a
                  SET shape = (
                     SELECT
                     b.shape
                     FROM
                     ' || str_owld || '_src_p b
                     WHERE
                     b.permid_joinkey = a.permid_joinkey
                  )
                  WHERE
                  a.eventtype = $1
               ' USING
               int_owld;
            
            END IF;

            -------------------------------------------------------------------
            IF p_return_linked_data_source
            THEN
               EXECUTE '
                  INSERT INTO tmp_src_points(
                      eventtype
                     ,permid_joinkey
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,featuredetailurl
                     ,orderingkey
                     ,shape
                  )
                  SELECT
                   $1
                  ,a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.featuredetailurl
                  ,b.network_distancekm
                  ,a.shape
                  FROM
                  ' || str_owld || '_src_p a
                  JOIN
                  tmp_rad_points b
                  ON
                  a.' || str_joinkey_fix || ' = b.' || str_joinkey_fix || '
                  ON CONFLICT DO NOTHING
               ' USING
               int_owld;

               GET DIAGNOSTICS int_count = ROW_COUNT;
               
               IF out_src_found_count IS NULL
               OR out_src_found_count = 0
               THEN
                  out_src_found_count := int_count;
                  
               ELSE
                  out_src_found_count := out_src_found_count + int_count;
                  
               END IF;

            END IF;
            
            EXECUTE '
               INSERT INTO tmp_sfid_found(
                   eventtype
                  ,source_originator
                  ,source_featureid
                  ,source_featureid2
                  ,source_series
                  ,source_subdivision
                  ,source_joinkey
                  ,start_date
                  ,end_date
                  ,sfiddetailurl
                  ,rad_event_count
                  ,nearest_rad_distancekm_permid
                  ,nearest_rad_network_distancekm
                  ,nearest_rad_flowtimeday_permid
                  ,nearest_rad_network_flowtimeday
               )
               SELECT
                $1
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.sfiddetailurl
               ,b.rad_event_count
               ,b.nearest_rad_distancekm_permid
               ,b.nearest_rad_network_distancekm
               ,b.nearest_rad_flowtimeday_permid
               ,b.nearest_rad_network_flowtimeda
               FROM
               ' || str_owld || '_sfid a
               JOIN (
                  /* needs better conversion from oracle */
                  SELECT
                   bb.source_joinkey
                  ,COUNT(*) AS rad_event_count
                  ,MIN(bb.permid_joinkey)      AS nearest_rad_distancekm_permid
                  ,MIN(bb.network_distancekm)  AS nearest_rad_network_distancekm
                  ,CASE
                   WHEN MIN(bb.network_flowtimeday) IS NULL
                   THEN
                     NULL
                   ELSE
                     MIN(bb.permid_joinkey)
                   END AS nearest_rad_flowtimeday_permid
                  ,MIN(bb.network_flowtimeday) AS nearest_rad_network_flowtimeda
                  FROM
                  tmp_rad_points bb
                  WHERE
                  bb.eventtype = $2
                  GROUP BY
                  bb.source_joinkey
               ) b
               ON
               a.source_joinkey = b.source_joinkey
            ' USING
             int_owld
            ,int_owld;
                
            GET DIAGNOSTICS int_count = ROW_COUNT;
       
            IF out_sfid_found_count IS NULL
            OR out_sfid_found_count = 0
            THEN
               out_sfid_found_count := int_count;
               
            ELSE
               out_sfid_found_count := out_sfid_found_count + int_count;
            
            END IF;
            
         --##################################################################--
         ELSIF str_search_precision IN ('CATCHMENT') 
         AND boo_catchment_okay
         THEN
            -------------------------------------------------------------------
            -------------------------------------------------------------------
            EXECUTE '
               INSERT INTO tmp_cip_found(
                   eventtype
                  ,cip_joinkey
                  ,permid_joinkey
                  ,source_originator
                  ,source_featureid
                  ,source_featureid2
                  ,source_series
                  ,source_subdivision
                  ,source_joinkey
                  ,start_date
                  ,end_date
                  ,cat_joinkey
                  ,catchmentstatecode
                  ,nhdplusid
                  ,istribal
                  ,istribal_areasqkm
                  ,catchmentresolution
                  ,catchmentareasqkm
                  ,xwalk_huc12
                  ,xwalk_method
                  ,xwalk_huc12_version
                  ,isnavigable
                  ,hasvaa
                  ,issink
                  ,isheadwater
                  ,iscoastal
                  ,isocean
                  ,isalaskan
                  ,h3hexagonaddr
                  ,network_distancekm
                  ,network_flowtimeday
               )
               SELECT
                $1
               ,a.cip_joinkey
               ,a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.cat_joinkey
               ,a.catchmentstatecode
               ,a.nhdplusid
               ,a.istribal
               ,a.istribal_areasqkm
               ,a.catchmentresolution
               ,a.catchmentareasqkm
               ,a.xwalk_huc12
               ,a.xwalk_method
               ,a.xwalk_huc12_version
               ,a.isnavigable
               ,a.hasvaa
               ,a.issink
               ,a.isheadwater
               ,a.iscoastal
               ,a.isocean
               ,a.isalaskan
               ,a.h3hexagonaddr
               ,b.network_distancekm
               ,b.network_flowtimeday
               FROM
               ' || str_owld || '_cip a
               JOIN (
                  SELECT
                   bb.nhdplusid
                  ,MIN(bb.network_distancekm)  AS network_distancekm
                  ,MIN(bb.network_flowtimeday) AS network_flowtimeday
                  FROM 
                  tmp_navigation_results bb
                  GROUP BY
                  bb.nhdplusid
               ) b
               ON
               b.nhdplusid = a.nhdplusid
               WHERE
                   a.catchmentresolution = $2
               AND ($3 IS NULL OR a.permid_joinkey != $4)
               AND ($5 IS NULL OR a.permid_joinkey != $6);
            ' USING
             int_owld
            ,str_resolution_abbrev
            ,str_remove_start_permid
            ,str_remove_start_permid
            ,str_remove_stop_permid
            ,str_remove_stop_permid;
            
            GET DIAGNOSTICS int_count = ROW_COUNT;

            IF out_cip_found_count IS NULL
            OR out_cip_found_count = 0
            THEN
               out_cip_found_count := int_count;
               
            ELSE
               out_cip_found_count := out_cip_found_count + int_count;
            
            END IF;
            
            -------------------------------------------------------------------
            -------------------------------------------------------------------
            IF p_return_linked_data_huc12
            THEN
               EXECUTE '
                  INSERT INTO tmp_huc12_found(            
                      eventtype
                     ,huc12_joinkey
                     ,permid_joinkey
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,xwalk_huc12
                     ,xwalk_huc12_version
                     ,xwalk_catresolution
                     ,xwalk_huc12_areasqkm
                  )
                  SELECT
                   $1
                  ,NULL
                  ,a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.xwalk_huc12
                  ,a.xwalk_huc12_version
                  ,a.xwalk_catresolution
                  ,a.xwalk_huc12_areasqkm
                  FROM
                  ' || str_owld || '_huc12 a
                  WHERE
                      a.xwalk_catresolution = $2
                  AND EXISTS (
                     SELECT
                     1
                     FROM
                     tmp_cip_found bb
                     WHERE
                     bb.' || str_joinkey_fix || ' = a.' || str_joinkey_fix || '
                  ) 
               ' USING
                int_owld
               ,str_resolution_abbrev;
               
               GET DIAGNOSTICS int_count = ROW_COUNT;
               
               IF out_huc12_found_count IS NULL
               OR out_huc12_found_count = 0
               THEN
                  out_huc12_found_count := int_count;
                  
               ELSE
                  out_huc12_found_count := out_huc12_found_count + int_count;
                  
               END IF;
            
            END IF;

            -------------------------------------------------------------------
            -------------------------------------------------------------------
            IF p_return_linked_data_source
            THEN
               ----------------------------------------------------------------
               EXECUTE '
                  INSERT INTO tmp_src_points(
                      eventtype
                     ,permid_joinkey
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,featuredetailurl
                     ,orderingkey
                     ,shape
                  )
                  SELECT
                   $1
                  ,a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.featuredetailurl
                  ,b.network_distancekm
                  ,a.shape
                  FROM
                  ' || str_owld || '_src_p a
                  JOIN (
                     SELECT
                      bb.' || str_joinkey_fix || '
                     ,MIN(bb.network_distancekm) AS network_distancekm
                     FROM
                     tmp_cip_found bb
                     GROUP BY
                     bb.' || str_joinkey_fix || '
                  ) b
                  ON
                  a.' || str_joinkey_fix || ' = b.' || str_joinkey_fix || '
                  ON CONFLICT DO NOTHING
               ' USING
               int_owld;
               
               GET DIAGNOSTICS int_count = ROW_COUNT;
               
               IF out_src_found_count IS NULL
               OR out_src_found_count = 0
               THEN
                  out_src_found_count := int_count;
                  
               ELSE
                  out_src_found_count := out_src_found_count + int_count;
                  
               END IF;

               ----------------------------------------------------------------
               EXECUTE '
                  INSERT INTO tmp_src_lines(
                      eventtype
                     ,permid_joinkey
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,featuredetailurl
                     ,lengthkm
                     ,orderingkey
                     ,shape
                  )
                  SELECT
                   $1
                  ,a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.featuredetailurl
                  ,a.lengthkm
                  ,b.network_distancekm
                  ,a.shape
                  FROM
                  ' || str_owld || '_src_l a
                  JOIN (
                     SELECT
                      bb.' || str_joinkey_fix || '
                     ,MIN(bb.network_distancekm) AS network_distancekm
                     FROM
                     tmp_cip_found bb
                     GROUP BY
                     bb.' || str_joinkey_fix || '
                  ) b
                  ON
                  a.' || str_joinkey_fix || ' = b.' || str_joinkey_fix || '
                  ON CONFLICT DO NOTHING
               ' USING
               int_owld;
               
               GET DIAGNOSTICS int_count = ROW_COUNT;
               
               IF out_src_found_count IS NULL
               OR out_src_found_count = 0
               THEN
                  out_src_found_count := int_count;
                  
               ELSE
                  out_src_found_count := out_src_found_count + int_count;
                  
               END IF;

               ----------------------------------------------------------------
               EXECUTE '
                  INSERT INTO tmp_src_areas(
                      eventtype
                     ,permid_joinkey
                     ,source_originator
                     ,source_featureid
                     ,source_featureid2
                     ,source_series
                     ,source_subdivision
                     ,source_joinkey
                     ,start_date
                     ,end_date
                     ,featuredetailurl
                     ,areasqkm
                     ,orderingkey
                     ,shape
                  )
                  SELECT
                   $1
                  ,a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,a.featuredetailurl
                  ,a.areasqkm
                  ,b.network_distancekm
                  ,a.shape
                  FROM
                  ' || str_owld || '_src_a a
                  JOIN (
                     SELECT
                      bb.' || str_joinkey_fix || '
                     ,MIN(bb.network_distancekm) AS network_distancekm
                     FROM
                     tmp_cip_found bb
                     GROUP BY
                     bb.' || str_joinkey_fix || '
                  ) b
                  ON
                  a.' || str_joinkey_fix || ' = b.' || str_joinkey_fix || '
                  ON CONFLICT DO NOTHING
               ' USING
               int_owld;

               GET DIAGNOSTICS int_count = ROW_COUNT;
               
               IF out_src_found_count IS NULL
               OR out_src_found_count = 0
               THEN
                  out_src_found_count := int_count;
                  
               ELSE
                  out_src_found_count := out_src_found_count + int_count;
                  
               END IF;               

            END IF;
            
            -------------------------------------------------------------------
            -------------------------------------------------------------------
            EXECUTE '
               INSERT INTO tmp_sfid_found(
                   eventtype
                  ,source_originator
                  ,source_featureid
                  ,source_featureid2
                  ,source_series
                  ,source_subdivision
                  ,source_joinkey
                  ,start_date
                  ,end_date
                  ,sfiddetailurl
                  ,src_cat_joinkey_count
                  ,nearest_cip_distancekm_permid
                  ,nearest_cip_network_distancekm
                  ,nearest_cip_flowtimeday_permid
                  ,nearest_cip_network_flowtimeday
               )
               SELECT
                $1
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.sfiddetailurl
               ,b.src_cat_joinkey_count
               ,b.nearest_cip_distancekm_permid
               ,b.nearest_cip_network_distancekm
               ,b.nearest_cip_flowtimeday_permid
               ,b.nearest_cip_network_flowtimeday
               FROM
               ' || str_owld || '_sfid a
               JOIN (
                  SELECT
                   bb.source_joinkey
                  ,COUNT(*) AS src_cat_joinkey_count
                  ,MIN(bb.permid_joinkey)      AS nearest_cip_distancekm_permid
                  ,MIN(bb.network_distancekm)  AS nearest_cip_network_distancekm
                  ,CASE
                   WHEN MIN(bb.network_flowtimeday) IS NULL
                   THEN
                     NULL
                   ELSE
                     MIN(bb.permid_joinkey)
                   END AS nearest_cip_flowtimeday_permid
                  ,MIN(bb.network_flowtimeday) AS nearest_cip_network_flowtimeday
                  FROM
                  tmp_cip_found bb
                  WHERE
                  bb.eventtype = $2
                  GROUP BY
                  bb.source_joinkey
               ) b
               ON
               a.source_joinkey = b.source_joinkey
            ' USING
             int_owld
            ,int_owld;
                
            GET DIAGNOSTICS int_count = ROW_COUNT;
       
            IF out_sfid_found_count IS NULL
            OR out_sfid_found_count = 0
            THEN
               out_sfid_found_count := int_count;
               
            ELSE
               out_sfid_found_count := out_sfid_found_count + int_count;
            
            END IF;
      
         END IF;
   
      END LOOP;
      
   END IF;
   
   ----------------------------------------------------------------------------
   IF NOT p_return_linked_data_rad
   THEN
      out_rad_found_count := NULL;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.upstreamdownstream';
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
