DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.upstreamdownstream';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.upstreamdownstream(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   rec                               RECORD;
   json_input                        JSONB := $1;
   str_search_type                   VARCHAR(5);
   str_nhdplus_version               VARCHAR(50);
   
   int_start_nhdplusid               BIGINT;
   str_start_permanent_identifier    VARCHAR(40);
   str_start_reachcode               VARCHAR(14);
   int_start_hydroseq                BIGINT;
   num_start_measure                 NUMERIC;
   str_start_source_featureid        VARCHAR;
   str_start_source_featureid2       VARCHAR;
   str_start_source_originator       VARCHAR;
   str_start_source_series           VARCHAR;
   dat_start_start_date              DATE;
   dat_start_end_date                DATE;
   str_start_permid_joinkey          VARCHAR;
   str_start_source_joinkey          VARCHAR;
   str_start_cip_joinkey             VARCHAR;
   str_start_search_precision        VARCHAR;
   boo_start_push_rad_for_permid     BOOLEAN;
   str_start_linked_data_program     VARCHAR;
   
   int_stop_nhdplusid                BIGINT;
   str_stop_permanent_identifier     VARCHAR(40);
   str_stop_reachcode                VARCHAR(14);
   int_stop_hydroseq                 BIGINT;
   num_stop_measure                  NUMERIC;
   str_stop_source_featureid         VARCHAR;
   str_stop_source_featureid2        VARCHAR;
   str_stop_source_originator        VARCHAR;
   str_stop_source_series            VARCHAR;
   dat_stop_start_date               DATE;
   dat_stop_end_date                 DATE;
   str_stop_permid_joinkey           VARCHAR;
   str_stop_source_joinkey           VARCHAR;
   str_stop_cip_joinkey              VARCHAR;
   str_stop_search_precision         VARCHAR;
   boo_stop_push_rad_for_permid      BOOLEAN;
   str_stop_linked_data_program      VARCHAR;
   
   num_max_distancekm                NUMERIC;
   num_max_flowtimeday               NUMERIC;
   ary_linked_data_search_list       VARCHAR[];
   str_search_precision              VARCHAR;
   
   boo_return_flowlines              BOOLEAN;
   boo_return_flowline_details       BOOLEAN;
   boo_return_flowline_geometry      BOOLEAN;
   boo_return_catchments             BOOLEAN;
   boo_return_linked_data_sfid       BOOLEAN;
   boo_return_linked_data_cip        BOOLEAN;
   boo_return_linked_data_huc12      BOOLEAN;
   boo_return_linked_data_source     BOOLEAN;
   boo_return_linked_data_rad        BOOLEAN;
   boo_return_linked_data_attributes BOOLEAN;
   boo_remove_stop_start_sfids       BOOLEAN;
   boo_push_source_geometry_as_rad   BOOLEAN;
   
   int_grid_srid                     INTEGER;
   int_flowline_count                INTEGER;
   int_catchment_count               INTEGER;
   int_sfid_found_count              INTEGER;
   int_rad_found_count               INTEGER;
   int_src_found_count               INTEGER;
   int_cip_found_count               INTEGER;
   int_return_code                   INTEGER;
   str_status_message                VARCHAR;
   
   json_flowlines                    JSONB;
   json_catchments                   JSONB;
   json_linked_data_sfid_found       JSONB;
   json_linked_data_cip_found        JSONB;
   json_linked_data_huc12_found      JSONB;
   json_linked_data_source_points    JSONB;
   json_linked_data_source_lines     JSONB;
   json_linked_data_source_areas     JSONB;
   json_linked_data_reached_points   JSONB;
   json_linked_data_reached_lines    JSONB;
   json_linked_data_reached_areas    JSONB;
   json_linked_data_attributes       JSONB;
   str_known_region                  VARCHAR;
   int_count                         INTEGER;
   
BEGIN
   
   int_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   AND json_input->>'nhdplus_version' != ''
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';

   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'flowlines'                 , NULL
         ,'flowline_count'            , NULL
         ,'catchments'                , NULL
         ,'catchment_count'           , NULL
         ,'linked_data_sfid_found'    , NULL
         ,'linked_data_sfid_count'    , NULL
         ,'linked_data_cip_found'     , NULL
         ,'linked_data_cip_count'     , NULL
         ,'linked_data_huc12_found'   , NULL
         ,'linked_data_huc12_count'   , NULL
         ,'linked_data_source_points' , NULL
         ,'linked_data_source_lines'  , NULL
         ,'linked_data_source_areas'  , NULL
         ,'linked_data_source_count'  , NULL
         ,'linked_data_reached_points', NULL
         ,'linked_data_reached_lines' , NULL
         ,'linked_data_reached_areas' , NULL
         ,'linked_data_reached_count' , NULL
         ,'linked_data_attributes'    , NULL
         ,'start_nhdplusid'           , NULL
         ,'start_measure'             , NULL
         ,'start_linked_data_program' , NULL
         ,'stop_nhdplusid'            , NULL
         ,'stop_measure'              , NULL
         ,'stop_linked_data_program'  , NULL
         ,'return_code'               , -100
         ,'status_message'            , 'nhdplus version parameter is required'
      );
   
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.search_type')
   AND json_input->>'search_type' IS NOT NULL
   AND json_input->>'search_type' != ''
   THEN     
      str_search_type := json_input->>'search_type';

   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_nhdplusid')
   AND json_input->'start_nhdplusid' IS NOT NULL
   AND json_input->>'start_nhdplusid' != ''
   THEN
      int_start_nhdplusid := cipsrv_engine.json2bigint(json_input->'start_nhdplusid');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_permanent_identifier')
   AND json_input->>'start_permanent_identifier' IS NOT NULL
   AND json_input->>'start_permanent_identifier' != ''
   THEN
      str_start_permanent_identifier := json_input->>'start_permanent_identifier';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_reachcode')
   AND json_input->>'start_reachcode' IS NOT NULL
   AND json_input->>'start_reachcode' != ''
   THEN
      str_start_reachcode := json_input->>'start_reachcode';
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.start_hydroseq')
   AND json_input->'start_hydroseq' IS NOT NULL
   AND json_input->>'start_hydroseq' != ''
   THEN
      int_start_hydroseq := cipsrv_engine.json2numeric(json_input->'start_hydroseq');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_measure')
   AND json_input->'start_measure' IS NOT NULL
   AND json_input->>'start_measure' != ''
   THEN
      num_start_measure := cipsrv_engine.json2numeric(json_input->'start_measure');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_source_featureid')
   AND json_input->>'start_source_featureid' IS NOT NULL
   AND json_input->>'start_source_featureid' != ''
   THEN
      str_start_source_featureid := json_input->>'start_source_featureid';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_source_featureid2')
   AND json_input->>'start_source_featureid2' IS NOT NULL
   AND json_input->>'start_source_featureid2' != ''
   THEN
      str_start_source_featureid2 := json_input->>'start_source_featureid2';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_source_originator')
   AND json_input->>'start_source_originator' IS NOT NULL
   AND json_input->>'start_source_originator' != ''
   THEN
      str_start_source_originator := json_input->>'start_source_originator';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_source_series')
   AND json_input->>'start_source_series' IS NOT NULL
   AND json_input->>'start_source_series' != ''
   THEN
      str_start_source_series := json_input->>'start_source_series';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_start_date')
   AND json_input->>'start_start_date' IS NOT NULL
   AND json_input->>'start_start_date' != ''
   THEN
      dat_start_start_date := cipsrv_engine.json2date(json_input->>'start_start_date');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_end_date')
   AND json_input->>'start_end_date' IS NOT NULL
   AND json_input->>'start_end_date' != ''
   THEN
      dat_start_end_date := cipsrv_engine.json2date(json_input->>'start_end_date');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_permid_joinkey')
   AND json_input->>'start_permid_joinkey' IS NOT NULL
   AND json_input->>'start_permid_joinkey' != ''
   THEN
      str_start_permid_joinkey := json_input->>'start_permid_joinkey';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_source_joinkey')
   AND json_input->>'start_source_joinkey' IS NOT NULL
   AND json_input->>'start_source_joinkey' != ''
   THEN
      str_start_source_joinkey := json_input->>'start_source_joinkey';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_cip_joinkey')
   AND json_input->>'start_cip_joinkey' IS NOT NULL
   AND json_input->>'start_cip_joinkey' != ''
   THEN
      str_start_cip_joinkey := json_input->>'start_cip_joinkey';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_linked_data_program')
   AND json_input->>'start_linked_data_program' IS NOT NULL
   AND json_input->>'start_linked_data_program' != ''
   THEN
      str_start_linked_data_program := json_input->>'start_linked_data_program';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_search_precision')
   AND json_input->>'start_search_precision' IS NOT NULL
   AND json_input->>'start_search_precision' != ''
   THEN
      str_start_search_precision := json_input->>'start_search_precision';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_push_rad_for_permid')
   AND json_input->>'start_push_rad_for_permid' IS NOT NULL
   AND json_input->>'start_push_rad_for_permid' != ''
   THEN
      boo_start_push_rad_for_permid := (json_input->>'start_push_rad_for_permid')::BOOLEAN;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.stop_nhdplusid')
   AND json_input->'stop_nhdplusid' IS NOT NULL
   AND json_input->>'stop_nhdplusid' != ''
   THEN
      int_stop_nhdplusid := cipsrv_engine.json2bigint(json_input->'stop_nhdplusid');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_permanent_identifier')
   AND json_input->>'stop_permanent_identifier' IS NOT NULL
   AND json_input->>'stop_permanent_identifier' != ''
   THEN
      str_stop_permanent_identifier := json_input->>'stop_permanent_identifier';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_reachcode')
   AND json_input->>'stop_reachcode' IS NOT NULL
   AND json_input->>'stop_reachcode' != ''
   THEN
      str_stop_reachcode := json_input->>'stop_reachcode';
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.stop_hydroseq')
   AND json_input->'stop_hydroseq' IS NOT NULL
   AND json_input->>'stop_hydroseq' != ''
   THEN
      int_stop_hydroseq := cipsrv_engine.json2bigint(json_input->'stop_hydroseq');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_measure')
   AND json_input->'stop_measure' IS NOT NULL
   AND json_input->>'stop_measure' != ''
   THEN
      num_stop_measure := cipsrv_engine.json2numeric(json_input->'stop_measure');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_source_featureid')
   AND json_input->>'stop_source_featureid' IS NOT NULL
   AND json_input->>'stop_source_featureid' != ''
   THEN
      str_stop_source_featureid := json_input->>'stop_source_featureid';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_source_featureid2')
   AND json_input->>'stop_source_featureid2' IS NOT NULL
   AND json_input->>'stop_source_featureid2' != ''
   THEN
      str_stop_source_featureid2 := json_input->>'stop_source_featureid2';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_source_originator')
   AND json_input->>'stop_source_originator' IS NOT NULL
   AND json_input->>'stop_source_originator' != ''
   THEN
      str_stop_source_originator := json_input->>'stop_source_originator';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_source_series')
   AND json_input->>'stop_source_series' IS NOT NULL
   AND json_input->>'stop_source_series' != ''
   THEN
      str_stop_source_series := json_input->>'stop_source_series';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_start_date')
   AND json_input->>'stop_start_date' IS NOT NULL
   AND json_input->>'stop_start_date' != ''
   THEN
      dat_stop_start_date := cipsrv_engine.json2date(json_input->>'stop_start_date');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_end_date')
   AND json_input->>'stop_end_date' IS NOT NULL
   AND json_input->>'stop_end_date' != ''
   THEN
      dat_stop_end_date := cipsrv_engine.json2date(json_input->>'stop_end_date');
   
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_permid_joinkey')
   AND json_input->>'stop_permid_joinkey' IS NOT NULL
   AND json_input->>'stop_permid_joinkey' != ''
   THEN
      str_stop_permid_joinkey := json_input->>'stop_permid_joinkey';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_source_joinkey')
   AND json_input->>'stop_source_joinkey' IS NOT NULL
   AND json_input->>'stop_source_joinkey' != ''
   THEN
      str_stop_source_joinkey := json_input->>'stop_source_joinkey';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_cip_joinkey')
   AND json_input->>'stop_cip_joinkey' IS NOT NULL
   AND json_input->>'stop_cip_joinkey' != ''
   THEN
      str_stop_cip_joinkey := json_input->>'stop_cip_joinkey';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_linked_data_program')
   AND json_input->>'stop_linked_data_program' IS NOT NULL
   AND json_input->>'stop_linked_data_program' != ''
   THEN
      str_stop_linked_data_program := json_input->>'stop_linked_data_program';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_search_precision')
   AND json_input->>'stop_search_precision' IS NOT NULL
   AND json_input->>'stop_search_precision' != ''
   THEN
      str_stop_search_precision := json_input->>'stop_search_precision';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_push_rad_for_permid')
   AND json_input->>'stop_push_rad_for_permid' IS NOT NULL
   AND json_input->>'stop_push_rad_for_permid' != ''
   THEN
      boo_stop_push_rad_for_permid := (json_input->>'stop_push_rad_for_permid')::BOOLEAN;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.max_distancekm')
   AND json_input->'max_distancekm' IS NOT NULL 
   AND json_input->>'max_distancekm' != ''
   THEN
      num_max_distancekm := cipsrv_engine.json2numeric(json_input->'max_distancekm');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.max_flowtimeday')
   AND json_input->'max_flowtimeday' IS NOT NULL 
   AND json_input->>'max_flowtimeday' != ''
   THEN
      num_max_flowtimeday := cipsrv_engine.json2numeric(json_input->'max_flowtimeday');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.linked_data_search_list')
   AND json_input->>'linked_data_search_list' IS NOT NULL
   AND json_input->>'linked_data_search_list' != ''
   THEN
      ary_linked_data_search_list := cipsrv_engine.json2strary(json_input->'linked_data_search_list');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.search_precision')
   AND json_input->>'search_precision' IS NOT NULL
   AND json_input->>'search_precision' != ''
   THEN
      str_search_precision := json_input->>'search_precision';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_flowlines')
   AND json_input->'return_flowlines' IS NOT NULL
   AND json_input->>'return_flowlines' != ''
   THEN
      boo_return_flowlines        := (json_input->'return_flowlines')::BOOLEAN;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_flowline_details')
   AND json_input->'return_flowline_details' IS NOT NULL
   AND json_input->>'return_flowline_details' != ''
   THEN
      boo_return_flowline_details := (json_input->'return_flowline_details')::BOOLEAN;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_flowline_geometry')
   AND json_input->'return_flowline_geometry' IS NOT NULL
   AND json_input->>'return_flowline_geometry' != ''
   THEN
      boo_return_flowline_geometry := (json_input->'return_flowline_geometry')::BOOLEAN;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.known_region')
   AND json_input->>'known_region' IS NOT NULL
   AND json_input->>'known_region' != ''
   THEN
      str_known_region := json_input->>'known_region';
      
   END IF;
 
   IF JSONB_PATH_EXISTS(json_input,'$.return_catchments')
   AND json_input->'return_catchments' IS NOT NULL
   AND json_input->>'return_catchments' != ''
   THEN
      boo_return_catchments       := (json_input->'return_catchments')::BOOLEAN;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_linked_data_sfid')
   AND json_input->'return_linked_data_sfid' IS NOT NULL
   AND json_input->>'return_linked_data_sfid' != ''
   THEN
      boo_return_linked_data_sfid := (json_input->'return_linked_data_sfid')::BOOLEAN;
      
   ELSE
      boo_return_linked_data_sfid := TRUE;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.return_linked_data_rad')
   AND json_input->'return_linked_data_rad' IS NOT NULL
   AND json_input->>'return_linked_data_rad' != ''
   THEN
      boo_return_linked_data_rad := (json_input->'return_linked_data_rad')::BOOLEAN;
      
   ELSE
      boo_return_linked_data_rad := FALSE;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.return_linked_data_cip')
   AND json_input->'return_linked_data_cip' IS NOT NULL
   AND json_input->>'return_linked_data_cip' != ''
   THEN
      boo_return_linked_data_cip := (json_input->'return_linked_data_cip')::BOOLEAN;
      
   ELSE
      boo_return_linked_data_cip := FALSE;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.return_linked_data_source')
   AND json_input->'return_linked_data_source' IS NOT NULL
   AND json_input->>'return_linked_data_source' != ''
   THEN
      boo_return_linked_data_source := (json_input->'return_linked_data_source')::BOOLEAN;
      
   ELSE
      boo_return_linked_data_source := FALSE;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.push_source_geometry_as_rad')
   AND json_input->'push_source_geometry_as_rad' IS NOT NULL
   AND json_input->>'push_source_geometry_as_rad' != ''
   THEN
      boo_push_source_geometry_as_rad := (json_input->'push_source_geometry_as_rad')::BOOLEAN;
      
   ELSE
      boo_push_source_geometry_as_rad := FALSE;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.remove_stop_start_sfids')
   AND json_input->'remove_stop_start_sfids' IS NOT NULL
   AND json_input->>'remove_stop_start_sfids' != ''
   THEN
      boo_remove_stop_start_sfids := (json_input->'remove_stop_start_sfids')::BOOLEAN;
      
   ELSE
      boo_remove_stop_start_sfids := FALSE;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Call the upstreamdownstream engine
   ----------------------------------------------------------------------------
   rec := cipsrv_owld.upstreamdownstream(
       p_nhdplus_version               := str_nhdplus_version
      ,p_search_type                   := str_search_type
      
      ,p_start_nhdplusid               := int_start_nhdplusid
      ,p_start_permanent_identifier    := str_start_permanent_identifier
      ,p_start_reachcode               := str_start_reachcode
      ,p_start_hydroseq                := int_start_hydroseq
      ,p_start_measure                 := num_start_measure
      ,p_start_source_featureid        := str_start_source_featureid
      ,p_start_source_featureid2       := str_start_source_featureid2
      ,p_start_source_originator       := str_start_source_originator
      ,p_start_source_series           := str_start_source_series
      ,p_start_start_date              := dat_start_start_date
      ,p_start_end_date                := dat_start_end_date
      ,p_start_permid_joinkey          := str_start_permid_joinkey
      ,p_start_source_joinkey          := str_start_source_joinkey
      ,p_start_cip_joinkey             := str_start_cip_joinkey
      ,p_start_linked_data_program     := str_start_linked_data_program
      ,p_start_search_precision        := str_start_search_precision
      ,p_start_push_rad_for_permid     := boo_start_push_rad_for_permid
      
      ,p_stop_nhdplusid                := int_stop_nhdplusid
      ,p_stop_permanent_identifier     := str_stop_permanent_identifier
      ,p_stop_reachcode                := str_stop_reachcode
      ,p_stop_hydroseq                 := int_stop_hydroseq
      ,p_stop_measure                  := num_stop_measure
      ,p_stop_source_featureid         := str_stop_source_featureid
      ,p_stop_source_featureid2        := str_stop_source_featureid2
      ,p_stop_source_originator        := str_stop_source_originator
      ,p_stop_source_series            := str_stop_source_series
      ,p_stop_start_date               := dat_stop_start_date
      ,p_stop_end_date                 := dat_stop_end_date
      ,p_stop_permid_joinkey           := str_stop_permid_joinkey
      ,p_stop_source_joinkey           := str_stop_source_joinkey
      ,p_stop_cip_joinkey              := str_stop_cip_joinkey
      ,p_stop_linked_data_program      := str_stop_linked_data_program
      ,p_stop_search_precision         := str_stop_search_precision
      ,p_stop_push_rad_for_permid      := boo_stop_push_rad_for_permid
      
      ,p_max_distancekm                := num_max_distancekm
      ,p_max_flowtimeday               := num_max_flowtimeday
      ,p_linked_data_search_list       := ary_linked_data_search_list
      ,p_search_precision              := str_search_precision
      
      ,p_return_flowlines              := boo_return_flowlines
      ,p_return_flowline_details       := boo_return_flowline_details
      ,p_return_flowline_geometry      := boo_return_flowline_geometry
      ,p_return_catchments             := boo_return_catchments
      ,p_return_linked_data_cip        := boo_return_linked_data_cip
      ,p_return_linked_data_huc12      := boo_return_linked_data_huc12
      ,p_return_linked_data_source     := boo_return_linked_data_source
      ,p_return_linked_data_rad        := boo_return_linked_data_rad
      ,p_return_linked_data_attributes := boo_return_linked_data_attributes
      ,p_remove_stop_start_sfids       := boo_remove_stop_start_sfids
      ,p_push_source_geometry_as_rad   := boo_push_source_geometry_as_rad
      
      ,p_known_region                  := str_known_region
   );
   int_start_nhdplusid            := rec.out_start_nhdplusid;
   str_start_permanent_identifier := rec.out_start_permanent_identifier;
   num_start_measure              := rec.out_start_measure;
   str_start_linked_data_program  := rec.out_start_linked_data_program; 
   int_grid_srid                  := rec.out_grid_srid;
   int_stop_nhdplusid             := rec.out_stop_nhdplusid;
   num_stop_measure               := rec.out_stop_measure;
   str_stop_linked_data_program   := rec.out_stop_linked_data_program;
   int_flowline_count             := rec.out_flowline_count;
   int_catchment_count            := rec.out_catchment_count;
   int_rad_found_count            := rec.out_rad_found_count;
   int_sfid_found_count           := rec.out_sfid_found_count;
   int_cip_found_count            := rec.out_cip_found_count;
   int_src_found_count            := rec.out_src_found_count;
   int_return_code                := rec.out_return_code;
   str_status_message             := rec.out_status_message;
   
   IF int_return_code != 0
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'flowlines'                 , NULL
         ,'flowline_count'            , NULL
         ,'catchments'                , NULL
         ,'catchment_count'           , NULL
         ,'linked_data_sfid_found'    , NULL
         ,'linked_data_sfid_count'    , NULL
         ,'linked_data_cip_found'     , NULL
         ,'linked_data_cip_count'     , NULL
         ,'linked_data_huc12_found'   , NULL
         ,'linked_data_huc12_count'   , NULL
         ,'linked_data_source_points' , NULL
         ,'linked_data_source_lines'  , NULL
         ,'linked_data_source_areas'  , NULL
         ,'linked_data_source_count'  , NULL
         ,'linked_data_reached_points', NULL
         ,'linked_data_reached_lines' , NULL
         ,'linked_data_reached_areas' , NULL
         ,'linked_data_reached_count' , NULL
         ,'linked_data_attributes'    , NULL
         ,'start_nhdplusid'           , int_start_nhdplusid
         ,'start_measure'             , num_start_measure
         ,'start_linked_data_program' , str_start_linked_data_program
         ,'stop_nhdplusid'            , int_stop_nhdplusid
         ,'stop_measure'              , num_stop_measure
         ,'stop_linked_data_program'  , str_stop_linked_data_program
         ,'return_code'               , int_return_code
         ,'status_message'            , str_status_message
      );
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Build the flowlines featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_flowlines
   THEN
      json_flowlines := (
         SELECT 
         JSONB_AGG(j.my_json) AS my_feats
         FROM (
            SELECT 
            JSONB_BUILD_OBJECT(
                'type',       'Feature'
               ,'obj_type',   'navigated_flowline_properties'
               ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
               ,'properties', TO_JSONB(t.*) - 'geom'
            ) AS my_json
            FROM (
               SELECT
                a.nhdplusid
               ,a.hydroseq
               ,a.fmeasure
               ,a.tmeasure
               ,a.levelpathi
               ,a.terminalpa
               ,a.uphydroseq
               ,a.dnhydroseq
               ,TRUNC(a.lengthkm,8)    AS lengthkm
               ,TRUNC(a.flowtimeday,8) AS flowtimeday
               /* +++++++++ */
               ,TRUNC(a.network_distancekm,8)  AS network_distancekm
               ,TRUNC(a.network_flowtimeday,8) AS network_flowtimeday
               /* +++++++++ */
               ,a.permanent_identifier
               ,a.reachcode
               ,a.fcode
               ,a.gnis_id
               ,a.gnis_name
               ,a.wbarea_permanent_identifier
               /* +++++++++ */
               ,a.navtermination_flag
               ,a.nav_order
               ,CASE WHEN boo_return_flowline_geometry THEN ST_Transform(a.shape,4326) ELSE NULL::GEOMETRY END AS geom
               FROM
               tmp_navigation_results a
               ORDER BY
                a.nav_order
               ,a.network_distancekm
            ) t
         ) j
      );
            
      IF json_flowlines IS NULL
      OR JSONB_ARRAY_LENGTH(json_flowlines) = 0
      THEN
         json_flowlines := NULL;
         
      ELSE
         json_flowlines := JSON_BUILD_OBJECT(
             'type'    , 'FeatureCollection'
            ,'features', json_flowlines
         );
         
      END IF;
      
   ELSE
      json_flowlines := NULL;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Build the catchments featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_catchments
   THEN
      json_catchments := (
         SELECT 
         JSONB_AGG(j.my_json) AS my_feats
         FROM (
            SELECT 
            JSONB_BUILD_OBJECT(
                'type',       'Feature'
               ,'obj_type',   'navigated_catchment_properties'
               ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
               ,'properties', TO_JSONB(t.*) - 'geom'
            ) AS my_json
            FROM (
               SELECT
                a.nhdplusid
               ,a.sourcefc
               ,a.areasqkm
               ,ST_Transform(a.shape,4326) AS geom
               FROM
               tmp_catchments a
               ORDER BY
               a.orderingkey
            ) t
         ) j
      );
            
      IF json_catchments IS NULL
      OR JSONB_ARRAY_LENGTH(json_catchments) = 0
      THEN
         json_catchments := NULL;
         
      ELSE
         json_catchments := JSON_BUILD_OBJECT(
             'type'    , 'FeatureCollection'
            ,'features', json_catchments
         );
         
      END IF;
      
   ELSE
      json_catchments := NULL;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Build the sfid tabular output
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_sfid
   THEN
      json_linked_data_sfid_found := (
         SELECT 
         JSONB_AGG(a.*)
         FROM (
            SELECT
             aa.eventtype
            ,aa.source_joinkey
            ,aa.source_originator
            ,aa.source_featureid
            ,aa.source_featureid2
            ,aa.source_series
            ,aa.source_subdivision
            ,aa.start_date
            ,aa.end_date
            ,aa.sfiddetailurl
            ,aa.src_event_count
            ,aa.rad_event_count
            ,aa.src_cat_joinkey_count
            ,aa.nearest_cip_distancekm_permid
            ,aa.nearest_cip_distancekm_cat
            ,aa.nearest_cip_network_distancekm
            ,aa.nearest_rad_distancekm_permid
            ,aa.nearest_rad_network_distancekm
            ,aa.nearest_cip_flowtimeday_permid
            ,aa.nearest_cip_flowtimeday_cat
            ,aa.nearest_cip_network_flowtimeday
            ,aa.nearest_rad_flowtimeday_permid
            ,aa.nearest_rad_network_flowtimeday
            FROM
            tmp_sfid_found aa
            ORDER BY
             aa.eventtype
            ,aa.nearest_cip_network_distancekm
            ,aa.nearest_rad_network_distancekm
         ) a
      );
            
      IF json_linked_data_sfid_found IS NULL
      OR JSONB_ARRAY_LENGTH(json_linked_data_sfid_found) = 0
      THEN
         json_linked_data_sfid_found := NULL;
         
      END IF;
      
   ELSE
      json_linked_data_sfid_found := NULL;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- Build the cip tabular output
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_cip
   THEN
      json_linked_data_cip_found := (
         SELECT 
         JSONB_AGG(a.*)
         FROM (
            SELECT
             aa.eventtype
            ,aa.cip_joinkey
            ,aa.source_originator
            ,aa.source_featureid
            ,aa.source_featureid2
            ,aa.source_series
            ,aa.source_subdivision
            ,aa.start_date
            ,aa.end_date
            ,aa.catchmentstatecode
            ,aa.nhdplusid
            ,aa.istribal
            ,aa.istribal_areasqkm
            ,aa.catchmentresolution
            ,aa.catchmentareasqkm
            ,aa.xwalk_huc12
            ,aa.xwalk_method
            ,aa.xwalk_huc12_version
            ,aa.isnavigable
            ,aa.hasvaa
            ,aa.issink
            ,aa.isheadwater
            ,aa.iscoastal
            ,aa.isocean
            ,aa.isalaskan
            ,aa.h3hexagonaddr
            ,aa.network_distancekm
            ,aa.network_flowtimeday
            FROM
            tmp_cip_found aa
            ORDER BY
             aa.network_distancekm
            ,aa.network_flowtimeday
         ) a
      );
            
      IF json_linked_data_cip_found IS NULL
      OR JSONB_ARRAY_LENGTH(json_linked_data_cip_found) = 0
      THEN
         json_linked_data_cip_found := NULL;
         
      END IF;
      
   ELSE
      json_linked_data_cip_found := NULL;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Build the source points featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_source
   AND int_src_found_count > 0
   THEN
      json_linked_data_source_points := (
         SELECT 
         JSONB_AGG(j.my_json) AS my_feats
         FROM (
            SELECT 
            JSONB_BUILD_OBJECT(
                'type',       'Feature'
               ,'obj_type',   'linked_source_point_properties'
               ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
               ,'properties', TO_JSONB(t.*) - 'geom'
            ) AS my_json
            FROM (
               SELECT
                a.eventtype
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
               ,ST_Transform(a.shape,4326) AS geom
               FROM
               tmp_src_points a
               ORDER BY
                a.eventtype
               ,a.orderingkey
            ) t
         ) j
      );
            
      IF json_linked_data_source_points IS NULL
      OR JSONB_ARRAY_LENGTH(json_linked_data_source_points) = 0
      THEN
         json_linked_data_source_points := NULL;
         
      ELSE
         json_linked_data_source_points := JSON_BUILD_OBJECT(
             'type'    , 'FeatureCollection'
            ,'features', json_linked_data_source_points
         );
         
      END IF;
      
   ELSE
      json_linked_data_source_points := NULL;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 80
   -- Build the source lines featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_source
   AND int_src_found_count > 0
   THEN
      json_linked_data_source_lines := (
         SELECT 
         JSONB_AGG(j.my_json) AS my_feats
         FROM (
            SELECT 
            JSONB_BUILD_OBJECT(
                'type',       'Feature'
               ,'obj_type',   'linked_source_point_properties'
               ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
               ,'properties', TO_JSONB(t.*) - 'geom'
            ) AS my_json
            FROM (
               SELECT
                a.eventtype
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
               ,ST_Transform(a.shape,4326) AS geom
               FROM
               tmp_src_lines a
               ORDER BY
                a.eventtype
               ,a.orderingkey
            ) t
         ) j
      );
            
      IF json_linked_data_source_lines IS NULL
      OR JSONB_ARRAY_LENGTH(json_linked_data_source_lines) = 0
      THEN
         json_linked_data_source_lines := NULL;
         
      ELSE
         json_linked_data_source_lines := JSON_BUILD_OBJECT(
             'type'    , 'FeatureCollection'
            ,'features', json_linked_data_source_lines
         );
         
      END IF;
      
   ELSE
      json_linked_data_source_lines := NULL;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 80
   -- Build the source areas featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_source
   AND int_src_found_count > 0
   THEN
      json_linked_data_source_areas := (
         SELECT 
         JSONB_AGG(j.my_json) AS my_feats
         FROM (
            SELECT 
            JSONB_BUILD_OBJECT(
                'type',       'Feature'
               ,'obj_type',   'linked_source_point_properties'
               ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
               ,'properties', TO_JSONB(t.*) - 'geom'
            ) AS my_json
            FROM (
               SELECT
                a.eventtype
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
               ,ST_Transform(a.shape,4326) AS geom
               FROM
               tmp_src_areas a
               ORDER BY
                a.eventtype
               ,a.orderingkey
            ) t
         ) j
      );
            
      IF json_linked_data_source_areas IS NULL
      OR JSONB_ARRAY_LENGTH(json_linked_data_source_areas) = 0
      THEN
         json_linked_data_source_areas := NULL;
         
      ELSE
         json_linked_data_source_areas := JSON_BUILD_OBJECT(
             'type'    , 'FeatureCollection'
            ,'features', json_linked_data_source_areas
         );
         
      END IF;
      
   ELSE
      json_linked_data_source_areas := NULL;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 110
   -- Build the reached points featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_rad
   AND int_rad_found_count > 0
   THEN
      json_linked_data_reached_points := (
         SELECT 
         JSONB_AGG(j.my_json) AS my_feats
         FROM (
            SELECT 
            JSONB_BUILD_OBJECT(
                'type',       'Feature'
               ,'obj_type',   'linked_reached_point_properties'
               ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
               ,'properties', TO_JSONB(t.*) - 'geom'
            ) AS my_json
            FROM (
               SELECT
                a.eventtype
               ,a.permanent_identifier
               ,a.eventdate
               ,a.reachcode
               ,a.reachsmdate
               ,a.reachresolution
               ,a.feature_permanent_identifier
               ,a.featureclassref
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_datadesc
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.permid_joinkey
               ,a.start_date
               ,a.end_date
               ,a.featuredetailurl
               ,a.measure
               ,a.eventoffset
               ,a.geogstate
               ,a.xwalk_huc12
               ,a.isnavigable
               ,a.network_distancekm
               ,a.network_flowtimeday
               ,ST_Transform(a.shape,4326) AS geom
               FROM
               tmp_rad_points a
               ORDER BY
                a.eventtype
               ,a.network_distancekm
            ) t
         ) j
      );

      IF json_linked_data_reached_points IS NULL
      OR JSONB_ARRAY_LENGTH(json_linked_data_reached_points) = 0
      THEN
         json_linked_data_reached_points := NULL;
         
      ELSE
         json_linked_data_reached_points := JSON_BUILD_OBJECT(
             'type'    , 'FeatureCollection'
            ,'features', json_linked_data_reached_points
         );
         
      END IF;
      
   ELSE
      json_linked_data_reached_points := NULL;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 120
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSONB_BUILD_OBJECT(
       'flowlines'                 , json_flowlines
      ,'flowline_count'            , int_flowline_count
      ,'catchments'                , json_catchments
      ,'catchment_count'           , int_catchment_count
      ,'linked_data_sfid_found'    , json_linked_data_sfid_found
      ,'linked_data_sfid_count'    , int_sfid_found_count
      ,'linked_data_cip_found'     , json_linked_data_cip_found
      ,'linked_data_cip_count'     , int_cip_found_count
      ,'linked_data_huc12_found'   , NULL
      ,'linked_data_huc12_count'   , NULL
      ,'linked_data_source_points' , json_linked_data_source_points
      ,'linked_data_source_lines'  , json_linked_data_source_lines
      ,'linked_data_source_areas'  , json_linked_data_source_areas
      ,'linked_data_source_count'  , int_src_found_count
      ,'linked_data_reached_points', json_linked_data_reached_points
      ,'linked_data_reached_lines' , NULL
      ,'linked_data_reached_areas' , NULL
      ,'linked_data_reached_count' , int_rad_found_count
      ,'linked_data_attributes'    , NULL
      ,'start_nhdplusid'           , int_start_nhdplusid
      ,'start_measure'             , num_start_measure
      ,'start_linked_data_program' , str_start_linked_data_program
      ,'stop_nhdplusid'            , int_stop_nhdplusid
      ,'stop_measure'              , num_stop_measure
      ,'stop_linked_data_program'  , str_stop_linked_data_program
      ,'return_code'               , int_return_code
      ,'status_message'            , str_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.upstreamdownstream(
   JSONB
) OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.upstreamdownstream(
   JSONB
) TO PUBLIC;

