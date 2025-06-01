DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.delineate';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.delineate(
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
   int_stop_nhdplusid                BIGINT;
   str_stop_permanent_identifier     VARCHAR(40);
   str_stop_reachcode                VARCHAR(14);
   int_stop_hydroseq                 BIGINT;
   num_stop_measure                  NUMERIC;
   num_max_distancekm                NUMERIC;
   num_max_flowtimeday               NUMERIC;   
   str_aggregation_engine            VARCHAR;
   boo_split_initial_catchment       BOOLEAN;
   boo_fill_basin_holes              BOOLEAN;
   boo_force_no_cache                BOOLEAN;
   boo_return_delineation_geometry   BOOLEAN;
   boo_return_flowlines              BOOLEAN;
   boo_return_flowline_details       BOOLEAN;
   boo_return_flowline_geometry      BOOLEAN;
   int_grid_srid                     INTEGER;
   int_flowline_count                INTEGER;
   int_return_code                   INTEGER;
   str_status_message                VARCHAR;
   json_delineated_area              JSONB;
   json_flowlines                    JSONB;
   str_aggregation_used              VARCHAR;
   str_known_region                  VARCHAR;
   
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
          'flowlines',       NULL
         ,'flowline_count',  NULL
         ,'nhdplus_version', NULL
         ,'return_code',     -100
         ,'status_message',  'nhdplus version parameter is required'
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
   
   IF JSONB_PATH_EXISTS(json_input,'$.aggregation_engine')
   AND json_input->>'aggregation_engine' IS NOT NULL
   AND json_input->>'aggregation_engine' != ''
   THEN
      str_aggregation_engine := json_input->>'aggregation_engine';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.split_initial_catchment')
   AND json_input->'split_initial_catchment' IS NOT NULL
   AND json_input->>'split_initial_catchment' != ''
   THEN
      boo_split_initial_catchment := (json_input->'split_initial_catchment')::BOOLEAN;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fill_basin_holes')
   AND json_input->'fill_basin_holes' IS NOT NULL
   AND json_input->>'fill_basin_holes' != ''
   THEN
      boo_fill_basin_holes := (json_input->'fill_basin_holes')::BOOLEAN;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.force_no_cache')
   AND json_input->'force_no_cache' IS NOT NULL
   AND json_input->>'force_no_cache' != ''
   THEN
      boo_force_no_cache := (json_input->'force_no_cache')::BOOLEAN;
      
   END IF; 
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_delineation_geometry')
   AND json_input->'return_delineation_geometry' IS NOT NULL
   AND json_input->>'return_delineation_geometry' != ''
   THEN
      boo_return_delineation_geometry := (json_input->'return_delineation_geometry')::BOOLEAN;
      
   END IF;   
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_flowlines')
   AND json_input->'return_flowlines' IS NOT NULL
   AND json_input->>'return_flowlines' != ''
   THEN
      boo_return_flowlines := (json_input->'return_flowlines')::BOOLEAN;
      
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
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Call the indexing engine
   ----------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cipsrv_nhdplus_m.delineate(
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
         ,p_aggregation_engine          := str_aggregation_engine
         ,p_split_initial_catchment     := boo_split_initial_catchment
         ,p_fill_basin_holes            := boo_fill_basin_holes
         ,p_force_no_cache              := boo_force_no_cache
         ,p_return_delineation_geometry := boo_return_delineation_geometry
         ,p_return_flowlines            := boo_return_flowlines
         ,p_return_flowline_details     := boo_return_flowline_details
         ,p_return_flowline_geometry    := boo_return_flowline_geometry
         ,p_known_region                := str_known_region
      );
      str_aggregation_used  := rec.out_aggregation_used;
      int_start_nhdplusid   := rec.out_start_nhdplusid;
      str_start_permanent_identifier := rec.out_start_permanent_identifier;
      num_start_measure     := rec.out_start_measure;
      int_grid_srid         := rec.out_grid_srid;
      int_stop_nhdplusid    := rec.out_stop_nhdplusid;
      num_stop_measure      := rec.out_stop_measure;
      int_flowline_count    := rec.out_flowline_count;
      boo_return_flowlines  := rec.out_return_flowlines;
      int_return_code       := rec.out_return_code;
      str_status_message    := rec.out_status_message;
      
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cipsrv_nhdplus_h.delineate(
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
         ,p_aggregation_engine          := str_aggregation_engine
         ,p_split_initial_catchment     := boo_split_initial_catchment
         ,p_fill_basin_holes            := boo_fill_basin_holes
         ,p_force_no_cache              := boo_force_no_cache
         ,p_return_delineation_geometry := boo_return_delineation_geometry
         ,p_return_flowlines            := boo_return_flowlines
         ,p_return_flowline_details     := boo_return_flowline_details
         ,p_return_flowline_geometry    := boo_return_flowline_geometry
         ,p_known_region                := str_known_region
      );
      str_aggregation_used  := rec.out_aggregation_used;
      int_start_nhdplusid   := rec.out_start_nhdplusid;
      str_start_permanent_identifier := rec.out_start_permanent_identifier;
      num_start_measure     := rec.out_start_measure;
      int_grid_srid         := rec.out_grid_srid;
      int_stop_nhdplusid    := rec.out_stop_nhdplusid;
      num_stop_measure      := rec.out_stop_measure;
      int_flowline_count    := rec.out_flowline_count;
      boo_return_flowlines  := rec.out_return_flowlines;
      int_return_code       := rec.out_return_code;
      str_status_message    := rec.out_status_message;
   
   ELSE
      RAISE EXCEPTION 'err %',str_nhdplus_version;
      
   END IF;
   
   IF int_return_code != 0
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'delineated_area',  NULL
         ,'aggregation_used', NULL
         ,'flowlines',        NULL
         ,'flowline_count',   NULL
         ,'nhdplus_version',  str_nhdplus_version
         ,'return_code',      int_return_code
         ,'status_message',   str_status_message
      );
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Exit if nothing returned from delineation
   ----------------------------------------------------------------------------
   IF int_flowline_count = 0
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'delineated_area',  NULL
         ,'aggregation_used', NULL
         ,'flowlines',        NULL
         ,'flowline_count',   NULL
         ,'nhdplus_version',  str_nhdplus_version
         ,'return_code',      -1
         ,'status_message',   'Navigation returned no results'
      );
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Build the delineated_area featurecollection
   ----------------------------------------------------------------------------
   IF str_aggregation_used = 'NONE'
   THEN
      json_delineated_area := (
         SELECT 
         JSONB_AGG(
            j.my_json ORDER BY j.my_json->'properties'->'nhdplusid'
         ) AS my_feats
         FROM (
            SELECT 
            JSONB_BUILD_OBJECT(
                'type',       'Feature'
               ,'obj_type',   'delineated_area_properties'
               ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
               ,'properties', TO_JSONB(t.*) - 'geom'
            ) AS my_json
            FROM (
               SELECT
                a.nhdplusid
               ,a.sourcefc
               ,a.hydroseq
               ,a.areasqkm
               ,CASE WHEN boo_return_delineation_geometry THEN ST_Transform(a.shape,4326) ELSE NULL::GEOMETRY END AS geom
               FROM
               tmp_catchments a
               WHERE
               a.sourcefc IS NULL
               ORDER BY
                a.nhdplusid
            ) t
         ) j
      );
   
   ELSE
      json_delineated_area := (
         SELECT 
         JSONB_AGG(
            j.my_json ORDER BY j.my_json->'properties'->'nhdplusid'
         ) AS my_feats
         FROM (
            SELECT 
            JSONB_BUILD_OBJECT(
                'type',       'Feature'
               ,'obj_type',   'delineated_area_properties'
               ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
               ,'properties', TO_JSONB(t.*) - 'geom'
            ) AS my_json
            FROM (
               SELECT
                a.nhdplusid
               ,a.sourcefc
               ,a.hydroseq
               ,a.areasqkm
               ,CASE WHEN boo_return_delineation_geometry THEN ST_Transform(a.shape,4326) ELSE NULL::GEOMETRY END AS geom
               FROM
               tmp_catchments a
               WHERE
               a.sourcefc  = 'AGGR'
               ORDER BY
                a.nhdplusid
            ) t
         ) j
      );
      
   END IF;
         
   IF json_delineated_area IS NULL
   OR JSONB_ARRAY_LENGTH(json_delineated_area) = 0
   THEN
      json_delineated_area := NULL;
      
   ELSE
      json_delineated_area := JSON_BUILD_OBJECT(
          'type'    , 'FeatureCollection'
         ,'features', json_delineated_area
      );
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Build the flowlines featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_flowlines
   THEN
      json_flowlines := (
         SELECT 
         JSONB_AGG(
            j.my_json ORDER BY j.my_json->'properties'->'nav_order',j.my_json->'properties'->'network_distancekm'
         ) AS my_feats
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
   -- Step 60
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSONB_BUILD_OBJECT(
       'delineated_area',  json_delineated_area
      ,'aggregation_used', str_aggregation_used
      ,'flowlines',        json_flowlines
      ,'flowline_count',   int_flowline_count
      ,'nhdplus_version',  str_nhdplus_version
      ,'return_code',      int_return_code
      ,'status_message',   str_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.delineate(
   JSONB
) OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.delineate(
   JSONB
) TO PUBLIC;

