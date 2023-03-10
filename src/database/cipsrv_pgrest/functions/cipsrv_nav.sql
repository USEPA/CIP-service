CREATE OR REPLACE FUNCTION cipsrv_pgrest.cipsrv_nav(
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
   
   boo_return_flowline_details       BOOLEAN;
   boo_return_flowline_geometry      BOOLEAN;
   int_grid_srid                     INTEGER;
   int_flowline_count                INTEGER;
   int_return_code                   INTEGER;
   str_status_message                VARCHAR;
   
   json_flowlines                    JSONB;
   
BEGIN
   
   int_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
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
   THEN
      str_search_type := json_input->>'search_type';

   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_nhdplusid')
   AND json_input->'start_nhdplusid' IS NOT NULL
   THEN
      int_start_nhdplusid := cipsrv_engine.json2bigint(json_input->'start_nhdplusid');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_permanent_identifier')
   AND json_input->>'start_permanent_identifier' IS NOT NULL
   THEN
      str_start_permanent_identifier := json_input->>'start_permanent_identifier';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_reachcode')
   AND json_input->>'start_reachcode' IS NOT NULL
   THEN
      str_start_reachcode := json_input->>'start_reachcode';
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.start_hydroseq')
   AND json_input->'start_hydroseq' IS NOT NULL
   THEN
      int_start_hydroseq := cipsrv_engine.json2numeric(json_input->'start_hydroseq');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.start_measure')
   AND json_input->'start_measure' IS NOT NULL
   THEN
      num_start_measure := cipsrv_engine.json2numeric(json_input->'start_measure');
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.stop_nhdplusid')
   AND json_input->'stop_nhdplusid' IS NOT NULL
   THEN
      int_stop_nhdplusid := cipsrv_engine.json2bigint(json_input->'stop_nhdplusid');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_permanent_identifier')
   AND json_input->>'stop_permanent_identifier' IS NOT NULL
   THEN
      str_stop_permanent_identifier := json_input->>'stop_permanent_identifier';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_reachcode')
   AND json_input->>'stop_reachcode' IS NOT NULL
   THEN
      str_stop_reachcode := json_input->>'stop_reachcode';
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.stop_hydroseq')
   AND json_input->'stop_hydroseq' IS NOT NULL
   THEN
      int_stop_hydroseq := cipsrv_engine.json2bigint(json_input->'stop_hydroseq');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.stop_measure')
   AND json_input->'stop_measure' IS NOT NULL
   THEN
      num_stop_measure := cipsrv_engine.json2numeric(json_input->'stop_measure');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.max_distancekm')
   AND json_input->'max_distancekm' IS NOT NULL 
   THEN
      num_max_distancekm := cipsrv_engine.json2numeric(json_input->'max_distancekm');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.max_flowtimeday')
   AND json_input->'max_flowtimeday' IS NOT NULL 
   THEN
      num_max_flowtimeday := cipsrv_engine.json2numeric(json_input->'max_flowtimeday');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_flowline_details')
   AND json_input->'return_flowline_details' IS NOT NULL
   THEN
      boo_return_flowline_details := (json_input->'return_flowline_details')::BOOLEAN;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_flowline_geometry')
   AND json_input->'return_flowline_geometry' IS NOT NULL
   THEN
      boo_return_flowline_geometry := (json_input->'return_flowline_geometry')::BOOLEAN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Call the indexing engine
   ----------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cipsrv_nhdplus_m.navigate(
          p_search_type                := str_search_type
         ,p_start_nhdplusid            := int_start_nhdplusid
         ,p_start_permanent_identifier := str_start_permanent_identifier
         ,p_start_reachcode            := str_start_reachcode
         ,p_start_hydroseq             := int_start_hydroseq
         ,p_start_measure              := num_start_measure
         ,p_stop_nhdplusid             := int_stop_nhdplusid
         ,p_stop_permanent_identifier  := str_stop_permanent_identifier
         ,p_stop_reachcode             := str_stop_reachcode
         ,p_stop_hydroseq              := int_stop_hydroseq
         ,p_stop_measure               := num_stop_measure
         ,p_max_distancekm             := num_max_distancekm
         ,p_max_flowtimeday            := num_max_flowtimeday
         ,p_return_flowline_details    := boo_return_flowline_details
         ,p_return_flowline_geometry   := boo_return_flowline_geometry
      );
      int_start_nhdplusid := rec.out_start_nhdplusid;
      str_start_permanent_identifier := rec.out_start_permanent_identifier;
      num_start_measure   := rec.out_start_measure;
      int_grid_srid       := rec.out_grid_srid;
      int_stop_nhdplusid  := rec.out_stop_nhdplusid;
      num_stop_measure    := rec.out_stop_measure;
      int_flowline_count  := rec.out_flowline_count;
      int_return_code     := rec.out_return_code;
      str_status_message  := rec.out_status_message;
      
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cipsrv_nhdplus_h.navigate(
          p_search_type                := str_search_type
         ,p_start_nhdplusid            := int_start_nhdplusid
         ,p_start_permanent_identifier := str_start_permanent_identifier
         ,p_start_reachcode            := str_start_reachcode
         ,p_start_hydroseq             := int_start_hydroseq
         ,p_start_measure              := num_start_measure
         ,p_stop_nhdplusid             := int_stop_nhdplusid
         ,p_stop_permanent_identifier  := str_stop_permanent_identifier
         ,p_stop_reachcode             := str_stop_reachcode
         ,p_stop_hydroseq              := int_stop_hydroseq
         ,p_stop_measure               := num_stop_measure
         ,p_max_distancekm             := num_max_distancekm
         ,p_max_flowtimeday            := num_max_flowtimeday
         ,p_return_flowline_details    := boo_return_flowline_details
         ,p_return_flowline_geometry   := boo_return_flowline_geometry
      );
      int_start_nhdplusid := rec.out_start_nhdplusid;
      str_start_permanent_identifier := rec.out_start_permanent_identifier;
      num_start_measure   := rec.out_start_measure;
      int_grid_srid       := rec.out_grid_srid;
      int_stop_nhdplusid  := rec.out_stop_nhdplusid;
      num_stop_measure    := rec.out_stop_measure;
      int_flowline_count  := rec.out_flowline_count;
      int_return_code     := rec.out_return_code;
      str_status_message  := rec.out_status_message;
   
   ELSE
      RAISE EXCEPTION 'err %',str_nhdplus_version;
      
   END IF;
   
   IF int_return_code != 0
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'flowlines',       NULL
         ,'flowline_count',  NULL
         ,'nhdplus_version', str_nhdplus_version
         ,'return_code',     int_return_code
         ,'status_message',  str_status_message
      );
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Exit if nothing returned from navigation
   ----------------------------------------------------------------------------
   IF int_flowline_count = 0
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'flowlines',       NULL
         ,'flowline_count',  NULL
         ,'nhdplus_version', str_nhdplus_version
         ,'return_code',     -1
         ,'status_message',  'Navigation returned no results'
      );
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Build the flowlines featurecollection
   ----------------------------------------------------------------------------
   json_flowlines := (
      SELECT JSON_AGG(ST_AsGeoJSON(t.*)::JSON)
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
         ,ST_Transform(a.shape,4326) AS geom
         FROM
         tmp_navigation_results a
         ORDER BY
          a.nav_order
         ,a.network_distancekm
      ) t
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
      
   ----------------------------------------------------------------------------
   -- Step 50
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSONB_BUILD_OBJECT(
       'flowlines',       json_flowlines
      ,'flowline_count',  int_flowline_count
      ,'nhdplus_version', str_nhdplus_version
      ,'return_code',     int_return_code
      ,'status_message',  str_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.cipsrv_nav(
   JSONB
) OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.cipsrv_nav(
   JSONB
) TO PUBLIC;

