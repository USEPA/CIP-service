--******************************--
----- functions/cipsrv_domains.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_domains';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.cipsrv_domains()
RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   json_states JSONB;
   json_tribes JSONB;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Collect the state domain values
   ----------------------------------------------------------------------------
   SELECT 
   JSON_AGG(a.*)
   INTO json_states
   FROM (
      SELECT
       aa.geoid
      ,aa.stusps
      ,aa.name
      FROM
      cipsrv_support.tiger_fedstatewaters aa
      ORDER BY
      aa.stusps
   ) a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Collect the tribes domain values
   ----------------------------------------------------------------------------
   SELECT 
   JSON_AGG(a.*)
   INTO json_tribes
   FROM (
      SELECT
       aa.aiannhns_stem
      ,aa.aiannhns_namelsad
      ,aa.has_reservation_lands
      ,aa.has_trust_lands
      FROM
      cipsrv_support.tribal_crosswalk aa
      ORDER BY
      aa.aiannhns_stem
   ) a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'states', json_states
      ,'tribes', json_tribes
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.cipsrv_domains() 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.cipsrv_domains() 
TO PUBLIC;

--******************************--
----- functions/cipsrv_index.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_index';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.cipsrv_index(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   rec                                RECORD;
   json_input                         JSONB := $1;
   json_points                        JSONB;
   json_lines                         JSONB;
   json_areas                         JSONB;
   json_geometry                      JSONB;
   ary_geometry_clip                  VARCHAR[];
   str_geometry_clip_stage            VARCHAR;
   ary_catchment_filter               VARCHAR[];
   str_nhdplus_version                VARCHAR;
   str_wbd_version                    VARCHAR;
   
   str_default_point_indexing_method  VARCHAR;
   
   str_default_line_indexing_method   VARCHAR;
   num_default_line_threshold         NUMERIC;
   
   str_default_ring_indexing_method   VARCHAR;
   num_default_ring_areacat_threshold NUMERIC;
   num_default_ring_areaevt_threshold NUMERIC;
   
   str_default_area_indexing_method   VARCHAR;
   num_default_areacat_threshold      NUMERIC;
   num_default_areaevt_threshold      NUMERIC;
   
   str_known_region                   VARCHAR;
   boo_return_indexed_features        BOOLEAN;
   boo_return_indexed_collection      BOOLEAN;
   boo_return_catchment_geometry      BOOLEAN;
   boo_return_flowlines               BOOLEAN;
   boo_return_huc12s                  BOOLEAN;
   boo_return_flowline_geometry       BOOLEAN;
   boo_return_huc12_geometry          BOOLEAN;
   boo_return_indexing_summary        BOOLEAN;
   int_catchment_count                INTEGER;
   num_catchment_areasqkm             NUMERIC;
   
   json_indexed_points                JSONB;
   json_indexed_lines                 JSONB;
   json_indexed_areas                 JSONB;
   sdo_indexed_collection             GEOMETRY;
   json_indexed_collection            JSONB;
   json_indexing_summary              JSONB;
   json_flowlines                     JSONB;
   json_catchments                    JSONB;
   json_huc12s                        JSONB;
   
   int_return_code                    INTEGER;
   str_status_message                 VARCHAR;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF JSONB_PATH_EXISTS(json_input,'$.points')
   AND json_input->>'points' IS NOT NULL
   AND json_input->>'points' != ''
   THEN
      json_points := json_input->'points';

   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.lines')
   AND json_input->>'lines' IS NOT NULL
   AND json_input->>'lines' != ''
   THEN
      json_lines := json_input->'lines';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.areas')
   AND json_input->>'areas' IS NOT NULL
   AND json_input->>'areas' != ''
   THEN
      json_areas := json_input->'areas';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.geometry')
   AND json_input->>'geometry' IS NOT NULL
   AND json_input->>'geometry' != ''
   THEN
      json_geometry := json_input->'geometry';
      
   END IF;

   IF  json_points   IS NULL
   AND json_lines    IS NULL
   AND json_areas    IS NULL
   AND json_geometry IS NULL
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'return_code', -10
         ,'status_message', 'input events required for cip indexing'
      );
            
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.geometry_clip')
   THEN
      IF jsonb_typeof(json_input->'geometry_clip') = 'array'
      THEN
         ary_geometry_clip := ARRAY(
            SELECT jsonb_array_elements_text(json_input->'geometry_clip')
         );
         
      END IF;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.geometry_clip_stage')
   AND json_input->>'geometry_clip_stage' IS NOT NULL
   THEN
      IF json_input->>'geometry_clip_stage' = ''
      OR json_input->>'geometry_clip_stage' = ' '
      THEN
         str_geometry_clip_stage := NULL;
         
      ELSE
         str_geometry_clip_stage := json_input->>'geometry_clip_stage';
         
      END IF;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.catchment_filter')
   THEN
      IF jsonb_typeof(json_input->'catchment_filter') = 'array'
      THEN
         ary_catchment_filter := ARRAY(
            SELECT jsonb_array_elements_text(json_input->'catchment_filter')
         );
         
      END IF;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code', -10
         ,'status_message', 'nhdplus_version required for cip indexing'
      );
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.wbd_version')
   AND json_input->>'wbd_version' IS NOT NULL
   THEN
      str_wbd_version := json_input->>'wbd_version';
      
   END IF;
   
   --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
   IF JSONB_PATH_EXISTS(json_input,'$.default_point_indexing_method')
   AND json_input->>'default_point_indexing_method' IS NOT NULL
   THEN
      str_default_point_indexing_method := json_input->>'default_point_indexing_method';
      
   ELSE
      str_default_point_indexing_method := 'point';
      
   END IF;
   
   --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
   IF JSONB_PATH_EXISTS(json_input,'$.default_line_indexing_method')
   AND json_input->>'default_line_indexing_method' IS NOT NULL
   THEN
      str_default_line_indexing_method := json_input->>'default_line_indexing_method';
      
   ELSE
      str_default_line_indexing_method := 'line';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.default_line_threshold')
   AND json_input->>'default_line_threshold' IS NOT NULL 
   THEN
      num_default_line_threshold := cipsrv_engine.json2numeric(json_input->'default_line_threshold');
      
   ELSE
      num_default_line_threshold := 10;
      
   END IF;
   
   --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
   IF JSONB_PATH_EXISTS(json_input,'$.default_ring_indexing_method')
   AND json_input->>'default_ring_indexing_method' IS NOT NULL
   THEN
      str_default_ring_indexing_method := json_input->>'default_ring_indexing_method';
      
   ELSE
      str_default_ring_indexing_method := 'area_simple';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.default_ring_areacat_threshold')
   AND json_input->>'default_ring_areacat_threshold' IS NOT NULL
   THEN
      num_default_ring_areacat_threshold := cipsrv_engine.json2numeric(json_input->'default_ring_areacat_threshold');
      
   ELSE
      num_default_ring_areacat_threshold := 10;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.default_ring_areaevt_threshold')
   AND json_input->>'default_ring_areaevt_threshold' IS NOT NULL
   THEN
      num_default_ring_areaevt_threshold := cipsrv_engine.json2numeric(json_input->'default_ring_areaevt_threshold');
      
   ELSE
      num_default_ring_areaevt_threshold := 10;
      
   END IF;
   
   --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
   IF JSONB_PATH_EXISTS(json_input,'$.default_area_indexing_method')
   AND json_input->>'default_area_indexing_method' IS NOT NULL
   THEN
      str_default_area_indexing_method := json_input->>'default_area_indexing_method';
      
   ELSE
      str_default_area_indexing_method := 'area_simple';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.default_areacat_threshold')
   AND json_input->>'default_areacat_threshold' IS NOT NULL
   THEN
      num_default_areacat_threshold := cipsrv_engine.json2numeric(json_input->'default_areacat_threshold');
      
   ELSE
      num_default_areacat_threshold := 10;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.default_areaevt_threshold')
   AND json_input->>'default_areaevt_threshold' IS NOT NULL
   THEN
      num_default_areaevt_threshold := cipsrv_engine.json2numeric(json_input->'default_areaevt_threshold');
      
   ELSE
      num_default_areaevt_threshold := 10;
      
   END IF;
   
   --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
   -- Allow known region override
   IF JSONB_PATH_EXISTS(json_input,'$.known_region')
   AND json_input->>'known_region' IS NOT NULL
   THEN
      str_known_region := json_input->>'known_region';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_indexed_features')
   AND json_input->>'return_indexed_features' IS NOT NULL
   THEN
      boo_return_indexed_features := (json_input->>'return_indexed_features')::BOOLEAN;
      
   ELSE
      boo_return_indexed_features := TRUE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_indexed_collection')
   AND json_input->>'return_indexed_collection' IS NOT NULL
   THEN
      boo_return_indexed_collection := (json_input->>'return_indexed_collection')::BOOLEAN;
      
   ELSE
      boo_return_indexed_collection := FALSE;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.return_catchment_geometry')
   AND json_input->>'return_catchment_geometry' IS NOT NULL
   THEN
      boo_return_catchment_geometry := (json_input->>'return_catchment_geometry')::BOOLEAN;
      
   ELSE
      boo_return_catchment_geometry := FALSE;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.return_indexing_summary')
   AND json_input->>'return_indexing_summary' IS NOT NULL
   THEN
      boo_return_indexing_summary := (json_input->>'return_indexing_summary')::BOOLEAN;
      
   ELSE
      boo_return_indexing_summary := TRUE;
      
   END IF;   
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_flowlines')
   AND json_input->>'return_flowlines' IS NOT NULL
   THEN
      boo_return_flowlines := (json_input->>'return_flowlines')::BOOLEAN;
      
   ELSE
      boo_return_flowlines := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_huc12s')
   AND json_input->>'return_huc12s' IS NOT NULL
   THEN
      boo_return_huc12s := (json_input->>'return_huc12s')::BOOLEAN;
      
   ELSE
      boo_return_huc12s := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_flowline_geometry')
   AND json_input->>'return_flowline_geometry' IS NOT NULL
   THEN
      boo_return_flowline_geometry := (json_input->>'return_flowline_geometry')::BOOLEAN;
      
   ELSE
      boo_return_flowline_geometry := FALSE;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.return_huc12_geometry')
   AND json_input->>'p_return_huc12_geometry' IS NOT NULL
   THEN
      boo_return_huc12_geometry := (json_input->>'p_return_huc12_geometry')::BOOLEAN;
      
   ELSE
      boo_return_huc12_geometry := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Call the indexing engine
   ----------------------------------------------------------------------------
   rec := cipsrv_engine.cipsrv_index(
       p_points                         := json_points
      ,p_lines                          := json_lines
      ,p_areas                          := json_areas
      ,p_geometry                       := json_geometry
      ,p_geometry_clip                  := ary_geometry_clip
      ,p_geometry_clip_stage            := str_geometry_clip_stage
      ,p_catchment_filter               := ary_catchment_filter
      ,p_nhdplus_version                := str_nhdplus_version
      ,p_wbd_version                    := str_wbd_version
      
      ,p_default_point_indexing_method  := str_default_point_indexing_method
      
      ,p_default_line_indexing_method   := str_default_line_indexing_method
      ,p_default_line_threshold         := num_default_line_threshold
      
      ,p_default_ring_indexing_method   := str_default_ring_indexing_method
      ,p_default_ring_areacat_threshold := num_default_ring_areacat_threshold
      ,p_default_ring_areaevt_threshold := num_default_ring_areaevt_threshold
      
      ,p_default_area_indexing_method   := str_default_area_indexing_method
      ,p_default_areacat_threshold      := num_default_areacat_threshold
      ,p_default_areaevt_threshold      := num_default_areaevt_threshold
      
      ,p_known_region                   := str_known_region
      ,p_return_indexed_features        := boo_return_indexed_features
      ,p_return_indexed_collection      := boo_return_indexed_collection
      ,p_return_catchment_geometry      := boo_return_catchment_geometry
      ,p_return_indexing_summary        := boo_return_indexing_summary
   );
   json_indexed_points      := rec.out_indexed_points;
   json_indexed_lines       := rec.out_indexed_lines;
   json_indexed_areas       := rec.out_indexed_areas;
   sdo_indexed_collection   := rec.out_indexed_collection;
   json_indexing_summary    := rec.out_indexing_summary;
   int_catchment_count      := rec.out_catchment_count;
   num_catchment_areasqkm   := rec.out_catchment_areasqkm;
   int_return_code          := rec.out_return_code;
   str_status_message       := rec.out_status_message;
   
   IF int_return_code != 0
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'indexed_points',     NULL
         ,'indexed_lines',      NULL
         ,'indexed_areas',      NULL
         ,'indexed_collection', NULL
         ,'indexing_summary',   NULL
         ,'catchment_count',    0
         ,'catchment_areasqkm', NULL
         ,'nhdplus_version',    str_nhdplus_version
         ,'catchments',         NULL
         ,'flowlines',          NULL
         ,'huc12s',             NULL
         ,'return_code',        int_return_code
         ,'status_message',     str_status_message
      );
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Exit if nothing returned from indexer
   ----------------------------------------------------------------------------
   IF int_catchment_count = 999990
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'indexed_points',     NULL
         ,'indexed_lines',      NULL
         ,'indexed_areas',      NULL
         ,'indexed_collection', NULL
         ,'indexing_summary',   NULL
         ,'catchment_count',    0
         ,'catchment_areasqkm', NULL
         ,'nhdplus_version',    str_nhdplus_version
         ,'catchments',         NULL
         ,'flowlines',          NULL
         ,'huc12s',             NULL
         ,'return_code',        -1
         ,'status_message',     'Indexer returned no results'
      );
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Build the flowlines featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_flowlines
   THEN
      IF str_nhdplus_version = 'nhdplus_m'
      THEN
         json_flowlines := (
            SELECT 
            JSONB_AGG(j.my_json) AS my_feats
            FROM (
               SELECT 
               JSONB_BUILD_OBJECT(
                   'type',       'Feature'
                  ,'obj_type',   'indexed_flowline_properties'
                  ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
                  ,'properties', TO_JSONB(t.*) - 'geom'
               ) AS my_json
               FROM (
                  SELECT
                   a.nhdplusid
                  ,a.gnis_id
                  ,a.gnis_name
                  ,a.reachcode
                  ,a.fmeasure
                  ,a.tmeasure
                  ,CASE
                   WHEN boo_return_flowline_geometry
                   THEN
                      ST_Transform(a.shape,4326)
                   ELSE
                      CAST(NULL AS GEOMETRY)
                   END AS geom
                   FROM
                   cipsrv_nhdplus_m.nhdflowline a
                   WHERE
                   EXISTS (SELECT 1 FROM tmp_cip_out b WHERE b.nhdplusid = a.nhdplusid)
                   ORDER BY
                   a.nhdplusid
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
         
      ELSIF str_nhdplus_version = 'nhdplus_h'
      THEN
         json_flowlines := (
            SELECT 
            JSONB_AGG(j.my_json) AS my_feats
            FROM (
               SELECT 
               JSONB_BUILD_OBJECT(
                   'type',       'Feature'
                  ,'obj_type',   'indexed_flowline_properties'
                  ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
                  ,'properties', TO_JSONB(t.*) - 'geom'
               ) AS my_json
               FROM (
                  SELECT
                   a.nhdplusid
                  ,a.gnis_id
                  ,a.gnis_name
                  ,a.reachcode
                  ,a.fmeasure
                  ,a.tmeasure
                  ,CASE
                   WHEN boo_return_flowline_geometry
                   THEN
                      ST_Transform(a.shape,4326)
                   ELSE
                      CAST(NULL AS GEOMETRY)
                   END AS geom
                   FROM
                   cipsrv_nhdplus_h.nhdflowline a
                   WHERE
                   EXISTS (SELECT 1 FROM tmp_cip_out b WHERE b.nhdplusid = a.nhdplusid)
                   ORDER BY
                   a.nhdplusid
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
         RAISE EXCEPTION 'err';
         
      END IF;
   
   ELSE
      json_flowlines := NULL;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Build the huc12 featurecollection
   ----------------------------------------------------------------------------
   json_huc12s := NULL;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Build the catchments featurecollection
   ----------------------------------------------------------------------------
   json_catchments := (
      SELECT 
      JSONB_AGG(j.my_json) AS my_feats
      FROM (
         SELECT 
         JSONB_BUILD_OBJECT(
             'type',       'Feature'
            ,'obj_type',   'indexed_catchment_properties'
            ,'geometry',   ST_AsGeoJSON(t.geom)::JSONB
            ,'properties', TO_JSONB(t.*) - 'geom'
         ) AS my_json
         FROM (
            SELECT
             a.nhdplusid
            ,a.catchmentstatecode
            ,a.xwalk_huc12
            ,a.areasqkm
            ,a.istribal
            ,a.istribal_areasqkm
            ,CASE WHEN boo_return_catchment_geometry
             THEN
               ST_Transform(ST_ForcePolygonCCW(a.shape),4326) 
             ELSE
               NULL::GEOMETRY
             END AS geom
            FROM
            tmp_cip_out a
            ORDER BY
             a.catchmentstatecode
            ,a.nhdplusid
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
      
   ----------------------------------------------------------------------------
   -- Step 60
   -- Return what we got
   ----------------------------------------------------------------------------
   IF sdo_indexed_collection IS NOT NULL
   THEN
      json_indexed_collection := JSONB_BUILD_OBJECT(
          'type'     ,'Feature'
         ,'geometry' ,ST_AsGeoJSON(sdo_indexed_collection)::JSONB
       );
       
   END IF; 
   
   RETURN JSONB_BUILD_OBJECT(
       'indexed_points',     json_indexed_points
      ,'indexed_lines',      json_indexed_lines
      ,'indexed_areas',      json_indexed_areas
      ,'indexed_collection', json_indexed_collection
      ,'indexing_summary',   json_indexing_summary      
      ,'catchment_count',    int_catchment_count
      ,'catchment_areasqkm', num_catchment_areasqkm
      ,'nhdplus_version',    str_nhdplus_version
      ,'catchments',         json_catchments
      ,'flowlines',          json_flowlines
      ,'huc12s',             json_huc12s
      ,'return_code',        int_return_code
      ,'status_message',     str_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.cipsrv_index(
   JSONB
) OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.cipsrv_index(
   JSONB
) TO PUBLIC;

--******************************--
----- functions/cipsrv_nav.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_nav';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

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
            ,ST_Transform(a.shape,4326) AS geom
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

--******************************--
----- functions/point_catreach_index.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.point_catreach_index';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.point_catreach_index(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   rec                               RECORD;
   json_input                        JSONB := $1;
   sdo_point                         GEOMETRY;
   boo_return_snap_path              BOOLEAN;
   int_return_code                   INTEGER;
   str_status_message                VARCHAR;
   str_known_region                  VARCHAR;
   str_nhdplus_version               VARCHAR;
   int_srid                          INTEGER;
   int_nhdplusid                     BIGINT;
   int_hydroseq                      BIGINT;
   int_fcode                         INTEGER;
   str_istribal                      VARCHAR;
   boo_isnavigable                   BOOLEAN;
   boo_iscoastal                     BOOLEAN;
   boo_isocean                       BOOLEAN;
   num_areasqkm                      NUMERIC;
   str_permanent_identifier          VARCHAR;
   str_reachcode                     VARCHAR;
   num_fmeasure                      NUMERIC;
   num_tmeasure                      NUMERIC;
   num_lengthkm                      NUMERIC;
   num_snap_measure                  NUMERIC;
   num_snap_distancekm               NUMERIC;
   sdo_flowline                      GEOMETRY;
   sdo_snap_point                    GEOMETRY;
   sdo_snap_path                     GEOMETRY;
   jsonb_snap_point                  JSONB;
   jsonb_snap_path                   JSONB;
   
BEGIN
   
   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   IF JSONB_PATH_EXISTS(json_input,'$.point')
   AND json_input->'point' IS NOT NULL
   AND 
   THEN
      sdo_point := cipsrv_engine.json2geometry(json_input->'point');
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    -10
         ,'status_message', 'input point is required.'
      );
   
   END IF;
   
   IF sdo_point IS NULL
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    -10
         ,'status_message', 'valid input point is required.'
      );
   
   ELSIF ST_GeometryType(sdo_point) != 'ST_Point'
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    -20
         ,'status_message', 'geometry must be single point.'
      );
   
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code', -10
         ,'status_message', 'nhdplus_version required.'
      );
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_snap_path')
   AND json_input->>'return_snap_path' IS NOT NULL
   THEN
      boo_return_snap_path := (json_input->>'return_snap_path')::BOOLEAN;
      
   ELSE
      boo_return_snap_path := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.known_region')
   AND json_input->>'known_region' IS NOT NULL
   THEN
      str_known_region := json_input->>'known_region';
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine proper location grid
   --------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cipsrv_nhdplus_m.determine_grid_srid(
          p_geometry       := sdo_point
         ,p_known_region   := str_known_region
      );
      int_srid           := rec.out_srid;
      int_return_code    := rec.out_return_code;
      str_status_message := rec.out_status_message;
      
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cipsrv_nhdplus_h.determine_grid_srid(
          p_geometry       := sdo_point
         ,p_known_region   := str_known_region
      );
      int_srid           := rec.out_srid;
      int_return_code    := rec.out_return_code;
      str_status_message := rec.out_status_message;
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code', -10
         ,'status_message', 'invalid nhdplus_version.'
      );
   
   END IF;
   
   IF int_return_code != 0
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    int_return_code
         ,'status_message', str_status_message
      );
   
   END IF;
   
   sdo_point := ST_Transform(sdo_point,int_srid);
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Call the engine
   --------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      IF int_srid = 5070
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_5070 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 3338
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_3338 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 26904
      THEN
         SELECT
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_26904 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32161
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_32161 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32655
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_32655 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32702
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_32702 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSE
         RAISE EXCEPTION 'err %',int_srid;
      
      END IF;
   
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      IF int_srid = 5070
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_5070 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 3338
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_3338 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 26904
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_26904 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32161
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_32161 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32655
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_32655 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32702
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_32702 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSE
         RAISE EXCEPTION 'err %',int_srid;
      
      END IF;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Search for the flowline reach measure
   --------------------------------------------------------------------------
   IF boo_isnavigable 
   OR boo_iscoastal
   THEN
      IF str_nhdplus_version = 'nhdplus_m'
      THEN
         SELECT
          a.permanent_identifier
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,ST_Transform(a.shape,int_srid)
         INTO
          str_permanent_identifier
         ,str_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,num_lengthkm
         ,sdo_flowline
         FROM
         cipsrv_nhdplus_m.nhdflowline a
         WHERE
         a.nhdplusid = int_nhdplusid;
         
      ELSIF str_nhdplus_version = 'nhdplus_h'
      THEN
         SELECT
          a.permanent_identifier
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,ST_Transform(a.shape,int_srid)
         INTO
          str_permanent_identifier
         ,str_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,num_lengthkm
         ,sdo_flowline
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
         a.nhdplusid = int_nhdplusid;
         
      ELSE
         RAISE EXCEPTION 'err %',str_nhdplus_version;
   
      END IF;
      
      sdo_flowline := ST_Transform(sdo_flowline,int_srid);
      
      num_snap_measure := ROUND(
          ST_InterpolatePoint(
             sdo_flowline
            ,sdo_point
          )::NUMERIC
         ,5
      );
      
      IF num_snap_measure IS NOT NULL
      THEN
         sdo_snap_point := ST_Force2D(
            ST_GeometryN(
                ST_LocateAlong(
                  sdo_flowline
                 ,num_snap_measure
                )
               ,1
            )
         );
         
         num_snap_distancekm := ST_Distance(
             ST_Transform(sdo_point,4326)::GEOGRAPHY
            ,ST_Transform(sdo_snap_point,4326)::GEOGRAPHY
         ) / 1000;
         
         IF boo_return_snap_path
         AND num_snap_distancekm > 0.00005
         THEN
            sdo_snap_path := ST_MakeLine(
                sdo_point
               ,sdo_snap_point
            );
      
         END IF;
         
      END IF;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Return what we got
   --------------------------------------------------------------------------
   IF sdo_snap_point IS NOT NULL
   THEN
      jsonb_snap_point := JSONB_BUILD_OBJECT(
          'type'      ,'Feature'
         ,'geometry'  ,ST_AsGeoJSON(ST_Transform(sdo_snap_point,4326))::JSONB
         ,'obj_type'  ,'snap_point_properties'
         ,'properties',JSONB_BUILD_OBJECT(
             'nhdplusid'  ,int_nhdplusid
            ,'reachcode'  ,str_reachcode
            ,'measure'    ,num_snap_measure
         )
      );
      
   END IF;
   
   IF sdo_snap_path IS NOT NULL
   THEN
      jsonb_snap_path := JSONB_BUILD_OBJECT(
          'type'      ,'Feature'
         ,'geometry'  ,ST_AsGeoJSON(ST_Transform(sdo_snap_path,4326))::JSONB
         ,'obj_type'  ,'snap_path_properties'
         ,'properties',JSONB_BUILD_OBJECT(
            'lengthkm', num_snap_distancekm
         )
      );
      
   END IF;
   
   RETURN JSONB_BUILD_OBJECT(
       'nhdplusid'           ,int_nhdplusid
      ,'hydroseq'            ,int_hydroseq
      ,'permanent_identifier',str_permanent_identifier
      ,'reachcode'           ,str_reachcode
      ,'fcode'               ,int_fcode
      ,'isnavigable'         ,boo_isnavigable
      ,'snap_measure'        ,num_snap_measure
      ,'snap_distancekm'     ,num_snap_distancekm
      ,'snap_point'          ,jsonb_snap_point
      ,'snap_path'           ,jsonb_snap_path
      ,'return_code'         ,int_return_code
      ,'status_message'      ,str_status_message
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.point_catreach_index(
   JSONB
) OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.point_catreach_index(
   JSONB
) TO PUBLIC;

--******************************--
----- functions/randomnav.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.randomnav';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.randomnav(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$
DECLARE
   rec                 RECORD;
   json_input          JSONB := $1;
   str_region          VARCHAR;
   str_nhdplus_version VARCHAR;
   boo_return_geometry BOOLEAN;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------   
   IF JSONB_PATH_EXISTS(json_input,'$.region')
   AND json_input->>'region' IS NOT NULL
   AND json_input->>'region' != ''
   THEN
      str_region := json_input->>'region';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   AND json_input->>'nhdplus_version' != ''
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      str_nhdplus_version := 'NHDPLUS_H';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_geometry')
   AND json_input->>'return_geometry' IS NOT NULL
   THEN
      boo_return_geometry := (json_input->>'return_geometry')::BOOLEAN;
      
   ELSE
      boo_return_geometry := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Get the results
   ----------------------------------------------------------------------------
   IF UPPER(str_nhdplus_version) IN ('NHDPLUS_M','MR')
   THEN
      rec := cipsrv_nhdplus_m.randomnav(
          p_region          := str_region
         ,p_return_geometry := boo_return_geometry
      );
      str_nhdplus_version := 'nhdplus_m';
   
   ELSIF UPPER(str_nhdplus_version) IN ('NHDPLUS_H','HR')
   THEN
      rec := cipsrv_nhdplus_h.randomnav(
          p_region          := str_region
         ,p_return_geometry := boo_return_geometry
      );
      str_nhdplus_version := 'nhdplus_h';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'nhdplusid'      ,rec.out_nhdplusid
      ,'reachcode'      ,rec.out_reachcode
      ,'measure'        ,rec.out_measure
      ,'nhdplus_version',str_nhdplus_version
      ,'shape'          ,ST_AsGeoJSON(ST_Transform(rec.out_shape,4326))::JSONB
      ,'return_code'    ,rec.out_return_code
      ,'status_message' ,rec.out_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.randomnav(
   JSONB
) 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.randomnav(
   JSONB
) 
TO PUBLIC;

--******************************--
----- functions/randomppnav.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.randomppnav';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.randomppnav(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$
DECLARE
   rec                 RECORD;
   json_input          JSONB := $1;
   str_region          VARCHAR;
   str_nhdplus_version VARCHAR;
   boo_return_geometry BOOLEAN;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------   
   IF JSONB_PATH_EXISTS(json_input,'$.region')
   AND json_input->>'region' IS NOT NULL
   AND json_input->>'region' != ''
   THEN
      str_region := json_input->>'region';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   AND json_input->>'nhdplus_version' != ''
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      str_nhdplus_version := 'NHDPLUS_H';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_geometry')
   AND json_input->>'return_geometry' IS NOT NULL
   THEN
      boo_return_geometry := (json_input->>'return_geometry')::BOOLEAN;
      
   ELSE
      boo_return_geometry := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Get the results
   ----------------------------------------------------------------------------
   IF UPPER(str_nhdplus_version) IN ('NHDPLUS_M','MR')
   THEN
      rec := cipsrv_nhdplus_m.randomppnav(
          p_region          := str_region
         ,p_return_geometry := boo_return_geometry
      );
      str_nhdplus_version := 'nhdplus_m';
   
   ELSIF UPPER(str_nhdplus_version) IN ('NHDPLUS_H','HR')
   THEN
      rec := cipsrv_nhdplus_h.randomppnav(
          p_region          := str_region
         ,p_return_geometry := boo_return_geometry
      );
      str_nhdplus_version := 'nhdplus_h';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'nhdplusid1'     ,rec.out_nhdplusid1
      ,'reachcode1'     ,rec.out_reachcode1
      ,'measure1'       ,rec.out_measure1
      ,'shape1'         ,ST_AsGeoJSON(ST_Transform(rec.out_shape1,4326))::JSONB
      ,'nhdplusid2'     ,rec.out_nhdplusid2
      ,'reachcode2'     ,rec.out_reachcode2
      ,'measure2'       ,rec.out_measure2
      ,'shape2'         ,ST_AsGeoJSON(ST_Transform(rec.out_shape2,4326))::JSONB
      ,'nhdplus_version',str_nhdplus_version
      ,'return_code'    ,rec.out_return_code
      ,'status_message' ,rec.out_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.randomppnav(
   JSONB
) 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.randomppnav(
   JSONB
) 
TO PUBLIC;

--******************************--
----- functions/randomcatchment.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.randomcatchment';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.randomcatchment(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$
DECLARE
   rec                  RECORD;
   json_input           JSONB := $1;
   str_region           VARCHAR;
   str_nhdplus_version  VARCHAR;
   boo_include_extended BOOLEAN;
   boo_return_geometry  BOOLEAN;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------   
   IF JSONB_PATH_EXISTS(json_input,'$.region')
   AND json_input->>'region' IS NOT NULL
   AND json_input->>'region' != ''
   THEN
      str_region := json_input->>'region';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   AND json_input->>'nhdplus_version' != ''
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      str_nhdplus_version := 'NHDPLUS_H';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.include_extended')
   AND json_input->>'include_extended' IS NOT NULL
   THEN
      boo_include_extended := (json_input->>'include_extended')::BOOLEAN;
      
   ELSE
      boo_include_extended := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_geometry')
   AND json_input->>'return_geometry' IS NOT NULL
   THEN
      boo_return_geometry := (json_input->>'return_geometry')::BOOLEAN;
      
   ELSE
      boo_return_geometry := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Get the results
   ----------------------------------------------------------------------------
   IF UPPER(str_nhdplus_version) IN ('NHDPLUS_M','MR')
   THEN
      rec := cipsrv_nhdplus_m.randomcatchment(
          p_region           := str_region
         ,p_include_extended := boo_include_extended
         ,p_return_geometry  := boo_return_geometry
      );
      str_nhdplus_version := 'nhdplus_m';
   
   ELSIF UPPER(str_nhdplus_version) IN ('NHDPLUS_H','HR')
   THEN
      rec := cipsrv_nhdplus_h.randomcatchment(
          p_region           := str_region
         ,p_include_extended := boo_include_extended
         ,p_return_geometry  := boo_return_geometry
      );
      str_nhdplus_version := 'nhdplus_h';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'nhdplusid'          ,rec.out_nhdplusid
      ,'areasqkm'           ,rec.out_areasqkm
      ,'catchmentstatecodes',rec.out_catchmentstatecodes
      ,'nhdplus_version'    ,str_nhdplus_version
      ,'shape'              ,ST_AsGeoJSON(ST_Transform(rec.out_shape,4326))::JSONB
      ,'centroid'           ,ST_AsGeoJSON(ST_Transform(rec.out_centroid,4326))::JSONB
      ,'return_code'        ,rec.out_return_code
      ,'status_message'     ,rec.out_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.randomcatchment(
   JSONB
) 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.randomcatchment(
   JSONB
) 
TO PUBLIC;

--******************************--
----- functions/randompoint.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.randompoint';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.randompoint(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$
DECLARE
   rec                  RECORD;
   json_input           JSONB := $1;
   str_region           VARCHAR;
   str_nhdplus_version  VARCHAR;
   boo_include_extended BOOLEAN;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------   
   IF JSONB_PATH_EXISTS(json_input,'$.region')
   AND json_input->>'region' IS NOT NULL
   AND json_input->>'region' != ''
   THEN
      str_region := json_input->>'region';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   AND json_input->>'nhdplus_version' != ''
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      str_nhdplus_version := 'NHDPLUS_H';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.include_extended')
   AND json_input->>'include_extended' IS NOT NULL
   THEN
      boo_include_extended := (json_input->>'include_extended')::BOOLEAN;
      
   ELSE
      boo_include_extended := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Get the results
   ----------------------------------------------------------------------------
   IF UPPER(str_nhdplus_version) IN ('NHDPLUS_M','MR')
   THEN
      rec := cipsrv_nhdplus_m.randompoint(
          p_region           := str_region
         ,p_include_extended := boo_include_extended
      );
      str_nhdplus_version := 'nhdplus_m';
   
   ELSIF UPPER(str_nhdplus_version) IN ('NHDPLUS_H','HR')
   THEN
      rec := cipsrv_nhdplus_h.randompoint(
          p_region           := str_region
         ,p_include_extended := boo_include_extended
      );
      str_nhdplus_version := 'nhdplus_h';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'nhdplus_version'    ,str_nhdplus_version
      ,'shape'              ,ST_AsGeoJSON(ST_Transform(rec.out_shape,4326))::JSONB
      ,'return_code'        ,rec.out_return_code
      ,'status_message'     ,rec.out_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.randompoint(
   JSONB
) 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.randompoint(
   JSONB
) 
TO PUBLIC;

--******************************--
----- functions/randomhuc12.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.randomhuc12';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.randomhuc12(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$
DECLARE
   rec                 RECORD;
   json_input          JSONB := $1;
   str_region          VARCHAR;
   str_source_dataset  VARCHAR;
   boo_return_geometry BOOLEAN;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------   
   IF JSONB_PATH_EXISTS(json_input,'$.region')
   AND json_input->>'region' IS NOT NULL
   AND json_input->>'region' != ''
   THEN
      str_region := json_input->>'region';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.source_dataset')
   AND json_input->>'source_dataset' IS NOT NULL
   AND json_input->>'source_dataset' != ''
   THEN
      str_source_dataset := json_input->>'source_dataset';
      
   ELSE
      str_source_dataset := 'NP21';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_geometry')
   AND json_input->>'return_geometry' IS NOT NULL
   THEN
      boo_return_geometry := (json_input->>'return_geometry')::BOOLEAN;
      
   ELSE
      boo_return_geometry := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Get the results
   ----------------------------------------------------------------------------
   rec := cipsrv_support.randomhuc12(
       p_region          := str_region
      ,p_source_dataset  := str_source_dataset
      ,p_return_geometry := boo_return_geometry
   );
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'huc12'          ,rec.out_huc12
      ,'huc12_name'     ,rec.out_huc12_name
      ,'source_dataset' ,rec.out_source_dataset
      ,'shape'          ,ST_AsGeoJSON(ST_Transform(rec.out_shape,4326))::JSONB
      ,'return_code'    ,rec.out_return_code
      ,'status_message' ,rec.out_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.randomhuc12(
   JSONB
) 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.randomhuc12(
   JSONB
) 
TO PUBLIC;

--******************************--
----- functions/healthcheck.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.healthcheck';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.healthcheck()
RETURNS JSONB
IMMUTABLE
AS
$BODY$ 
DECLARE
BEGIN
   
   RETURN JSONB_BUILD_OBJECT(
       'status', 'ok'
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.healthcheck() 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.healthcheck() 
TO PUBLIC;

