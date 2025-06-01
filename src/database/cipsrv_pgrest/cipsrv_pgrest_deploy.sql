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
   JSONB_AGG(a.* ORDER BY a.stusps)
   INTO json_states
   FROM (
      SELECT
       aa.geoid
      ,aa.stusps
      ,aa.name
      FROM
      cipsrv_support.tiger_fedstatewaters aa
   ) a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Collect the tribes domain values
   ----------------------------------------------------------------------------
   SELECT 
   JSONB_AGG(a.* ORDER BY a.aiannhns_stem)
   INTO json_tribes
   FROM (
      SELECT
       aa.aiannhns_stem
      ,aa.aiannhns_namelsad
      ,aa.has_reservation_lands
      ,aa.has_trust_lands
      FROM
      cipsrv_support.tribal_crosswalk aa
   ) a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSONB_BUILD_OBJECT(
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
   boo_return_full_catchments         BOOLEAN;
   
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
      str_default_point_indexing_method := 'point_simple';
      
   END IF;
   
   --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++--
   IF JSONB_PATH_EXISTS(json_input,'$.default_line_indexing_method')
   AND json_input->>'default_line_indexing_method' IS NOT NULL
   THEN
      str_default_line_indexing_method := json_input->>'default_line_indexing_method';
      
   ELSE
      str_default_line_indexing_method := 'line_simple';
      
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
   AND json_input->>'return_huc12_geometry' IS NOT NULL
   THEN
      boo_return_huc12_geometry := (json_input->>'return_huc12_geometry')::BOOLEAN;
      
   ELSE
      boo_return_huc12_geometry := FALSE;
      
   END IF;
 
   IF JSONB_PATH_EXISTS(json_input,'$.return_full_catchments')
   AND json_input->>'return_full_catchments' IS NOT NULL
   THEN
      boo_return_full_catchments := (json_input->>'return_full_catchments')::BOOLEAN;
      
   ELSE
      boo_return_full_catchments := FALSE;
      
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
      ,p_return_full_catchments         := boo_return_full_catchments
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
            JSONB_AGG(
               j.my_json ORDER BY j.my_json->'properties'->'nhdplusid'
            ) AS my_feats
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
                  ,a.frommeas AS fmeasure
                  ,a.tomeas   AS tmeasure
                  ,CASE
                   WHEN boo_return_flowline_geometry
                   THEN
                      ST_Transform(a.shape,4326)
                   ELSE
                      CAST(NULL AS GEOMETRY)
                   END AS geom
                   FROM
                   cipsrv_nhdplus_m.networknhdflowline a
                   WHERE
                   EXISTS (SELECT 1 FROM tmp_cip_out b WHERE b.nhdplusid = a.nhdplusid)
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
            JSONB_AGG(
               j.my_json ORDER BY j.my_json->'properties'->'nhdplusid'
            ) AS my_feats
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
                  ,a.frommeas AS fmeasure
                  ,a.tomeas   AS tmeasure
                  ,CASE
                   WHEN boo_return_flowline_geometry
                   THEN
                      ST_Transform(a.shape,4326)
                   ELSE
                      CAST(NULL AS GEOMETRY)
                   END AS geom
                   FROM
                   cipsrv_nhdplus_h.networknhdflowline a
                   WHERE
                   EXISTS (SELECT 1 FROM tmp_cip_out b WHERE b.nhdplusid = a.nhdplusid)
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
      JSONB_AGG(
         j.my_json ORDER BY j.my_json->'properties'->'catchmentstatecode',j.my_json->'properties'->'nhdplusid'
      ) AS my_feats
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
            ,a.isnavigable
            ,CASE WHEN boo_return_catchment_geometry
             THEN
               ST_Transform(ST_ForcePolygonCCW(a.shape),4326) 
             ELSE
               NULL::GEOMETRY
             END AS geom
            FROM
            tmp_cip_out a
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
----- functions/cipsrv_registry.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_registry';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.cipsrv_registry()
RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   json_components JSONB;
   str_version     VARCHAR;
   str_username    VARCHAR;
   dat_installed   DATE;
   str_notes       VARCHAR;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Collect the state domain values
   ----------------------------------------------------------------------------
   json_components := NULL;
   SELECT 
   JSON_AGG(a.*)
   INTO json_components
   FROM (
      SELECT
       aa.component
      ,aa.component_type
      ,aa.component_vintage
      ,aa.installer_username
      ,aa.installation_date
      ,aa.notes
      FROM
      cipsrv.registry aa
      ORDER BY
      aa.component
   ) a;
   
   IF json_components IS NULL
   THEN
      json_components := '[]'::JSONB;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Collect the components
   ----------------------------------------------------------------------------
   BEGIN
      SELECT
       a.cipsrv_version
      ,a.installer_username
      ,a.installation_date
      ,a.notes
      INTO
       str_version
      ,str_username
      ,dat_installed
      ,str_notes
      FROM
      cipsrv.version a
      LIMIT 1;
   
   EXCEPTION
      WHEN NO_DATA_FOUND THEN NULL;
      WHEN OTHERS THEN RAISE;
   
   END;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'components'       , json_components
      ,'version'          , str_version
      ,'installation_date', dat_installed
      ,'notes'            , str_notes
   );
      
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_registry';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv_pgrest',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv_pgrest',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;

--******************************--
----- functions/cipsrv_owld_programs.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_owld_programs';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.cipsrv_owld_programs()
RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   json_program  JSONB;
   json_array    JSONB;
   rec           RECORD;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Collect the available owld programs in system
   ----------------------------------------------------------------------------
   json_program := NULL;
   
   IF EXISTS(
      SELECT 1 FROM information_schema.routines a
      WHERE a.routine_schema = 'cipsrv_owld' AND a.routine_name = 'owld_programs'
   )
   THEN
      FOR rec IN
      SELECT a.* FROM cipsrv_owld.owld_programs() a
      LOOP
         json_program := ROW_TO_JSON(rec);
         
         IF json_array IS NULL
         THEN
            json_array := JSONB_BUILD_ARRAY(
               json_program
            );
            
         ELSE
            json_array := json_array || json_program;
            
         END IF;
         
      END LOOP;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return what we got
   ----------------------------------------------------------------------------
   IF json_array IS NULL
   THEN
      json_array := '[]'::JSONB;
      
   END IF;
   
   RETURN JSON_BUILD_OBJECT(
       'programs', json_array
   );
      
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.cipsrv_owld_programs';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv_pgrest',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv_pgrest',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;

--******************************--
----- functions/delineate.sql 

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

--******************************--
----- functions/navigate.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.navigate';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.navigate(
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
   -- Call the navigation engine
   ----------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
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
         ,p_return_flowline_details     := boo_return_flowline_details
         ,p_return_flowline_geometry    := boo_return_flowline_geometry
         ,p_known_region                := str_known_region
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
         ,p_return_flowline_details     := boo_return_flowline_details
         ,p_return_flowline_geometry    := boo_return_flowline_geometry
         ,p_known_region                := str_known_region
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

ALTER FUNCTION cipsrv_pgrest.navigate(
   JSONB
) OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.navigate(
   JSONB
) TO PUBLIC;

--******************************--
----- functions/pointindexing.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.pointindexing';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.pointindexing(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   rec1                              RECORD;
   rec2                              RECORD;
   json_input                        JSONB := $1;
   sdo_point                         GEOMETRY;
   str_nhdplus_version               VARCHAR;
   boo_return_link_path              BOOLEAN;
   str_known_region                  VARCHAR;
   str_indexing_engine               VARCHAR;
   ary_fcode_allow                   INTEGER[];
   ary_fcode_deny                    INTEGER[];
   num_distance_max_distkm           NUMERIC;
   num_raindrop_snap_max_distkm      NUMERIC;
   num_raindrop_path_max_distkm      NUMERIC;
   boo_limit_innetwork               BOOLEAN;
   boo_limit_navigable               BOOLEAN;
   ary_fallback_fcode_allow          INTEGER[];
   ary_fallback_fcode_deny           INTEGER[];
   num_fallback_distance_max_distkm  NUMERIC;
   boo_fallback_limit_innetwork      BOOLEAN;
   boo_fallback_limit_navigable      BOOLEAN;
   bint_known_catchment_nhdplusid    BIGINT;
   int_return_code                   INTEGER;
   str_status_message                VARCHAR;
   
BEGIN
   
   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   IF JSONB_PATH_EXISTS(json_input,'$.point')
   AND json_input->'point' IS NOT NULL 
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
   
   IF JSONB_PATH_EXISTS(json_input,'$.indexing_engine')
   AND json_input->>'indexing_engine' IS NOT NULL
   THEN
      str_indexing_engine := json_input->>'indexing_engine';
      
   ELSE
      str_indexing_engine := 'DISTANCE';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fcode_allow')
   THEN
      IF jsonb_typeof(json_input->'fcode_allow') = 'array'
      THEN
         ary_fcode_allow := ARRAY(
            SELECT jsonb_array_elements(json_input->'fcode_allow')
         );
         
      END IF;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fcode_deny')
   THEN
      IF jsonb_typeof(json_input->'fcode_deny') = 'array'
      THEN
         ary_fcode_deny := ARRAY(
            SELECT jsonb_array_elements(json_input->'fcode_deny')
         );
         
      END IF;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.distance_max_distkm')
   AND json_input->'distance_max_distkm' IS NOT NULL
   AND json_input->>'distance_max_distkm' != ''
   THEN
      num_distance_max_distkm := cipsrv_engine.json2numeric(json_input->'distance_max_distkm');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.raindrop_snap_max_distkm')
   AND json_input->'raindrop_snap_max_distkm' IS NOT NULL
   AND json_input->>'raindrop_snap_max_distkm' != ''
   THEN
      num_raindrop_snap_max_distkm := cipsrv_engine.json2numeric(json_input->'raindrop_snap_max_distkm');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.raindrop_path_max_distkm')
   AND json_input->'raindrop_path_max_distkm' IS NOT NULL
   AND json_input->>'raindrop_path_max_distkm' != ''
   THEN
      num_raindrop_path_max_distkm := cipsrv_engine.json2numeric(json_input->'raindrop_path_max_distkm');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.limit_innetwork')
   AND json_input->>'limit_innetwork' IS NOT NULL
   THEN
      boo_limit_innetwork := (json_input->>'limit_innetwork')::BOOLEAN;
      
   ELSE
      boo_limit_innetwork := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.limit_navigable')
   AND json_input->>'limit_navigable' IS NOT NULL
   THEN
      boo_limit_navigable := (json_input->>'limit_navigable')::BOOLEAN;
      
   ELSE
      boo_limit_navigable := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fallback_fcode_allow')
   THEN
      IF jsonb_typeof(json_input->'fallback_fcode_allow') = 'array'
      THEN
         ary_fallback_fcode_allow := ARRAY(
            SELECT jsonb_array_elements(json_input->'fallback_fcode_allow')
         );
         
      END IF;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fallback_fcode_deny')
   THEN
      IF jsonb_typeof(json_input->'fallback_fcode_deny') = 'array'
      THEN
         ary_fallback_fcode_deny := ARRAY(
            SELECT jsonb_array_elements(json_input->'fallback_fcode_deny')
         );
         
      END IF;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fallback_distance_max_distkm')
   AND json_input->'fallback_distance_max_distkm' IS NOT NULL
   AND json_input->>'fallback_distance_max_distkm' != ''
   THEN
      num_fallback_distance_max_distkm := cipsrv_engine.json2numeric(json_input->'fallback_distance_max_distkm');
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fallback_limit_innetwork')
   AND json_input->>'fallback_limit_innetwork' IS NOT NULL
   THEN
      boo_fallback_limit_innetwork := (json_input->>'fallback_limit_innetwork')::BOOLEAN;
      
   ELSE
      boo_fallback_limit_innetwork := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.fallback_limit_navigable')
   AND json_input->>'fallback_limit_navigable' IS NOT NULL
   THEN
      boo_fallback_limit_navigable := (json_input->>'fallback_limit_navigable')::BOOLEAN;
      
   ELSE
      boo_fallback_limit_navigable := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_link_path')
   AND json_input->>'return_link_path' IS NOT NULL
   THEN
      boo_return_link_path := (json_input->>'return_link_path')::BOOLEAN;
      
   ELSE
      boo_return_link_path := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.known_region')
   AND json_input->>'known_region' IS NOT NULL
   THEN
      str_known_region := json_input->>'known_region';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.known_catchment_nhdplusid')
   AND json_input->'known_catchment_nhdplusid' IS NOT NULL 
   AND json_input->>'known_catchment_nhdplusid' != ''
   THEN
      bint_known_catchment_nhdplusid := cipsrv_engine.json2bigint(json_input->'known_catchment_nhdplusid');
      
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

   --------------------------------------------------------------------------
   -- Step 20
   -- Call the engine
   --------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      rec1 := cipsrv_nhdplus_m.pointindexing(
          p_point                        := sdo_point
         ,p_indexing_engine              := str_indexing_engine
         ,p_fcode_allow                  := ary_fcode_allow
         ,p_fcode_deny                   := ary_fcode_deny
         ,p_distance_max_distkm          := num_distance_max_distkm
         ,p_raindrop_snap_max_distkm     := num_raindrop_snap_max_distkm
         ,p_raindrop_path_max_distkm     := num_raindrop_path_max_distkm
         ,p_limit_innetwork              := boo_limit_innetwork
         ,p_limit_navigable              := boo_limit_navigable
         ,p_fallback_fcode_allow         := ary_fallback_fcode_allow
         ,p_fallback_fcode_deny          := ary_fallback_fcode_deny
         ,p_fallback_distance_max_distkm := num_fallback_distance_max_distkm
         ,p_fallback_limit_innetwork     := boo_fallback_limit_innetwork
         ,p_fallback_limit_navigable     := boo_fallback_limit_navigable
         ,p_return_link_path             := boo_return_link_path
         ,p_known_region                 := str_known_region
         ,p_known_catchment_nhdplusid    := bint_known_catchment_nhdplusid
      );
      int_return_code    := rec1.out_return_code;
      str_status_message := rec1.out_status_message;
      
      IF int_return_code != 0
      THEN
         RETURN JSONB_BUILD_OBJECT(
             'return_code',    int_return_code
            ,'status_message', str_status_message
         );
      
      END IF;
      
      RETURN JSONB_BUILD_OBJECT(
          'flowlines'           ,cipsrv_nhdplus_m.snapflowlines2geojson(rec1.out_flowlines)
         ,'path_distance_km'    ,rec1.out_path_distance_km
         ,'end_point'           ,JSONB_BUILD_OBJECT(
             'type'    ,'Feature'
            ,'geometry',ST_AsGeoJSON(ST_Transform(rec1.out_end_point,4326))::JSONB
          )
         ,'indexing_line'       ,JSONB_BUILD_OBJECT(
             'type'    ,'Feature'
            ,'geometry',ST_AsGeoJSON(ST_Transform(rec1.out_indexing_line,4326))::JSONB
          )
         ,'region'              ,rec1.out_region
         ,'nhdplusid'           ,rec1.out_nhdplusid
         ,'return_code'         ,int_return_code
         ,'status_message'      ,str_status_message
      );
      
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      rec2 := cipsrv_nhdplus_h.pointindexing(
          p_point                        := sdo_point
         ,p_indexing_engine              := str_indexing_engine
         ,p_fcode_allow                  := ary_fcode_allow
         ,p_fcode_deny                   := ary_fcode_deny
         ,p_distance_max_distkm          := num_distance_max_distkm
         ,p_raindrop_snap_max_distkm     := num_raindrop_snap_max_distkm
         ,p_raindrop_path_max_distkm     := num_raindrop_path_max_distkm
         ,p_limit_innetwork              := boo_limit_innetwork
         ,p_limit_navigable              := boo_limit_navigable
         ,p_fallback_fcode_allow         := ary_fallback_fcode_allow
         ,p_fallback_fcode_deny          := ary_fallback_fcode_deny
         ,p_fallback_distance_max_distkm := num_fallback_distance_max_distkm
         ,p_fallback_limit_innetwork     := boo_fallback_limit_innetwork
         ,p_fallback_limit_navigable     := boo_fallback_limit_navigable
         ,p_return_link_path             := boo_return_link_path
         ,p_known_region                 := str_known_region
         ,p_known_catchment_nhdplusid    := bint_known_catchment_nhdplusid
      );
      int_return_code    := rec2.out_return_code;
      str_status_message := rec2.out_status_message;
      
      IF int_return_code != 0
      THEN
         RETURN JSONB_BUILD_OBJECT(
             'return_code',    int_return_code
            ,'status_message', str_status_message
         );
      
      END IF;
      
      RETURN JSONB_BUILD_OBJECT(
          'flowlines'           ,cipsrv_nhdplus_h.snapflowlines2geojson(rec2.out_flowlines)
         ,'path_distance_km'    ,rec2.out_path_distance_km
         ,'end_point'           ,JSONB_BUILD_OBJECT(
             'type'    ,'Feature'
            ,'geometry',ST_AsGeoJSON(ST_Transform(rec2.out_end_point,4326))::JSONB
          )
         ,'indexing_line'       ,JSONB_BUILD_OBJECT(
             'type'    ,'Feature'
            ,'geometry',ST_AsGeoJSON(ST_Transform(rec2.out_indexing_line,4326))::JSONB
          )
         ,'region'              ,rec2.out_region
         ,'nhdplusid'           ,rec2.out_nhdplusid
         ,'return_code'         ,int_return_code
         ,'status_message'      ,str_status_message
      );
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code', -10
         ,'status_message', 'invalid nhdplus_version.'
      );
   
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.pointindexing';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv_pgrest',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv_pgrest',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;

--******************************--
----- functions/upstreamdownstream.sql 

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
   int_huc12_found_count             INTEGER;
   int_attr_found_count              INTEGER;
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
          'flowlines'                   , NULL
         ,'flowline_count'              , NULL
         ,'catchments'                  , NULL
         ,'catchment_count'             , NULL
         ,'linked_data_sfid_found'      , NULL
         ,'linked_data_sfid_count'      , NULL
         ,'linked_data_cip_found'       , NULL
         ,'linked_data_cip_count'       , NULL
         ,'linked_data_huc12_found'     , NULL
         ,'linked_data_huc12_count'     , NULL
         ,'linked_data_source_points'   , NULL
         ,'linked_data_source_lines'    , NULL
         ,'linked_data_source_areas'    , NULL
         ,'linked_data_source_count'    , NULL
         ,'linked_data_reached_points'  , NULL
         ,'linked_data_reached_lines'   , NULL
         ,'linked_data_reached_areas'   , NULL
         ,'linked_data_reached_count'   , NULL
         ,'linked_data_attributes_found', NULL
         ,'linked_data_attributes_count', NULL
         ,'start_nhdplusid'             , NULL
         ,'start_measure'               , NULL
         ,'start_linked_data_program'   , NULL
         ,'stop_nhdplusid'              , NULL
         ,'stop_measure'                , NULL
         ,'stop_linked_data_program'    , NULL
         ,'return_code'                 , -100
         ,'status_message'              , 'nhdplus version parameter is required'
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

   IF JSONB_PATH_EXISTS(json_input,'$.return_linked_data_huc12')
   AND json_input->'return_linked_data_huc12' IS NOT NULL
   AND json_input->>'return_linked_data_huc12' != ''
   THEN
      boo_return_linked_data_huc12 := (json_input->'return_linked_data_huc12')::BOOLEAN;
      
   ELSE
      boo_return_linked_data_huc12 := FALSE;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.return_linked_data_source')
   AND json_input->'return_linked_data_source' IS NOT NULL
   AND json_input->>'return_linked_data_source' != ''
   THEN
      boo_return_linked_data_source := (json_input->'return_linked_data_source')::BOOLEAN;
      
   ELSE
      boo_return_linked_data_source := FALSE;
      
   END IF;

   IF JSONB_PATH_EXISTS(json_input,'$.return_linked_data_attributes')
   AND json_input->'return_linked_data_attributes' IS NOT NULL
   AND json_input->>'return_linked_data_attributes' != ''
   THEN
      boo_return_linked_data_attributes := (json_input->'return_linked_data_attributes')::BOOLEAN;
      
   ELSE
      boo_return_linked_data_attributes := FALSE;
      
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
   int_huc12_found_count          := rec.out_huc12_found_count;
   int_src_found_count            := rec.out_src_found_count;
   int_attr_found_count           := rec.out_attr_found_count;
   int_return_code                := rec.out_return_code;
   str_status_message             := rec.out_status_message;
   
   IF int_return_code != 0
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'flowlines'                   , NULL
         ,'flowline_count'              , NULL
         ,'catchments'                  , NULL
         ,'catchment_count'             , NULL
         ,'linked_data_sfid_found'      , NULL
         ,'linked_data_sfid_count'      , NULL
         ,'linked_data_cip_found'       , NULL
         ,'linked_data_cip_count'       , NULL
         ,'linked_data_huc12_found'     , NULL
         ,'linked_data_huc12_count'     , NULL
         ,'linked_data_source_points'   , NULL
         ,'linked_data_source_lines'    , NULL
         ,'linked_data_source_areas'    , NULL
         ,'linked_data_source_count'    , NULL
         ,'linked_data_reached_points'  , NULL
         ,'linked_data_reached_lines'   , NULL
         ,'linked_data_reached_areas'   , NULL
         ,'linked_data_reached_count'   , NULL
         ,'linked_data_attributes_found', NULL
         ,'linked_data_attributes_count', NULL
         ,'start_nhdplusid'             , int_start_nhdplusid
         ,'start_measure'               , num_start_measure
         ,'start_linked_data_program'   , str_start_linked_data_program
         ,'stop_nhdplusid'              , int_stop_nhdplusid
         ,'stop_measure'                , num_stop_measure
         ,'stop_linked_data_program'    , str_stop_linked_data_program
         ,'return_code'                 , int_return_code
         ,'status_message'              , str_status_message
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
         JSONB_AGG(
            j.my_json ORDER BY j.my_json->'properties'->'orderingkey'
         ) AS my_feats
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
         JSONB_AGG(
            a.* ORDER BY a.eventtype,a.nearest_cip_network_distancekm,a.nearest_rad_network_distancekm
         )
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
         JSONB_AGG(
            a.* ORDER BY a.network_distancekm,a.network_flowtimeday
         )
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
         ) a
      );
            
      IF json_linked_data_cip_found IS NULL
      OR JSONB_ARRAY_LENGTH(json_linked_data_cip_found) = 0
      THEN
         json_linked_data_cip_found := NULL;
         
      END IF;
      
   ELSE
      json_linked_data_cip_found := NULL;
      int_cip_found_count        := NULL;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 80
   -- Build the huc12 featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_huc12
   AND int_huc12_found_count > 0
   THEN
      json_linked_data_huc12_found := (
         SELECT 
         JSONB_AGG(
            a.* ORDER BY a.eventtype,a.source_joinkey,a.permid_joinkey,a.xwalk_huc12
         )
         FROM (
            SELECT
             aa.eventtype
            ,aa.huc12_joinkey
            ,aa.permid_joinkey
            ,aa.source_originator
            ,aa.source_featureid
            ,aa.source_featureid2
            ,aa.source_series
            ,aa.source_subdivision
            ,aa.source_joinkey
            ,aa.start_date
            ,aa.end_date
            ,aa.xwalk_huc12
            ,aa.xwalk_huc12_version
            ,aa.xwalk_catresolution
            ,aa.xwalk_huc12_areasqkm
            FROM
            tmp_huc12_found aa
         ) a
      );
            
      IF json_linked_data_huc12_found IS NULL
      OR JSONB_ARRAY_LENGTH(json_linked_data_huc12_found) = 0
      THEN
         json_linked_data_huc12_found := NULL;
         
      END IF;
      
   ELSE
      json_linked_data_huc12_found := NULL;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 90
   -- Build the source points featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_source
   AND int_src_found_count > 0
   THEN
      json_linked_data_source_points := (
         SELECT 
         JSONB_AGG(
            j.my_json ORDER BY j.my_json->'properties'->'eventtype',j.my_json->'properties'->'orderingkey'
         ) AS my_feats
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
   -- Step 100
   -- Build the source lines featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_source
   AND int_src_found_count > 0
   THEN
      json_linked_data_source_lines := (
         SELECT 
         JSONB_AGG(
            j.my_json ORDER BY j.my_json->'properties'->'eventtype',j.my_json->'properties'->'orderingkey'
         ) AS my_feats
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
   -- Step 110
   -- Build the source areas featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_source
   AND int_src_found_count > 0
   THEN
      json_linked_data_source_areas := (
         SELECT 
         JSONB_AGG(
            j.my_json ORDER BY j.my_json->'properties'->'eventtype',j.my_json->'properties'->'orderingkey'
         ) AS my_feats
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
   -- Step 120
   -- Build the reached points featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_rad
   AND int_rad_found_count > 0
   THEN
      json_linked_data_reached_points := (
         SELECT 
         JSONB_AGG(
            j.my_json ORDER BY j.my_json->'properties'->'eventtype',j.my_json->'properties'->'network_distancekm'
         ) AS my_feats
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
   -- Step 130
   -- Build the reached points featurecollection
   ----------------------------------------------------------------------------
   IF boo_return_linked_data_attributes
   AND int_attr_found_count > 0
   THEN
      json_linked_data_attributes := (
         SELECT 
         JSONB_AGG(
            j.my_json ORDER BY j.my_json->'properties'->'eventtype',j.my_json->'properties'->'source_joinkey'
         ) AS my_feats
         FROM (
            SELECT
            TO_JSONB(aa) -'attributes' || aa.attributes AS my_json
            FROM (
               SELECT
                aaa.eventtype
               ,aaa.source_joinkey
               ,aaa.source_originator
               ,aaa.source_featureid
               ,aaa.source_featureid2
               ,aaa.source_series
               ,aaa.source_subdivision
               ,aaa.start_date
               ,aaa.end_date
               ,aaa.sfiddetailurl
               ,aaa.attributes
               FROM
               tmp_attr aaa
            ) aa
         ) j
      );
            
      IF json_linked_data_attributes IS NULL
      OR JSONB_ARRAY_LENGTH(json_linked_data_attributes) = 0
      THEN
         json_linked_data_attributes := NULL;
         
      END IF;
      
   ELSE
      json_linked_data_attributes := NULL;
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 130
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSONB_BUILD_OBJECT(
       'flowlines'                   , json_flowlines
      ,'flowline_count'              , int_flowline_count
      ,'catchments'                  , json_catchments
      ,'catchment_count'             , int_catchment_count
      ,'linked_data_sfid_found'      , json_linked_data_sfid_found
      ,'linked_data_sfid_count'      , int_sfid_found_count
      ,'linked_data_cip_found'       , json_linked_data_cip_found
      ,'linked_data_cip_count'       , int_cip_found_count
      ,'linked_data_huc12_found'     , json_linked_data_huc12_found
      ,'linked_data_huc12_count'     , int_huc12_found_count
      ,'linked_data_source_points'   , json_linked_data_source_points
      ,'linked_data_source_lines'    , json_linked_data_source_lines
      ,'linked_data_source_areas'    , json_linked_data_source_areas
      ,'linked_data_source_count'    , int_src_found_count
      ,'linked_data_reached_points'  , json_linked_data_reached_points
      ,'linked_data_reached_lines'   , NULL
      ,'linked_data_reached_areas'   , NULL
      ,'linked_data_reached_count'   , int_rad_found_count
      ,'linked_data_attributes_found', json_linked_data_attributes
      ,'linked_data_attributes_count', int_attr_found_count
      ,'start_nhdplusid'             , int_start_nhdplusid
      ,'start_measure'               , num_start_measure
      ,'start_linked_data_program'   , str_start_linked_data_program
      ,'stop_nhdplusid'              , int_stop_nhdplusid
      ,'stop_measure'                , num_stop_measure
      ,'stop_linked_data_program'    , str_stop_linked_data_program
      ,'return_code'                 , int_return_code
      ,'status_message'              , str_status_message
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
       'catchment'          ,JSONB_BUILD_OBJECT(
          'type'        ,'Feature'
         ,'geometry'    ,ST_AsGeoJSON(ST_Transform(rec.out_shape,4326))::JSONB
         ,'properties'  ,JSONB_BUILD_OBJECT(
             'nhdplusid'          ,rec.out_nhdplusid
            ,'areasqkm'           ,rec.out_areasqkm
            ,'catchmentstatecodes',rec.out_catchmentstatecodes
          )
       )
      ,'centroid'           ,JSONB_BUILD_OBJECT(
          'type'        ,'Feature'
         ,'geometry'    ,ST_AsGeoJSON(ST_Transform(rec.out_centroid,4326))::JSONB
       )
      ,'nhdplus_version'    ,str_nhdplus_version
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
       'huc12'          ,JSONB_BUILD_OBJECT(
          'type'        ,'Feature'
         ,'geometry'    ,ST_AsGeoJSON(ST_Transform(rec.out_shape,4326))::JSONB
         ,'properties'  ,JSONB_BUILD_OBJECT(
             'huc12'        ,rec.out_huc12
            ,'huc12_name'   ,rec.out_huc12_name
          )
       )
      ,'source_dataset' ,rec.out_source_dataset
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
   RETURN JSONB_BUILD_OBJECT(
       'navpoint'       ,JSONB_BUILD_OBJECT(
          'type'        ,'Feature'
         ,'geometry'    ,ST_AsGeoJSON(ST_Transform(rec.out_shape,4326))::JSONB
         ,'properties'  ,JSONB_BUILD_OBJECT(
             'nhdplusid'      ,rec.out_nhdplusid
            ,'reachcode'      ,rec.out_reachcode
            ,'measure'        ,rec.out_measure
          )
       )
      ,'nhdplus_version',str_nhdplus_version
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
       'point'              ,JSONB_BUILD_OBJECT(
          'type'      ,'Feature'
         ,'geometry'  ,ST_AsGeoJSON(ST_Transform(rec.out_shape,4326))::JSONB
       )
      ,'nhdplus_version'    ,str_nhdplus_version
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
   RETURN JSONB_BUILD_OBJECT(
       'navpoint1'      ,JSONB_BUILD_OBJECT(
          'type'      ,'Feature'
         ,'geometry'  ,ST_AsGeoJSON(ST_Transform(rec.out_shape1,4326))::JSONB
         ,'properties',JSONB_BUILD_OBJECT(
             'nhdplusid' ,rec.out_nhdplusid1
            ,'reachcode' ,rec.out_reachcode1
            ,'measure'   ,rec.out_measure1
          )
       )
      ,'navpoint2'      ,JSONB_BUILD_OBJECT(
          'type'      ,'Feature'
         ,'geometry'  ,ST_AsGeoJSON(ST_Transform(rec.out_shape2,4326))::JSONB
         ,'properties',JSONB_BUILD_OBJECT(
             'nhdplusid' ,rec.out_nhdplusid2
            ,'reachcode' ,rec.out_reachcode2
            ,'measure'   ,rec.out_measure2
          )
       )
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

