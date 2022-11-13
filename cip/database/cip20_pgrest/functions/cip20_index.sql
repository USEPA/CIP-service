CREATE OR REPLACE FUNCTION cip20_pgrest.cip20_index(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   rec                           RECORD;
   json_input                    JSONB := $1;
   json_points                   JSONB;
   json_lines                    JSONB;
   json_areas                    JSONB;
   sdo_geometry                  GEOMETRY;
   str_nhdplus_version           VARCHAR;
   str_wbd_version               VARCHAR;
   str_state_filter              VARCHAR;
   str_known_region              VARCHAR;
   str_default_point_method      VARCHAR;
   str_default_line_method       VARCHAR;
   str_default_area_method       VARCHAR;
   num_default_line_threshold    NUMERIC;
   num_default_areacat_threshold NUMERIC;
   num_default_areaevt_threshold NUMERIC;
   boo_return_catchment_geometry BOOLEAN;
   boo_return_flowlines          BOOLEAN;
   boo_return_huc12s             BOOLEAN;
   boo_return_flowline_geometry  BOOLEAN;
   boo_return_huc12_geometry     BOOLEAN;
   int_catchment_count           INTEGER;
   num_catchment_areasqkm        NUMERIC;
   
   json_indexed_points           JSONB;
   json_indexed_lines            JSONB;
   json_indexed_areas            JSONB;
   sdo_indexed_collection        GEOMETRY;
   json_indexing_summary         JSONB;
   json_flowlines                JSONB;
   json_catchments               JSONB;
   json_huc12s                   JSONB;
   
   int_return_code               INTEGER;
   str_status_message            VARCHAR;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF jsonb_path_exists(json_input,'$.points')
   AND json_input->>'points' IS NOT NULL
   THEN
      json_points := json_input->'points';

   END IF;
   
   IF jsonb_path_exists(json_input,'$.lines')
   AND json_input->>'lines' IS NOT NULL
   THEN
      json_lines := json_input->'lines';
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.areas')
   AND json_input->>'areas' IS NOT NULL
   THEN
      json_areas := json_input->'areas';
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.geometry')
   AND json_input->>'geometry' IS NOT NULL
   THEN
      sdo_geometry := cip20_engine.json2geometry(json_input->'geometry');
      
   END IF;

   IF  json_points  IS NULL
   AND json_lines   IS NULL
   AND json_areas   IS NULL
   AND sdo_geometry IS NULL
   THEN
      RETURN jsonb_build_object(
          'return_code', -10
         ,'status_message', 'input events required for cip indexing'
      );
            
   END IF;
   
   IF jsonb_path_exists(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      RETURN jsonb_build_object(
          'return_code', -10
         ,'status_message', 'nhdplus_version required for cip indexing'
      );
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.wbd_version')
   AND json_input->>'wbd_version' IS NOT NULL
   THEN
      str_wbd_version := json_input->>'wbd_version';
      
   END IF;

   IF jsonb_path_exists(json_input,'$.state_filter')
   AND json_input->>'state_filter' IS NOT NULL
   THEN
      IF json_input->>'state_filter' = ''
      OR json_input->>'state_filter' = ' '
      THEN
         str_state_filter := NULL;
         str_known_region := NULL;
         
      ELSE
         str_state_filter := json_input->>'state_filter';
         str_known_region := json_input->>'state_filter';
         
      END IF;
      
   END IF;
   
   -- Allow known region override
   IF jsonb_path_exists(json_input,'$.known_region')
   AND json_input->>'known_region' IS NOT NULL
   THEN
      str_known_region := json_input->>'known_region';
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.default_point_method')
   AND json_input->>'default_point_method' IS NOT NULL
   THEN
      str_default_point_method := json_input->>'default_point_method';
      
   ELSE
      str_default_point_method := 'point';
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.default_line_method')
   AND json_input->>'default_line_method' IS NOT NULL
   THEN
      str_default_line_method := json_input->>'default_line_method';
      
   ELSE
      str_default_line_method := 'line';
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.default_area_method')
   AND json_input->>'default_area_method' IS NOT NULL
   THEN
      str_default_area_method := json_input->>'default_area_method';
      
   ELSE
      str_default_area_method := 'area-simple';
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.default_line_threshold')
   AND json_input->>'default_line_threshold' IS NOT NULL 
   THEN
      num_default_line_threshold := cip20_engine.json2numeric(json_input->'default_line_threshold');
      
   ELSE
      num_default_line_threshold := 10;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.default_areacat_threshold')
   AND json_input->>'default_areacat_threshold' IS NOT NULL
   THEN
      num_default_areacat_threshold := cip20_engine.json2numeric(json_input->'default_areacat_threshold');
      
   ELSE
      num_default_areacat_threshold := 10;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.default_areaevt_threshold')
   AND json_input->>'default_areaevt_threshold' IS NOT NULL
   THEN
      num_default_areaevt_threshold := cip20_engine.json2numeric(json_input->'default_areaevt_threshold');
      
   ELSE
      num_default_areaevt_threshold := 10;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.return_catchment_geometry')
   AND json_input->>'return_catchment_geometry' IS NOT NULL
   THEN
      boo_return_catchment_geometry := (json_input->>'return_catchment_geometry')::BOOLEAN;
      
   ELSE
      boo_return_catchment_geometry := FALSE;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.return_flowlines')
   AND json_input->>'return_flowlines' IS NOT NULL
   THEN
      boo_return_flowlines := (json_input->>'return_flowlines')::BOOLEAN;
      
   ELSE
      boo_return_flowlines := FALSE;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.return_huc12s')
   AND json_input->>'return_huc12s' IS NOT NULL
   THEN
      boo_return_huc12s := (json_input->>'return_huc12s')::BOOLEAN;
      
   ELSE
      boo_return_huc12s := FALSE;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.return_flowline_geometry')
   AND json_input->>'return_flowline_geometry' IS NOT NULL
   THEN
      boo_return_flowline_geometry := (json_input->>'return_flowline_geometry')::BOOLEAN;
      
   ELSE
      boo_return_flowline_geometry := FALSE;
      
   END IF;

   IF jsonb_path_exists(json_input,'$.return_huc12_geometry')
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
   rec := cip20_engine.cip20_index(
       p_points                    := json_points
      ,p_lines                     := json_lines
      ,p_areas                     := json_areas
      ,p_geometry                  := sdo_geometry
      ,p_state_filter              := str_state_filter
      ,p_nhdplus_version           := str_nhdplus_version
      ,p_wbd_version               := str_wbd_version
      ,p_default_point_method      := str_default_point_method
      ,p_default_line_method       := str_default_line_method
      ,p_default_area_method       := str_default_area_method
      ,p_default_line_threshold    := num_default_line_threshold
      ,p_default_areacat_threshold := num_default_areacat_threshold
      ,p_default_areaevt_threshold := num_default_areaevt_threshold
      ,p_known_region              := str_known_region
      ,p_return_catchment_geometry := boo_return_catchment_geometry
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
      RETURN jsonb_build_object(
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
   IF int_catchment_count = 0
   THEN
      RETURN jsonb_build_object(
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
         json_flowlines := JSON_BUILD_OBJECT(
             'type',    'FeatureCollection'
            ,'features', (
               SELECT JSON_AGG(ST_AsGeoJSON(t.*)::JSON)
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
                   cip20_nhdplus_m.nhdflowline a
                   WHERE
                   EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
                   ORDER BY
                   a.nhdplusid
               ) t
             )
         );
         
      ELSIF str_nhdplus_version = 'nhdplus_h'
      THEN
         json_flowlines := JSON_BUILD_OBJECT(
             'type',    'FeatureCollection'
            ,'features', (
               SELECT JSON_AGG(ST_AsGeoJSON(t.*)::JSON)
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
                   cip20_nhdplus_h.nhdflowline a
                   WHERE
                   EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
                   ORDER BY
                   a.nhdplusid
               ) t
             )
         );
         
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
   json_catchments := JSON_BUILD_OBJECT(
       'type',    'FeatureCollection'
      ,'features', (
         SELECT JSON_AGG(ST_AsGeoJSON(t.*)::JSON)  
         FROM (
            SELECT
             a.nhdplusid
            ,a.catchmentstatecode
            ,a.xwalk_huc12
            ,a.areasqkm
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
       )
   );
      
   ----------------------------------------------------------------------------
   -- Step 60
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN jsonb_build_object(
       'indexed_points',     json_indexed_points
      ,'indexed_lines',      json_indexed_lines
      ,'indexed_areas',      json_indexed_areas
      ,'indexed_collection', ST_AsGeoJSON(sdo_indexed_collection)::JSONB
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

ALTER FUNCTION cip20_pgrest.cip20_index(
   JSONB
) OWNER TO cip20_pgrest;

GRANT EXECUTE ON FUNCTION cip20_pgrest.cip20_index(
   JSONB
) TO PUBLIC;

