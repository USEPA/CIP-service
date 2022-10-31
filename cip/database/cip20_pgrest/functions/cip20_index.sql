CREATE OR REPLACE FUNCTION cip20_pgrest.cip20_index(
   JSON
) RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   json_input                    JSONB := $1;
   json_temp                     JSONB;
   json_feats                    JSONB;
   rec                           RECORD;
   p_geometry                    GEOMETRY;
   p_nhdplus_version             VARCHAR;
   p_state_filter                VARCHAR;
   p_known_region                VARCHAR;
   p_cip_indexing_method         VARCHAR;
   p_linear_threashold_perc      NUMERIC;
   p_cat_threashold_perc         NUMERIC;
   p_evt_threashold_perc         NUMERIC;
   p_catchment_count             INTEGER;
   p_catchment_areasqkm          NUMERIC;
   p_indexed_geometry            GEOMETRY;
   feat_indexed_geometry         JSONB;
   p_indexed_geom_measure        NUMERIC;
   p_indexed_geom_meas_unit      VARCHAR;
   p_return_catchment_geometry   BOOLEAN;
   p_return_flowlines            BOOLEAN;
   p_return_huc12s               BOOLEAN;
   p_return_flowline_geometry    BOOLEAN;
   p_return_huc12_geometry       BOOLEAN;
   feats_flowlines               JSONB;
   feats_huc12s                  JSONB;
   feats_catchments              JSONB;
   p_return_code                 INTEGER;
   p_status_message              VARCHAR;
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF jsonb_path_exists(json_input,'$.p_geometry')
   THEN
      json_temp := json_input->'p_geometry';
      
      IF jsonb_path_exists(json_temp,'$.type')
      THEN
         IF json_temp->>'type' = 'FeatureCollection' AND jsonb_path_exists(json_temp,'$.features')
         THEN
            json_feats := json_temp->'features';
            
            IF jsonb_array_length(json_feats) > 0
            THEN
               json_temp := json_temp->'features'->0->'geometry';
            
            END IF;
            
         ELSIF json_temp->>'type' = 'Feature' AND jsonb_path_exists(json_temp,'$.geometry')
         THEN
            json_temp := json_temp->'geometry';
         
         END IF;
         
         IF json_temp->>'type' IN ('Point','LineString','Polygon','MultiPoint','MultiLineString','MultiPolygon')
         THEN
            p_geometry = ST_GeomFromGeoJSON(json_temp);
            
         ELSE
            RETURN jsonb_build_object(
                'p_return_code', -10
               ,'p_status_message', 'unable to parse p_geometry input as GeoJSON'
            ); 
         
         END IF;
         
      END IF;
         
   ELSE
      RETURN jsonb_build_object(
          'p_return_code', -10
         ,'p_status_message', 'p_geometry required for cip indexing'
      );
            
   END IF;
   
   IF jsonb_path_exists(json_input,'$.p_nhdplus_version')
   THEN
      p_nhdplus_version := json_input->>'p_nhdplus_version';
      
   ELSE
      RETURN jsonb_build_object(
          'p_return_code', -10
         ,'p_status_message', 'p_nhdplus_version required for cip indexing'
      );
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.p_state_filter')
   THEN
      IF json_input->>'p_state_filter' = ''
      OR json_input->>'p_state_filter' = ' '
      THEN
         p_state_filter := NULL;
         p_known_region := NULL;
         
      ELSE
         p_state_filter := json_input->>'p_state_filter';
         p_known_region := json_input->>'p_state_filter';
         
      END IF;
      
   END IF;
   
   -- Allow known region override
   IF jsonb_path_exists(json_input,'$.p_known_region')
   THEN
      p_known_region := json_input->>'p_known_region';
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.p_cip_indexing_method')
   THEN
      p_cip_indexing_method := json_input->>'p_cip_indexing_method';
      
   ELSE
      RETURN jsonb_build_object(
          'p_return_code', -10
         ,'p_status_message', 'p_cip_indexing_method required for cip indexing'
      );
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.p_linear_threashold_perc')
   THEN
      IF jsonb_typeof(json_input->'p_linear_threashold_perc') = 'string'
      THEN
         IF json_input->>'p_linear_threashold_perc' = ''
         OR json_input->>'p_linear_threashold_perc' = ' '
         OR json_input->>'p_linear_threashold_perc' = 'null' 
         THEN
            p_linear_threashold_perc := NULL;

         ELSE            
            p_linear_threashold_perc := (json_input->>'p_linear_threashold_perc')::NUMERIC;
            
         END IF;
      
      ELSE
         p_linear_threashold_perc := json_input->'p_linear_threashold_perc';
   
      END IF;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.p_cat_threashold_perc')
   THEN
      IF jsonb_typeof(json_input->'p_cat_threashold_perc') = 'string'
      THEN
         IF json_input->>'p_cat_threashold_perc' = ''
         OR json_input->>'p_cat_threashold_perc' = ' '
         OR json_input->>'p_cat_threashold_perc' = 'null' 
         THEN
            p_cat_threashold_perc := NULL;

         ELSE 
            p_cat_threashold_perc := (json_input->>'p_cat_threashold_perc')::NUMERIC;
            
         END IF;
         
      ELSE
         p_cat_threashold_perc := json_input->'p_cat_threashold_perc';
         
      END IF;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.p_evt_threashold_perc')
   THEN
      IF jsonb_typeof(json_input->'p_evt_threashold_perc') = 'string'
      THEN
         IF json_input->>'p_evt_threashold_perc' = ''
         OR json_input->>'p_evt_threashold_perc' = ' '
         OR json_input->>'p_evt_threashold_perc' = 'null' 
         THEN
            p_evt_threashold_perc := NULL;

         ELSE 
            p_evt_threashold_perc := (json_input->>'p_evt_threashold_perc')::NUMERIC;
            
         END IF;
         
      ELSE
         p_evt_threashold_perc := json_input->'p_evt_threashold_perc';
         
      END IF;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.p_return_catchment_geometry')
   THEN
      p_return_catchment_geometry := (json_input->>'p_return_catchment_geometry')::BOOLEAN;
      
   ELSE
      p_return_catchment_geometry := FALSE;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.p_return_flowlines')
   THEN
      p_return_flowlines := (json_input->>'p_return_flowlines')::BOOLEAN;
      
   ELSE
      p_return_flowlines := FALSE;
      
   END IF;
   
   
   IF jsonb_path_exists(json_input,'$.p_return_huc12s')
   THEN
      p_return_huc12s := (json_input->>'p_return_huc12s')::BOOLEAN;
      
   ELSE
      p_return_huc12s := FALSE;
      
   END IF;
   
   IF jsonb_path_exists(json_input,'$.p_return_flowline_geometry')
   THEN
      p_return_flowline_geometry := (json_input->>'p_return_flowline_geometry')::BOOLEAN;
      
   ELSE
      p_return_flowline_geometry := FALSE;
      
   END IF;

   IF jsonb_path_exists(json_input,'$.p_return_huc12_geometry')
   THEN
      p_return_huc12_geometry := (json_input->>'p_return_huc12_geometry')::BOOLEAN;
      
   ELSE
      p_return_huc12_geometry := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Call the one-off indexer
   ----------------------------------------------------------------------------
   rec := cip20_engine.cip20_index(
       p_geometry               := p_geometry
      ,p_nhdplus_version        := p_nhdplus_version
      ,p_known_region           := p_known_region
      ,p_state_filter           := p_state_filter
      ,p_cip_indexing_method    := p_cip_indexing_method
      ,p_linear_threashold_perc := p_linear_threashold_perc
      ,p_cat_threashold_perc    := p_cat_threashold_perc
      ,p_evt_threashold_perc    := p_evt_threashold_perc
      ,p_return_geometry        := p_return_catchment_geometry
      
   );
   p_catchment_count        := rec.p_catchment_count;
   p_catchment_areasqkm     := rec.p_catchment_areasqkm;
   p_indexed_geometry       := rec.p_indexed_geometry;
   p_indexed_geom_measure   := rec.p_indexed_geom_measure;
   p_indexed_geom_meas_unit := rec.p_indexed_geom_meas_unit;
   p_return_code            := rec.p_return_code;
   p_status_message         := rec.p_status_message;
   
   IF p_return_code != 0
   THEN
      RETURN jsonb_build_object(
          'p_indexed_geometry',   NULL
         ,'p_catchment_count',    0
         ,'p_catchment_areasqkm', NULL
         ,'p_nhdplus_version',    p_nhdplus_version
         ,'p_catchments',         NULL
         ,'p_flowlines',          NULL
         ,'p_huc12s',             NULL
         ,'p_return_code',        p_return_code
         ,'p_status_message',     p_status_message
      );
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Build the Indexed Geometry output Feature
   ----------------------------------------------------------------------------
   IF p_indexed_geometry IS NULL
   THEN
      RETURN jsonb_build_object(
          'p_indexed_geometry',   NULL
         ,'p_catchment_count',    0
         ,'p_catchment_areasqkm', NULL
         ,'p_nhdplus_version',    p_nhdplus_version
         ,'p_catchments',         NULL
         ,'p_flowlines',          NULL
         ,'p_huc12s',             NULL
         ,'p_return_code',        -1
         ,'p_status_message',     'Indexer returned no results'
      );
      
   END IF;
   
   SELECT ST_AsGeoJSON(t.*)
   INTO
   feat_indexed_geometry 
   FROM 
   (VALUES(p_indexed_geom_measure,p_indexed_geom_meas_unit,ST_Transform(ST_ForcePolygonCCW(p_indexed_geometry),4326))) 
   AS t("indexed_geom_measure","indexed_geom_meas_unit", geom); 
   
   feat_indexed_geometry := jsonb_build_object(
       'type',     'FeatureCollection'
      ,'features', jsonb_build_array(feat_indexed_geometry)
   );   
   
   IF p_catchment_count = 0
   THEN
      RETURN jsonb_build_object(
          'p_indexed_geometry',   feat_indexed_geometry 
         ,'p_catchment_count',    0
         ,'p_catchment_areasqkm', NULL
         ,'p_nhdplus_version',    p_nhdplus_version
         ,'p_catchments',         NULL
         ,'p_flowlines',          NULL
         ,'p_huc12s',             NULL
         ,'p_return_code',        -1
         ,'p_status_message',     'Indexer returned no results'
      );
         
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Build the flowlines featurecollection
   ----------------------------------------------------------------------------
   IF p_return_flowlines
   THEN
      IF p_nhdplus_version = 'nhdplus_m'
      THEN
         feats_flowlines := json_build_object(
             'type',    'FeatureCollection'
            ,'features', (
               SELECT json_agg(ST_AsGeoJSON(t.*)::JSON)
               FROM (
                  SELECT
                   a.nhdplusid
                  ,a.gnis_id
                  ,a.gnis_name
                  ,a.reachcode
                  ,a.fmeasure
                  ,a.tmeasure
                  ,CASE
                   WHEN p_return_flowline_geometry
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
         
      ELSIF p_nhdplus_version = 'nhdplus_h'
      THEN
         feats_flowlines := json_build_object(
             'type',    'FeatureCollection'
            ,'features', (
               SELECT json_agg(ST_AsGeoJSON(t.*)::JSON)
               FROM (
                  SELECT
                   a.nhdplusid
                  ,a.gnis_id
                  ,a.gnis_name
                  ,a.reachcode
                  ,a.fmeasure
                  ,a.tmeasure
                  ,CASE
                   WHEN p_return_flowline_geometry
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
      feats_flowlines := NULL;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Build the huc12 featurecollection
   ----------------------------------------------------------------------------
   feats_huc12s := NULL;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Build the catchments featurecollection
   ----------------------------------------------------------------------------
   IF p_return_catchment_geometry
   THEN
      feats_catchments := json_build_object(
          'type',    'FeatureCollection'
         ,'features', (
            SELECT json_agg(ST_AsGeoJSON(t.*)::JSON)  
            FROM (
               SELECT
                a.nhdplusid
               ,a.catchmentstatecode
               ,a.xwalk_huc12
               ,a.areasqkm
               ,ST_Transform(ST_ForcePolygonCCW(a.shape),4326) AS geom
               FROM
               tmp_cip_out a
               ORDER BY
                a.catchmentstatecode
               ,a.nhdplusid
            ) t
          )
      );
      
   END IF;
      
   ----------------------------------------------------------------------------
   -- Step 60
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN jsonb_build_object(
       'p_indexed_geometry',   feat_indexed_geometry 
      ,'p_catchment_count',    p_catchment_count
      ,'p_catchment_areasqkm', p_catchment_areasqkm
      ,'p_nhdplus_version',    p_nhdplus_version
      ,'p_catchments',         feats_catchments
      ,'p_flowlines',          feats_flowlines
      ,'p_huc12s',             feats_huc12s
      ,'p_return_code',        p_return_code
      ,'p_status_message',     p_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_pgrest.cip20_index(
   JSON
) OWNER TO cip20_pgrest;

GRANT EXECUTE ON FUNCTION cip20_pgrest.cip20_index(
   JSON
) TO PUBLIC;

