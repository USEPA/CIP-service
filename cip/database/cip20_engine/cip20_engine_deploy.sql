--******************************--
----- functions/temp_table_exists.sql 

CREATE or REPLACE FUNCTION cip20_engine.temp_table_exists(
   IN p_table_name VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_table_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for temp table
   ----------------------------------------------------------------------------
   SELECT 
    n.nspname
   INTO str_table_name
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   where 
       n.nspname like 'pg_temp_%'
   AND pg_catalog.pg_table_is_visible(c.oid)
   AND UPPER(relname) = UPPER(p_table_name);

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_table_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.temp_table_exists(
   VARCHAR
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.temp_table_exists(
   VARCHAR
) TO PUBLIC;

--******************************--
----- functions/create_cip_temp_tables.sql 

CREATE OR REPLACE FUNCTION cip20_engine.create_cip_temp_tables()
RETURNS INT4
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_cip temp table
   ----------------------------------------------------------------------------
   IF cip20_engine.temp_table_exists('tmp_cip')
   THEN
      TRUNCATE TABLE tmp_cip;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_cip(
         nhdplusid             BIGINT
      );

      CREATE UNIQUE INDEX tmp_cip_pk 
      ON tmp_cip(nhdplusid);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_cip_out temp table
   ----------------------------------------------------------------------------
   IF cip20_engine.temp_table_exists('tmp_cip_out')
   THEN
      TRUNCATE TABLE tmp_cip_out;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_cip_out(
          nhdplusid            BIGINT
         ,catchmentstatecode   VARCHAR(2)
         ,xwalk_huc12          VARCHAR(12)
         ,areasqkm             NUMERIC
         ,shape                GEOMETRY
      );

      CREATE UNIQUE INDEX tmp_cip_out_pk 
      ON tmp_cip_out(catchmentstatecode,nhdplusid);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.create_cip_temp_tables() OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.create_cip_temp_tables() TO PUBLIC;

--******************************--
----- functions/measure_geometry.sql 

CREATE OR REPLACE FUNCTION cip20_engine.measure_geometry(
    IN  p_geometry          GEOMETRY
   ,IN  p_nhdplus_version   VARCHAR
   ,IN  p_known_region      VARCHAR
   ,OUT p_measurement       NUMERIC
   ,OUT p_sub_meas          NUMERIC[]
   ,OUT p_unit              VARCHAR
   ,OUT p_return_code       INTEGER
   ,OUT p_status_message    VARCHAR
)
IMMUTABLE
AS $BODY$ 
DECLARE 
   rec          RECORD;
   int_srid     INTEGER;
   str_gtype    VARCHAR;
   int_count    INTEGER;
   
BEGIN

   p_return_code := 0;
   
   rec := cip20_engine.determine_grid_srid(
       p_geometry        := p_geometry
      ,p_nhdplus_version := p_nhdplus_version
      ,p_known_region    := p_known_region
   );
   int_srid         := rec.p_srid;
   p_return_code    := rec.p_return_code;
   p_status_message := rec.p_status_message;
   
   IF p_return_code!= 0
   THEN
      RETURN;
      
   END IF;
   
   str_gtype := ST_GeometryType(p_geometry);
   int_count := ST_NumGeometries(p_geometry);
   
   IF str_gtype IN ('ST_Point','ST_MultiPoint')
   THEN
      p_measurement   := NULL;
      p_unit          := NULL;
      
   ELSIF str_gtype IN ('ST_LineString','ST_MultiLineString')
   THEN
      p_measurement   := ROUND(ST_Length(ST_Transform(p_geometry,int_srid))::NUMERIC * 0.001,8);
      p_unit          := 'KM';
      int_count       := ST_NumGeometries(p_geometry);
      
      IF int_count = 1
      THEN
         p_sub_meas[1] := p_measurement;
      
      ELSE
         FOR i IN 1 .. int_count
         LOOP
            p_sub_meas[i] := ROUND(ST_Length(ST_Transform(ST_GeometryN(p_geometry,i),int_srid))::NUMERIC * 0.001,8);
            
         END LOOP;

      END IF;
      
   ELSIF str_gtype IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      p_measurement   := ROUND(ST_Area(ST_Transform(p_geometry,int_srid))::NUMERIC / 1000000,8);
      p_unit          := 'SQ_KM';
      int_count       := ST_NumGeometries(p_geometry);
      
      IF int_count = 1
      THEN
         p_sub_meas[1] := p_measurement;
      
      ELSE
         FOR i IN 1 .. int_count
         LOOP
            p_sub_meas[i] := ROUND(ST_Area(ST_Transform(ST_GeometryN(p_geometry,i),int_srid))::NUMERIC / 1000000,8);
            
         END LOOP;

      END IF;         
   
   ELSE
      p_return_code    := -1;
      p_status_message := 'unable to measure ' || str_gtype;
      RETURN; 
   
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.measure_geometry(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.measure_geometry(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/determine_grid_srid.sql 

CREATE OR REPLACE FUNCTION cip20_engine.determine_grid_srid(
    IN  p_geometry             GEOMETRY
   ,IN  p_nhdplus_version      VARCHAR
   ,IN  p_known_region         VARCHAR
   ,OUT p_srid                 INTEGER
   ,OUT p_grid_size            NUMERIC
   ,OUT p_return_code          INTEGER
   ,OUT p_status_message       VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec RECORD;
   
BEGIN

   IF p_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cip20_nhdplus_m.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      p_srid           := rec.p_srid;
      p_grid_size      := rec.p_grid_size;
      p_return_code    := rec.p_return_code;
      p_status_message := rec.p_status_message;
   
   ELSIF p_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cip20_nhdplus_h.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      p_srid           := rec.p_srid;
      p_grid_size      := rec.p_grid_size;
      p_return_code    := rec.p_return_code;
      p_status_message := rec.p_status_message;

   ELSE
      RAISE EXCEPTION 'err %',p_nhdplus_version;

   END IF;   
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/cip20_index.sql 

CREATE OR REPLACE FUNCTION cip20_engine.cip20_index(
    IN  p_geometry               GEOMETRY
   ,IN  p_nhdplus_version        VARCHAR
   ,IN  p_known_region           VARCHAR
   ,IN  p_state_filter           VARCHAR
   ,IN  p_cip_indexing_method    VARCHAR
   ,IN  p_linear_threashold_perc NUMERIC
   ,IN  p_cat_threashold_perc    NUMERIC
   ,IN  p_evt_threashold_perc    NUMERIC
   ,IN  p_return_geometry        BOOLEAN
   ,OUT p_catchment_count        INTEGER
   ,OUT p_catchment_areasqkm     NUMERIC
   ,OUT p_indexed_geometry       GEOMETRY
   ,OUT p_indexed_geom_measure   NUMERIC
   ,OUT p_indexed_geom_meas_unit VARCHAR
   ,OUT p_return_code            INTEGER
   ,OUT p_status_message         VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                 RECORD;
   str_gtype           VARCHAR;
   str_known_region    VARCHAR;
   int_meassrid        INTEGER;
   int_gridsize        INTEGER;
   sdo_input           GEOMETRY;
   sdo_state_clipped   GEOMETRY;
   ary_idx_geom_meas   NUMERIC[];
   boo_return_geometry BOOLEAN;
   str_state_filter    VARCHAR(2);

BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_geometry IS NULL
   THEN
      p_return_code    := -10;
      p_status_message := 'input geometry cannot be null';
      RETURN;
   
   END IF;
   
   IF p_nhdplus_version IS NULL
   THEN
      p_return_code    := -10;
      p_status_message := 'nhdplus version cannot be null';
      RETURN;
      
   ELSIF p_nhdplus_version NOT IN ('nhdplus_m','nhdplus_h') 
   THEN
      p_return_code    := -10;
      p_status_message := 'invalid nhdplus version';
      RETURN;
   
   END IF;
   
   str_state_filter := UPPER(p_state_filter);
   
   IF p_cip_indexing_method IS NULL
   THEN
      p_return_code    := -10;
      p_status_message := 'CIP event type cannot be null';
      RETURN;
      
   ELSIF p_cip_indexing_method NOT IN ('point','line','area_artpath','area_simple','area_centroid')
   THEN
      p_return_code    := -10;
      p_status_message := 'invalid CIP event type';
      RETURN;
    
   END IF;
   
   boo_return_geometry := p_return_geometry;
   IF boo_return_geometry IS NULL
   THEN
      boo_return_geometry := FALSE;
      
   END IF;
   
   str_known_region := p_known_region;
   
   -----------------------------------------------------------------------------
   -- Step 20
   -- Flush or create the temp tables
   -----------------------------------------------------------------------------
   p_return_code := cip20_engine.create_cip_temp_tables();
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Check that geometry is appropriate for CIP event type
   ----------------------------------------------------------------------------
   p_indexed_geometry := p_geometry;
   str_gtype := ST_GeometryType(p_indexed_geometry);
   
   IF ( p_cip_indexing_method = 'point' and str_gtype NOT IN ('ST_Point','ST_MultiPoint') )
   OR ( p_cip_indexing_method = 'line' and str_gtype NOT IN ('ST_Point','ST_MultiPoint') )
   OR ( p_cip_indexing_method IN ('area','huc-like') and str_gtype NOT IN ('ST_Polygon','ST_MultiPolygon') )
   THEN
      p_return_code    := -20;
      p_status_message := 'geometry type ' || str_gtype || ' is invalid for CIP event type ' || p_cip_indexing_method;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Determine the known region to get measure srid
   ----------------------------------------------------------------------------
   rec := cip20_engine.determine_grid_srid(
       p_geometry        := p_indexed_geometry
      ,p_nhdplus_version := p_nhdplus_version
      ,p_known_region    := str_known_region
   );
   int_meassrid       := rec.p_srid;
   int_gridsize       := rec.p_grid_size;
   p_return_code      := rec.p_return_code;
   p_status_message   := rec.p_status_message;
   
   IF p_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_meassrid::VARCHAR;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Measure the geometry, note this measurement is not state-clipped
   ----------------------------------------------------------------------------
   rec := cip20_engine.measure_geometry(
       p_geometry        := p_indexed_geometry
      ,p_nhdplus_version := p_nhdplus_version
      ,p_known_region    := str_known_region
   );
   p_indexed_geom_measure   := rec.p_measurement;
   p_indexed_geom_meas_unit := rec.p_unit;
   ary_idx_geom_meas        := rec.p_sub_meas;
   p_return_code            := rec.p_return_code;
   p_status_message         := rec.p_status_message;
   
   IF p_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Determine the catchments
   ----------------------------------------------------------------------------
   FOR i IN 1 .. ST_NumGeometries(p_indexed_geometry)
   LOOP
      sdo_input := ST_GeometryN(p_indexed_geometry,i);
   
      IF p_cip_indexing_method = 'point'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_point(
                p_geometry               := sdo_input
               ,p_known_region           := str_known_region
            );
            p_return_code    := rec.p_return_code;
            p_status_message := rec.p_status_message;
         
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_point(
                p_geometry               := sdo_input
               ,p_known_region           := str_known_region
            );
            p_return_code    := rec.p_return_code;
            p_status_message := rec.p_status_message;
            
         END IF;
      
      ELSIF p_cip_indexing_method = 'line'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_linear(
                p_geometry               := sdo_input
               ,p_geometry_lengthkm      := ary_idx_geom_meas[i]
               ,p_known_region           := str_known_region
               ,p_linear_threashold_perc := p_linear_threashold_perc
            );
            p_return_code    := rec.p_return_code;
            p_status_message := rec.p_status_message;
            
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_linear(
                p_geometry               := sdo_input
               ,p_geometry_lengthkm      := ary_idx_geom_meas[i]
               ,p_known_region           := str_known_region
               ,p_linear_threashold_perc := p_linear_threashold_perc
            );
            p_return_code    := rec.p_return_code;
            p_status_message := rec.p_status_message;
            
         END IF;
      
      ELSIF p_cip_indexing_method = 'area_artpath'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_area_artpath(
                p_geometry               := sdo_input
               ,p_geometry_areasqkm      := ary_idx_geom_meas[i]
               ,p_known_region           := str_known_region
               ,p_cat_threashold_perc    := p_cat_threashold_perc
               ,p_evt_threashold_perc    := p_evt_threashold_perc
            );
            p_return_code    := rec.p_return_code;
            p_status_message := rec.p_status_message;
         
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_area_artpath(
                p_geometry               := sdo_input
               ,p_geometry_areasqkm      := ary_idx_geom_meas[i]
               ,p_known_region           := str_known_region
               ,p_cat_threashold_perc    := p_cat_threashold_perc
               ,p_evt_threashold_perc    := p_evt_threashold_perc
            );
            p_return_code    := rec.p_return_code;
            p_status_message := rec.p_status_message;
            
         END IF;
         
      ELSIF p_cip_indexing_method = 'area_simple'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_area_simple(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := ary_idx_geom_meas[i]
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := p_cat_threashold_perc
               ,p_evt_threashold_perc  := p_evt_threashold_perc
            );
            p_return_code    := rec.p_return_code;
            p_status_message := rec.p_status_message;
         
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_area_simple(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := ary_idx_geom_meas[i]
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := p_cat_threashold_perc
               ,p_evt_threashold_perc  := p_evt_threashold_perc
            );
            p_return_code    := rec.p_return_code;
            p_status_message := rec.p_status_message;
            
         END IF;
         
      ELSIF p_cip_indexing_method = 'area_centroid'
      THEN
         IF p_nhdplus_version = 'nhdplus_m'
         THEN
            rec := cip20_nhdplus_m.index_area_centroid(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := ary_idx_geom_meas[i]
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := p_cat_threashold_perc
               ,p_evt_threashold_perc  := p_evt_threashold_perc
            );
            p_return_code    := rec.p_return_code;
            p_status_message := rec.p_status_message;
            
         ELSIF p_nhdplus_version = 'nhdplus_h'
         THEN
            rec := cip20_nhdplus_h.index_area_centroid(
                p_geometry             := sdo_input
               ,p_geometry_areasqkm    := ary_idx_geom_meas[i]
               ,p_known_region         := str_known_region
               ,p_cat_threashold_perc  := p_cat_threashold_perc
               ,p_evt_threashold_perc  := p_evt_threashold_perc
            );
            p_return_code    := rec.p_return_code;
            p_status_message := rec.p_status_message;
            
         END IF;
      
      ELSE
         RAISE EXCEPTION 'err %',p_cip_indexing_method;
         
      END IF;

      IF p_return_code != 0
      THEN
         RETURN;
         
      END IF;      
      
   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 70
   -- Return the full state-clipped catchment results
   ----------------------------------------------------------------------------
   IF p_nhdplus_version = 'nhdplus_m'
   THEN
      INSERT INTO tmp_cip_out(
          nhdplusid
         ,catchmentstatecode
         ,xwalk_huc12
         ,areasqkm
         ,shape
      )
      SELECT
       a.nhdplusid
      ,a.catchmentstatecode
      ,a.xwalk_huc12
      ,a.areasqkm
      ,CASE
       WHEN boo_return_geometry
       THEN
         a.shape
       ELSE
         CAST(NULL AS GEOMETRY)       
       END AS shape
      FROM
      cip20_nhdplus_m.catchment_fabric a
      WHERE
      EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
      AND (str_state_filter IS NULL OR a.catchmentstatecode = str_state_filter);
   
   ELSIF p_nhdplus_version = 'nhdplus_h'
   THEN
      INSERT INTO tmp_cip_out(
          nhdplusid
         ,catchmentstatecode
         ,xwalk_huc12
         ,areasqkm
         ,shape
      )
      SELECT
       a.nhdplusid
      ,a.catchmentstatecode
      ,a.xwalk_huc12
      ,a.areasqkm
      ,CASE
       WHEN boo_return_geometry
       THEN
         a.shape
       ELSE
         CAST(NULL AS GEOMETRY)       
       END AS shape
      FROM
      cip20_nhdplus_h.catchment_fabric a
      WHERE
      EXISTS (SELECT 1 FROM tmp_cip b WHERE b.nhdplusid = a.nhdplusid)
      AND (str_state_filter IS NULL OR a.catchmentstatecode = str_state_filter);
   
   ELSE
      RAISE EXCEPTION 'err';
   
   END IF;
   
   SELECT
    COUNT(*)
   ,SUM(a.areasqkm)
   INTO
    p_catchment_count
   ,p_catchment_areasqkm
   FROM
   tmp_cip_out a;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Clip by state if requested and remeasure for results
   ----------------------------------------------------------------------------
   IF p_state_filter IS NOT NULL
   AND p_state_filter NOT IN ('AK','HI','AS')
   THEN
      rec := cip20_support.clip_by_state(
          p_geometry     := p_indexed_geometry
         ,p_known_region := str_known_region
         ,p_state_filter := p_state_filter
      );
      p_indexed_geometry := rec.p_clipped_geometry;
      p_return_code      := rec.p_return_code;
      p_status_message   := rec.p_status_message;
      
      IF p_return_code != 0
      THEN
         RETURN;
         
      END IF;
      
      rec := cip20_engine.measure_geometry(
          p_geometry        := p_indexed_geometry
         ,p_nhdplus_version := p_nhdplus_version
         ,p_known_region    := str_known_region
      );
      p_indexed_geom_measure   := rec.p_measurement;
      p_indexed_geom_meas_unit := rec.p_unit;
      ary_idx_geom_meas        := rec.p_sub_meas;
      p_return_code            := rec.p_return_code;
      p_status_message         := rec.p_status_message;
      
      IF p_return_code != 0
      THEN
         RETURN;
         
      END IF;
      
   END IF;   
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.cip20_index(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.cip20_index(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
) TO PUBLIC;

