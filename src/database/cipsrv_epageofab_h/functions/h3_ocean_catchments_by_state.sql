DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_epageofab_h.h3_ocean_catchments_by_state';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_epageofab_h.h3_ocean_catchments_by_state(
    IN  p_srid                 INTEGER
   ,IN  p_stusps               VARCHAR
   ,IN  p_state_clip           BOOLEAN DEFAULT TRUE
) RETURNS TABLE(
    objectid           INTEGER
   ,catchmentstatecode VARCHAR(2)
   ,h3hexagonaddr      VARCHAR(64)
   ,areasqkm           NUMERIC
   ,shape              GEOMETRY
)
STABLE
AS $BODY$
DECLARE
   rec                RECORD;
   rec2               RECORD;
   cnt                INTEGER;
   str_src_state      VARCHAR;
   str_src_cats       VARCHAR;
   boo_changed        BOOLEAN;
   geom_state         GEOMETRY;
   geom_diff          GEOMETRY;
   num_areasqkm       NUMERIC;

BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   str_src_state := 'tiger_fedstatewaters_' || p_srid::VARCHAR;
   
   IF NOT cipsrv_engine.resource_exists('cipsrv_support',str_src_state)
   THEN
      RAISE EXCEPTION 'fedstatewaters table not found - cipsrv_support.%',str_src_state;
   
   END IF;

   str_src_cats := 'nhdpluscatchment_' || p_srid::VARCHAR;
   
   IF NOT cipsrv_engine.resource_exists('cipsrv_nhdplus_h',str_src_cats)
   THEN
      RAISE EXCEPTION 'catchment table not found - cipsrv_nhdplus_h.%',str_src_cats;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Capture the state polygon in case of clip by state
   --------------------------------------------------------------------------
   EXECUTE '
      SELECT
      cipsrv_nhdplus_h.snap_to_common_grid(
          a.shape
         ,$1
         ,0.05
      ) AS shape
      FROM 
      cipsrv_support.' || str_src_state || ' a
      WHERE
      a.stusps = $2
   '
   USING p_srid::VARCHAR,p_stusps
   INTO geom_state;
   
   IF geom_state IS NULL
   OR ST_ISEMPTY(geom_state)
   THEN
      RAISE EXCEPTION '% not found in %',p_stusps,str_src_state;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Get the H3 polygons covering the polygon
   --------------------------------------------------------------------------
   cnt := 0;
   FOR rec IN EXECUTE '
      SELECT
       a.stusps
      ,a.h3hexagonaddr
      ,ST_AREA(a.shape)::NUMERIC / 1000000 AS areasqkm
      ,cipsrv_nhdplus_h.snap_to_common_grid(
          a.shape
         ,$1
         ,0.05
      ) AS shape
      FROM (
         SELECT
          aa.stusps
         ,aa.h3hexagonaddr::VARCHAR
         ,ST_TRANSFORM(h3_cell_to_boundary_geometry(aa.h3hexagonaddr),$2) AS shape
         FROM (
            SELECT
             $3 AS stusps
            ,H3_POLYGON_TO_CELLS(ST_TRANSFORM($4,4326),8) AS h3hexagonaddr
         ) aa	
      ) a
   '
   USING p_srid::VARCHAR,p_srid,p_stusps,geom_state
   LOOP
      boo_changed  := FALSE;
      geom_diff    := rec.shape;
      
      IF p_state_clip
      THEN
         boo_changed  := TRUE;
         
         geom_diff := ST_COLLECTIONEXTRACT(
             ST_INTERSECTION(
                geom_diff
               ,geom_state
             )
            ,3
         );
         
         IF  geom_diff IS NULL
         OR ST_ISEMPTY(geom_diff)
         THEN
            CONTINUE;
         
         END IF;
   
      END IF;
      
      FOR rec2 IN EXECUTE '
         SELECT
          a.nhdplusid
         ,cipsrv_nhdplus_h.snap_to_common_grid(
             a.shape
            ,$1
            ,0.05
         ) AS shape
         FROM
         cipsrv_nhdplus_h.' || str_src_cats || ' a
         WHERE
         ST_INTERSECTS(a.shape,$2)
      '
      USING p_srid::VARCHAR,geom_diff
      LOOP
         boo_changed := TRUE;
         
         geom_diff := ST_COLLECTIONEXTRACT(
             ST_DIFFERENCE(
                geom_diff
               ,rec2.shape
             )
            ,3
         );
         
         IF  geom_diff IS NULL
         OR ST_ISEMPTY(geom_diff)
         THEN
            CONTINUE;
         
         END IF;
      
      END LOOP;
      
      IF NOT boo_changed
      THEN
         cnt := cnt + 1;
         
         RETURN QUERY
         SELECT 
          cnt
         ,rec.stusps
         ,rec.h3hexagonaddr
         ,rec.areasqkm
         ,ST_TRANSFORM(rec.shape,4269);
         
         CONTINUE;
         
      ELSE
         IF  geom_diff IS NOT NULL
         AND NOT ST_ISEMPTY(geom_diff)
         THEN
            num_areasqkm := ST_AREA(ST_TRANSFORM(geom_diff,4326)::GEOGRAPHY)::NUMERIC / 1000000;
            
            IF num_areasqkm > 0.00000005
            THEN
               cnt := cnt + 1;
               
               RETURN QUERY
               SELECT
                cnt
               ,rec.stusps
               ,rec.h3hexagonaddr
               ,num_areasqkm
               ,ST_TRANSFORM(geom_diff,4269);
               
               CONTINUE;
               
            END IF;
            
         END IF;
         
      END IF;
   
   END LOOP;
   
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_epageofab_h.h3_ocean_catchments_by_state';
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

