DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.edges2rings';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.edges2rings()
RETURNS BOOLEAN
VOLATILE
AS $BODY$ 
DECLARE
   int_edge_id        INTEGER;
   ary_edge_id        INTEGER[];
   ary_interior_side  VARCHAR(1)[];
   ary_start_node_id  INTEGER[];
   ary_end_node_id    INTEGER[];
   ary_shape          GEOMETRY[];
   int_touch_count    INTEGER;
   
   str_snake_interior VARCHAR(1);
   int_snake_start_id INTEGER;
   int_snake_end_id   INTEGER;
   sdo_snake_shape    GEOMETRY;
   ary_snake_body     INTEGER[];
   
   boo_forwards       BOOLEAN;
   boo_backwards      BOOLEAN;
   boo_ccw            BOOLEAN;
   str_ring_type      VARCHAR(1);
   int_sanity         INTEGER;
   
BEGIN

   -----------------------------------------------------------------------------
   -- Step 10
   -- Pick out a random edge
   -----------------------------------------------------------------------------
   SELECT
    a.edge_id
   ,a.interior_side
   ,a.start_node_id
   ,a.end_node_id
   ,a.shape
   ,a.touch_count
   INTO
    int_edge_id
   ,str_snake_interior
   ,int_snake_start_id
   ,int_snake_end_id
   ,sdo_snake_shape
   ,int_touch_count
   FROM
   tmp_delineation_edges a
   ORDER BY
   a.touch_count ASC
   LIMIT 1;
      
   IF int_edge_id IS NULL
   THEN  
      RETURN FALSE;
      
   END IF;

   ary_snake_body := ARRAY[int_edge_id];
   
   -----------------------------------------------------------------------------
   -- Step 20
   -- Grow the search snake
   -----------------------------------------------------------------------------
   boo_forwards  := TRUE;
   boo_backwards := TRUE;
   
   WHILE boo_forwards AND boo_backwards
   LOOP
      SELECT
       array_agg(a.edge_id)
      ,array_agg(a.interior_side)
      ,array_agg(a.start_node_id)
      ,array_agg(a.end_node_id)
      ,array_agg(a.shape)
      INTO
       ary_edge_id
      ,ary_interior_side
      ,ary_start_node_id
      ,ary_end_node_id
      ,ary_shape
      FROM (
         SELECT
          aa.edge_id
         ,CASE
          WHEN int_snake_end_id = aa.start_node_id
          THEN
            aa.interior_side
          ELSE
            CASE
            WHEN aa.interior_side = 'R'
            THEN
              'L'::VARCHAR(1)
            ELSE
              'R'::VARCHAR(1)
            END
          END AS interior_side
         ,CASE
          WHEN int_snake_end_id = aa.start_node_id
          THEN
            aa.start_node_id
          ELSE
            aa.end_node_id
          END AS start_node_id
         ,CASE
          WHEN int_snake_end_id = aa.start_node_id
          THEN
            aa.end_node_id
          ELSE
            aa.start_node_id
          END AS end_node_id
         ,CASE
          WHEN int_snake_end_id = aa.start_node_id
          THEN
            aa.shape
          ELSE
            ST_Reverse(aa.shape)
          END AS shape
         FROM
         tmp_delineation_edges aa
         WHERE (
               ( int_snake_end_id = aa.start_node_id AND aa.interior_side  = str_snake_interior )
            OR ( int_snake_end_id = aa.end_node_id   AND aa.interior_side <> str_snake_interior )
         ) 
         AND aa.edge_id <> ALL(ary_snake_body)            
      ) a;
      
      IF array_length(ary_edge_id,1) = 1
      THEN
         int_snake_end_id := ary_end_node_id[1];
         sdo_snake_shape  := ST_MakeLine(sdo_snake_shape,ary_shape[1]);
         ary_snake_body   := array_append(ary_snake_body,ary_edge_id[1]);

      ELSE
         boo_forwards := FALSE;
      
      END IF;
      
      SELECT
       array_agg(a.edge_id)
      ,array_agg(a.interior_side)
      ,array_agg(a.start_node_id)
      ,array_agg(a.end_node_id)
      ,array_agg(a.shape)
      INTO
       ary_edge_id
      ,ary_interior_side
      ,ary_start_node_id
      ,ary_end_node_id
      ,ary_shape
      FROM (
         SELECT
          aa.edge_id
         ,CASE
          WHEN int_snake_start_id = aa.end_node_id
          THEN
            aa.interior_side
          ELSE
            CASE
            WHEN aa.interior_side = 'R'
            THEN
              'L'::VARCHAR(1)
            ELSE
              'R'::VARCHAR(1)
            END
          END AS interior_side
         ,CASE
          WHEN int_snake_start_id = aa.end_node_id
          THEN
            aa.start_node_id
          ELSE
            aa.end_node_id
          END AS start_node_id
         ,CASE
          WHEN int_snake_start_id = aa.end_node_id
          THEN
            aa.end_node_id
          ELSE
            aa.start_node_id
          END AS end_node_id
         ,CASE
          WHEN int_snake_start_id = aa.end_node_id
          THEN
            aa.shape
          ELSE
            ST_Reverse(aa.shape)
          END AS shape
         FROM
         tmp_delineation_edges aa
         WHERE (
               ( int_snake_start_id = aa.end_node_id   AND aa.interior_side  = str_snake_interior )
            OR ( int_snake_start_id = aa.start_node_id AND aa.interior_side <> str_snake_interior )
         ) 
         AND aa.edge_id <> ALL(ary_snake_body)            
      ) a;
      
      IF array_length(ary_edge_id,1) = 1
      THEN
         int_snake_start_id := ary_start_node_id[1];
         sdo_snake_shape    := ST_MakeLine(ary_shape[1],sdo_snake_shape);
         ary_snake_body     := array_append(ary_snake_body,ary_edge_id[1]);

      ELSE
         boo_backwards := FALSE;
      
      END IF;
      
      int_sanity := int_sanity + 1;
      
      IF int_sanity > 50000
      THEN
         RAISE EXCEPTION 'sanity check';
      
      END IF;
      
   END LOOP;
   
   -----------------------------------------------------------------------------
   -- Test for completed ring
   -----------------------------------------------------------------------------
   IF int_snake_start_id = int_snake_end_id
   THEN
      boo_ccw := ST_IsPolygonCCW(ST_MakePolygon(sdo_snake_shape));
      
      IF boo_ccw AND str_snake_interior = 'L'
      OR NOT boo_ccw AND str_snake_interior = 'R'
      THEN
         str_ring_type := 'E';
         
      ELSE
         str_ring_type := 'I';
         
      END IF;
      
      INSERT INTO tmp_delineation_rings(
          ring_id
         ,ring_type
         ,shape
      ) VALUES (
          int_edge_id
         ,str_ring_type
         ,sdo_snake_shape
      );
      
      DELETE FROM tmp_delineation_edges
      WHERE edge_id = ANY(ary_snake_body);
   
   -----------------------------------------------------------------------------
   -- Scrunch the edges
   -----------------------------------------------------------------------------
   ELSE
      DELETE FROM tmp_delineation_edges
      WHERE edge_id = ANY(ary_snake_body);
      
      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape 
         ,touch_count
      ) VALUES (
          int_edge_id
         ,str_snake_interior
         ,int_snake_start_id
         ,int_snake_end_id
         ,sdo_snake_shape
         ,int_touch_count + 1
      );
   
   END IF;
   
   RETURN TRUE;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.edges2rings
OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.edges2rings
TO PUBLIC;

