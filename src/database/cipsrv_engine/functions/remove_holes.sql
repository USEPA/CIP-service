DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.remove_holes';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.remove_holes(
    IN  p_input          GEOMETRY
   ,IN  p_threshold_sqkm NUMERIC DEFAULT NULL
) RETURNS GEOMETRY
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_results   GEOMETRY;
   ary_testing   GEOMETRY[];
   ary_clean     GEOMETRY[];
   int_counter   INTEGER;
   int_counter2  INTEGER;
   outer_ring    GEOMETRY;
   inner_rings   GEOMETRY[];
   inner_ring    GEOMETRY;
   num_ring_size NUMERIC;
   int_cnt_ring  INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Short circuit if not polygon or polygon only has a single outer ring
   ----------------------------------------------------------------------------
   IF p_input IS NULL OR ST_ISEMPTY(p_input)
   THEN
      RETURN NULL;
      
   END IF;   
   
   IF GEOMETRYTYPE(p_input) NOT IN ('POLYGON','MULTIPOLYGON')
   OR ST_NRINGS(p_input) = 1
   THEN
      RETURN p_input;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- To remove all inner rings, just grab outer rings sorted by size
   ----------------------------------------------------------------------------
   IF  p_threshold_sqkm IS NULL
   THEN
      SELECT
      ARRAY_AGG(a.shape)
      INTO
      ary_testing
      FROM (
         SELECT
         aa.shape
         FROM (
            SELECT
            ST_MAKEPOLYGON(aaa.shape) AS shape
            FROM (
               SELECT
               ST_EXTERIORRING(aaaa.geom) AS shape
               FROM
               ST_DUMP(p_input) aaaa
            ) aaa
         ) aa
         ORDER BY
         ST_AREA(aa.shape) DESC
      ) a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Exit early if only one outer ring found
   ----------------------------------------------------------------------------
      IF ARRAY_LENGTH(ary_testing,1) = 1
      THEN
         RETURN ary_testing[1];
         
      END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Flush away any exterior rings inside largest exterior ring
   -- This could be more robust for whacked out multi-polygons having lakes
   -- with islands with ponds with even smaller islands
   ----------------------------------------------------------------------------
      ary_clean[1] := ary_testing[1];
      
      int_counter := 2;
      FOR i IN 2 .. ARRAY_LENGTH(ary_testing,1)
      LOOP
         IF NOT ST_WITHIN(ary_testing[i],ary_clean[1])
         THEN
            ary_clean[int_counter] := ary_testing[i];
            int_counter := int_counter + 1;
         
         END IF;
      
      END LOOP;
      
   ELSE
   ----------------------------------------------------------------------------
   -- Step 50
   -- Break down multis into array of polygons sorted by size of outer rings
   ----------------------------------------------------------------------------
      SELECT
      ARRAY_AGG(a.shape)
      INTO
      ary_testing
      FROM (
         SELECT
         aa.shape
         FROM (
            SELECT
            aaa.geom AS shape
            FROM
            ST_DUMP(p_input) aaa
         ) aa
         ORDER BY
         ST_AREA(
            ST_MAKEPOLYGON(ST_EXTERIORRING(aa.shape))
         ) DESC
      ) a;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Remake each polygon applying threshold
   ----------------------------------------------------------------------------      
      int_counter := 1;
      FOR i IN 1 .. ARRAY_LENGTH(ary_testing,1)
      LOOP
         int_cnt_ring := ST_NUMINTERIORRINGS(ary_testing[i]);
         
         IF int_cnt_ring = 0
         THEN
            ary_clean[int_counter] := ary_testing[i];
            
         ELSE
            outer_ring   := ST_EXTERIORRING(ary_testing[i]);
            inner_rings  := ARRAY[]::GEOMETRY[];
         
            int_counter2 := 1;
            FOR j IN 1 .. int_cnt_ring
            LOOP
               inner_ring := ST_INTERIORRINGN(ary_testing[i],j);
               num_ring_size := ST_AREA(ST_TRANSFORM(ST_MAKEPOLYGON(inner_ring),4326)::GEOGRAPHY)::NUMERIC / 1000000;
               
               IF num_ring_size > p_threshold_sqkm
               THEN
                  inner_rings[int_counter2] := inner_ring;
                  int_counter2 := int_counter2 + 1;
               
               END IF;
            
            END LOOP;
            
            IF ARRAY_LENGTH(inner_rings,1) = 0
            THEN
               ary_clean[int_counter] := ST_MAKEPOLYGON(outer_ring);
               
            ELSE
               ary_clean[int_counter] := ST_MAKEPOLYGON(outer_ring,inner_rings);
            
            END IF;
         
         END IF;

         int_counter := int_counter + 1;         
      
      END LOOP;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Rebuild into geometry
   ----------------------------------------------------------------------------
   sdo_results := ST_COLLECT(ary_clean);
   
   RETURN sdo_results;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.remove_holes';
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

