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
    IN  p_input         GEOMETRY 
) RETURNS GEOMETRY
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_results   GEOMETRY;
   ary_testing   GEOMETRY[];
   ary_clean     GEOMETRY[];
   int_counter   INTEGER;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF GeometryType(p_input) NOT IN ('POLYGON','MULTIPOLYGON')
   OR ST_NRings(p_input) = 1
   THEN
      RETURN p_input;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Grab all exterior rings as array
   ----------------------------------------------------------------------------
   SELECT
   array_agg(a.shape)
   INTO
   ary_testing
   FROM (
      SELECT
      ST_MakePolygon(aa.shape) AS shape
      FROM (
         SELECT
         ST_ExteriorRing(aaa.geom) AS shape
         FROM
         ST_Dump(p_input) aaa
      ) aa
   ) a;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Flush away any interior islands
   ----------------------------------------------------------------------------
   IF array_length(ary_testing,1) = 1
   THEN
      RETURN ary_testing[1];
      
   ELSE
      ary_clean[1] := ary_testing[1];
      
      int_counter := 2;
      FOR i IN 2 .. array_length(ary_testing,1)
      LOOP
         IF NOT ST_Within(ary_testing[i],ary_clean[1])
         THEN
            ary_clean[int_counter] := ary_testing[i];
            int_counter := int_counter + 1;
         
         END IF;
      
      END LOOP;
   
      sdo_results := ST_Collect(ary_clean);
      
      RETURN sdo_results;
   
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.remove_holes(
   GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.remove_holes(
   GEOMETRY
) TO PUBLIC;

