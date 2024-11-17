DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.safe_concatenate_geom_segments';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.safe_concatenate_geom_segments(
    IN  p_geometry1           GEOMETRY
   ,IN  p_geometry2           GEOMETRY
) RETURNS GEOMETRY 
AS
$BODY$ 
DECLARE
   sdo_array_in      GEOMETRY[];
   sdo_array_in2     GEOMETRY[];
   sdo_concatenate   GEOMETRY;
   sdo_output        GEOMETRY;
   num_remove1       NUMERIC;
   num_remove2       NUMERIC;
   int_counter       INTEGER;
   int_sanity        INTEGER := 0;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_geometry1 IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF p_geometry2 IS NULL
   THEN
      RETURN p_geometry1;
      
   END IF;

   IF ST_GeometryType(p_geometry1) NOT IN ('ST_LineString','ST_MultiLineString')
   OR NOT cipsrv_engine.is_lrs(p_geometry := p_geometry1)
   THEN
      RAISE EXCEPTION 'geometry 1 must be LRS linestring';
      
   END IF;
   
   IF ST_GeometryType(p_geometry2) NOT IN ('ST_LineString','ST_MultiLineString')
   OR NOT cipsrv_engine.is_lrs(p_geometry := p_geometry2)
   THEN
      RAISE EXCEPTION 'geometry 2 must be LRS linestring';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Do the easiest solution of two single linestrings
   ----------------------------------------------------------------------------
   IF  ST_GeometryType(p_geometry1) = 'ST_LineString'
   AND ST_GeometryType(p_geometry2) = 'ST_LineString'
   THEN
      IF ST_M(ST_EndPoint(p_geometry1)) = ST_M(ST_StartPoint(p_geometry2))
      THEN
         RETURN ST_MakeLine(
             p_geometry1
            ,p_geometry2
         );
         
      ELSIF ST_M(ST_EndPoint(p_geometry2)) = ST_M(ST_StartPoint(p_geometry1))
      THEN
         RETURN ST_MakeLine(
             p_geometry2
            ,p_geometry1
         );
         
      ELSE
         RETURN ST_Collect(
             p_geometry2
            ,p_geometry1
         );
      
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Create an array of all the linestrings in both geometries
   ----------------------------------------------------------------------------
   SELECT
   array_agg(a.geom)
   INTO
   sdo_array_in
   FROM (
      SELECT (ST_Dump(
         ST_Collect(p_geometry1,p_geometry2)
      )).*
   ) a;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Set an anchor point for processing
   ----------------------------------------------------------------------------
   <<start_over>>
   LOOP
      num_remove1   := NULL;
      num_remove2   := NULL;
      sdo_array_in2 := NULL;
      
   ----------------------------------------------------------------------------
   -- Step 50
   -- Loop over all the linestrings and search for match
   ----------------------------------------------------------------------------
      <<outer_loop>>
      FOR i IN 1 .. array_length(sdo_array_in, 1)
      LOOP
         FOR j IN 1 .. array_length(sdo_array_in, 1)
         LOOP
            IF i <> j
            AND ST_M(ST_EndPoint(sdo_array_in[i])) = ST_M(ST_StartPoint(sdo_array_in[j]))
            THEN
               sdo_concatenate := ST_MakeLine(
                   sdo_array_in[i]
                  ,sdo_array_in[j]
               );
               
               IF ST_GeometryType(sdo_concatenate) = 'ST_LineString'
               THEN
                  num_remove1 := i;
                  num_remove2 := j;
                  EXIT outer_loop;
                  
               END IF;
               
            END IF;
            
         END LOOP;
         
      END LOOP outer_loop;

   --------------------------------------------------------------------------
   -- Step 60
   -- Bail if there are no matches in the mess
   --------------------------------------------------------------------------
      IF num_remove1 IS NULL
      THEN
         sdo_output := ST_Collect(sdo_array_in);
         
         IF ST_NumGeometries(sdo_output) = 1
         THEN
            RETURN ST_GeometryN(sdo_output,1);
         
         ELSE
            RETURN sdo_output;
         
         END IF;
         
      END IF;
  
   --------------------------------------------------------------------------
   -- Step 70
   -- Add match to start of array and remove parts from array
   --------------------------------------------------------------------------
      int_counter := 1;
      sdo_array_in2[int_counter] := sdo_concatenate;
      int_counter := int_counter + 1;
      
      FOR i IN 1 .. array_length(sdo_array_in,1)
      LOOP
         IF  i <> num_remove1
         AND i <> num_remove2
         THEN
            sdo_array_in2[int_counter] := sdo_array_in[i];
            int_counter := int_counter + 1;
            
         END IF;
         
      END LOOP;
      
      sdo_array_in := sdo_array_in2;
   
   --------------------------------------------------------------------------
   -- Step 80
   -- Check that loop is not stuck
   --------------------------------------------------------------------------
      IF int_sanity > array_length(sdo_array_in,1) * array_length(sdo_array_in,1)
      THEN
         sdo_output := ST_Collect(sdo_array_in);
         
         IF ST_NumGeometries(sdo_output) = 1
         THEN
            RETURN ST_GeometryN(sdo_output,1);
         
         ELSE
            RETURN sdo_output;
         
         END IF;
         
      END IF;
   
      int_sanity := int_sanity + 1;
      
   END LOOP start_over; 
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.safe_concatenate_geom_segments(
    GEOMETRY
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.safe_concatenate_geom_segments(
    GEOMETRY
   ,GEOMETRY
) TO PUBLIC;

