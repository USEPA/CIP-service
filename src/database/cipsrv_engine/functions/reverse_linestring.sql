DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.reverse_linestring';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.reverse_linestring(
    IN  p_geometry          GEOMETRY
) RETURNS GEOMETRY
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_lrs_output    GEOMETRY;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF ST_GeometryType(p_geometry)  <> 'ST_LineString'
   THEN
      RAISE EXCEPTION 'geometry must a single linestring';
      
   END IF;
   
   IF ST_M(
      ST_StartPoint(p_geometry)
   ) IS NULL
   THEN
      RETURN ST_Reverse(p_geometry);
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Reverse the linestring taking measures along for a ride
   --------------------------------------------------------------------------
   SELECT
   ST_MakeLine(a.geom)
   INTO
   sdo_lrs_output
   FROM (
      SELECT
      aa.geom
      FROM (
         SELECT (ST_DumpPoints(
            p_geometry
         )).*
      ) aa
      ORDER BY
      aa.path[1] DESC
   ) a;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Return the results
   --------------------------------------------------------------------------
   RETURN sdo_lrs_output;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.reverse_linestring(
    GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.reverse_linestring(
    GEOMETRY
) TO PUBLIC;
