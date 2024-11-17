DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.is_lrs';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.is_lrs(
    IN  p_geometry          GEOMETRY
) RETURNS BOOLEAN
AS
$BODY$
DECLARE
BEGIN

   IF ST_M(
      ST_PointN(
          ST_GeometryN(p_geometry,1)
         ,1
      )
   ) IS NULL
   OR ST_M(
      ST_PointN(
          ST_GeometryN(p_geometry,1)
         ,ST_NumPoints(ST_GeometryN(p_geometry,1))
      )
   ) IS NULL
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.is_lrs(
   GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.is_lrs(
   GEOMETRY
) TO PUBLIC;


