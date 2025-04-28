DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.clean_geometry';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.clean_geometry(
    IN  p_geometry          GEOMETRY
   ,IN  p_colletiontype     INTEGER DEFAULT NULL
) RETURNS GEOMETRY
IMMUTABLE
AS $BODY$
DECLARE
   str_geometry VARCHAR;
   int_numgeoms INTEGER;
   rez          GEOMETRY;
   
BEGIN

   IF p_geometry IS NULL
   OR ST_ISEMPTY(p_geometry)
   THEN
      RETURN NULL;
      
   END IF;
   
   str_geometry := ST_GeometryType(p_geometry);
   int_numgeoms := ST_NumGeometries(p_geometry);
   
   IF  str_geometry IN ('ST_MultiPoint','ST_MultiLineString','ST_MultiPolygon') 
   AND int_numgeoms = 1
   THEN
      RETURN ST_GeometryN(p_geometry,1);
   
   ELSIF  str_geometry = 'ST_GeometryCollection' 
   AND p_colletiontype IS NOT NULL
   THEN
      rez := ST_COLLECTIONEXTRACT(p_geometry,p_colletiontype);
      
      str_geometry := ST_GeometryType(rez);
      int_numgeoms := ST_NumGeometries(rez);
      
      IF  str_geometry IN ('ST_MultiPoint','ST_MultiLineString','ST_MultiPolygon') 
      AND int_numgeoms = 1
      THEN
         RETURN ST_GeometryN(rez,1);
         
      ELSE
         RETURN rez;
         
      END IF;
      
   ELSE
      RETURN p_geometry;
   
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.clean_geometry';
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
