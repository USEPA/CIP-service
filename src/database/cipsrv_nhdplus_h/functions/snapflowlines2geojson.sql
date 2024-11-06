DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.snapflowlines2geojson';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.snapflowlines2geojson(
    IN  p_input                        cipsrv_nhdplus_h.snapflowline[]
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   RETURN JSONB_BUILD_OBJECT(
       'type'                ,'FeatureCollection'
      ,'features'            ,ARRAY[(
         SELECT
         cipsrv_nhdplus_h.snapflowline2geojson(x)
         FROM
         UNNEST(p_input) AS x
      )]
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.snapflowlines2geojson(
    cipsrv_nhdplus_h.snapflowline[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.snapflowlines2geojson(
    cipsrv_nhdplus_h.snapflowline[]
) TO PUBLIC;
