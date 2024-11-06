DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.raindrop_st_value';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.raindrop_st_value(
    IN  p_raster                 RASTER
   ,IN  p_column_x               INTEGER
   ,IN  p_row_y                  INTEGER
   ,IN  p_offset_x               INTEGER
   ,IN  p_offset_y               INTEGER
) RETURNS INTEGER
IMMUTABLE
AS $BODY$ 
DECLARE 
BEGIN

   RETURN ST_Value(
       rast     := p_raster
      ,band     := 1
      ,x        := p_column_x - p_offset_x + 1
      ,y        := p_row_y    - p_offset_y + 1
      ,exclude_nodata_value := true
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.raindrop_st_value(
    RASTER
   ,INTEGER
   ,INTEGER
   ,INTEGER
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.raindrop_st_value(
    RASTER
   ,INTEGER
   ,INTEGER
   ,INTEGER
   ,INTEGER
) TO PUBLIC;
