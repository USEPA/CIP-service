DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.adjust_point_extent';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.adjust_point_extent(
    IN  p_extent_value      NUMERIC
   ,IN  p_direction         VARCHAR
   ,IN  p_flowline_amount   NUMERIC
   ,IN  p_flowline_fmeasure NUMERIC
   ,IN  p_flowline_tmeasure NUMERIC
   ,IN  p_event_measure     NUMERIC
) RETURNS NUMERIC 
IMMUTABLE 
AS $BODY$
DECLARE
   str_direction VARCHAR := UPPER(p_direction);
   num_amount    NUMERIC;

BEGIN
   
   IF str_direction IN ('UT','UM','U')
   THEN
      IF p_flowline_tmeasure = p_event_measure
      THEN
         RETURN p_extent_value;

      END IF;
      
      num_amount := p_flowline_amount * ((p_flowline_tmeasure - p_event_measure) / (p_flowline_tmeasure - p_flowline_fmeasure));
      
   ELSIF str_direction IN ('DM','DD','PP','D')
   THEN
      IF p_flowline_fmeasure = p_event_measure
      THEN
         RETURN p_extent_value;

      END IF;
      
      num_amount := p_flowline_amount * ((p_event_measure - p_flowline_fmeasure) / (p_flowline_tmeasure - p_flowline_fmeasure));
      
   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;
   
   RETURN p_extent_value - num_amount;

END;
$BODY$ LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.adjust_point_extent(
    NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.adjust_point_extent(
    NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

