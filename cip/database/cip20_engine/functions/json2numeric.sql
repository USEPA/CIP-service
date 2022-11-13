CREATE OR REPLACE FUNCTION cip20_engine.json2numeric(
    IN  p_in                         JSONB
) RETURNS NUMERIC
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   IF p_in IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   IF JSONB_TYPEOF(p_in) = 'string'
   THEN
      IF p_in::VARCHAR IN ('',' ','null','""')
      THEN
         RETURN NULL;

      ELSE            
         RETURN REPLACE(
            p_in::VARCHAR
           ,'"'
           ,''           
         )::NUMERIC;
         
      END IF;
   
   ELSE
      RETURN p_in;

   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.json2numeric(
   JSONB
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.json2numeric(
   JSONB
) TO PUBLIC;

