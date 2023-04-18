CREATE OR REPLACE FUNCTION cipsrv_pgrest.healthcheck()
RETURNS JSONB
IMMUTABLE
AS
$BODY$ 
DECLARE
BEGIN
   
   RETURN JSON_BUILD_OBJECT(
       'status', 'ok'
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.healthcheck() 
OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.healthcheck() 
TO PUBLIC;

