GRANT CREATE ON DATABASE ${POSTGRES_DB} TO cip20_user,cip20_upload,cip20_pgrest;

CREATE SCHEMA cip20           AUTHORIZATION cip20;
CREATE SCHEMA cip20_engine    AUTHORIZATION cip20;
CREATE SCHEMA cip20_nhdplus_m AUTHORIZATION cip20;
CREATE SCHEMA cip20_nhdplus_h AUTHORIZATION cip20;
CREATE SCHEMA cip20_support   AUTHORIZATION cip20;
CREATE SCHEMA cip20_pgrest    AUTHORIZATION cip20_pgrest;

GRANT  USAGE ON SCHEMA cip20_engine    TO cip20_pgrest;
GRANT  USAGE ON SCHEMA cip20_support   TO cip20_pgrest;
GRANT  USAGE ON SCHEMA cip20_nhdplus_m TO cip20_pgrest;
GRANT  USAGE ON SCHEMA cip20_nhdplus_h TO cip20_pgrest;

CREATE FUNCTION cip20_pgrest.test() RETURNS JSON AS $$ BEGIN RETURN json_object_agg('works',TRUE); END; $$ LANGUAGE 'plpgsql';
ALTER  FUNCTION cip20_pgrest.test() OWNER TO cip20_pgrest;
