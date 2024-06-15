GRANT CREATE ON DATABASE ${POSTGRESQL_DB} TO cipsrv_user,cipsrv_upload,cipsrv_pgrest;

CREATE SCHEMA cipsrv           AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_engine    AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_nhdplus_m AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_nhdplus_h AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_support   AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_pgrest    AUTHORIZATION cipsrv_pgrest;
CREATE SCHEMA cipsrv_upload    AUTHORIZATION cipsrv_upload;

GRANT  USAGE ON SCHEMA cipsrv_engine    TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_support   TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_nhdplus_m TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_nhdplus_h TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_pgrest    TO cipsrv;
GRANT  USAGE ON SCHEMA cipsrv_upload    TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_engine    TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_support   TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_nhdplus_m TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_nhdplus_h TO cipsrv_upload;

CREATE FUNCTION cipsrv_pgrest.test() RETURNS JSON AS $$ BEGIN RETURN json_object_agg('works',TRUE); END; $$ LANGUAGE 'plpgsql';
ALTER  FUNCTION cipsrv_pgrest.test() OWNER TO cipsrv_pgrest;

CREATE TABLE cipsrv_upload.batch_control(
    dataset_prefix     VARCHAR(255)
   ,upload_date        DATE
   ,point_count        INTEGER
   ,line_count         INTEGER
   ,area_count         INTEGER
   ,execute_date       DATE
   ,current_status     VARCHAR
   ,completion_date    DATE
   ,return_code        INTEGER
   ,status_message     VARCHAR
);
ALTER TABLE cipsrv_upload.batch_control OWNER TO cipsrv_upload;
GRANT SELECT ON cipsrv_upload.batch_control TO PUBLIC;
