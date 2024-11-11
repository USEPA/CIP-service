GRANT CREATE ON DATABASE ${POSTGRESQL_DB} TO cipsrv_user,cipsrv_upload,cipsrv_pgrest;

CREATE SCHEMA cipsrv               AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_engine        AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_nhdplus_m     AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_nhdplusgrid_m AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_nhdplustopo_m AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_nhdplus_h     AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_nhdplusgrid_h AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_nhdplustopo_h AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_support       AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_owld          AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_tap           AUTHORIZATION cipsrv;
CREATE SCHEMA cipsrv_pgrest        AUTHORIZATION cipsrv_pgrest;
CREATE SCHEMA cipsrv_upload        AUTHORIZATION cipsrv_upload;
CREATE SCHEMA cipsrv_gis           AUTHORIZATION cipsrv_gis;

GRANT  USAGE ON SCHEMA cipsrv_engine        TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_support       TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_owld          TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_nhdplus_m     TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_nhdplusgrid_m TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_nhdplustopo_m TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_nhdplus_h     TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_nhdplusgrid_h TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_nhdplustopo_h TO cipsrv_pgrest;
GRANT  USAGE ON SCHEMA cipsrv_pgrest        TO cipsrv;
GRANT  ALL   ON SCHEMA cipsrv_gis           TO cipsrv;
GRANT  USAGE ON SCHEMA cipsrv_upload        TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_engine        TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_support       TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_owld          TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_nhdplus_m     TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_nhdplusgrid_m TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_nhdplustopo_m TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_nhdplus_h     TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_nhdplusgrid_h TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_nhdplustopo_h TO cipsrv_upload;
GRANT  USAGE ON SCHEMA cipsrv_tap           TO public;
GRANT  USAGE ON SCHEMA cipsrv_nhdplus_m     TO cipsrv_gis;
GRANT  USAGE ON SCHEMA cipsrv_nhdplusgrid_m TO cipsrv_gis;
GRANT  USAGE ON SCHEMA cipsrv_nhdplustopo_m TO cipsrv_gis;
GRANT  USAGE ON SCHEMA cipsrv_nhdplus_h     TO cipsrv_gis;
GRANT  USAGE ON SCHEMA cipsrv_nhdplusgrid_h TO cipsrv_gis;
GRANT  USAGE ON SCHEMA cipsrv_nhdplustopo_h TO cipsrv_gis;
GRANT  USAGE ON SCHEMA cipsrv_support       TO cipsrv_gis;
GRANT  USAGE ON SCHEMA cipsrv_owld          TO cipsrv_gis;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.healthcheck() RETURNS JSONB IMMUTABLE AS 'BEGIN RETURN JSONB_BUILD_OBJECT(''status'',''ok''); END;' LANGUAGE 'plpgsql';
ALTER  FUNCTION cipsrv_pgrest.healthcheck() OWNER TO cipsrv_pgrest;

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
