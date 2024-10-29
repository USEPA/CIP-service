CREATE USER cipsrv         WITH PASSWORD '${POSTGRESQL_CIP_PASS}';

CREATE USER cipsrv_user    WITH PASSWORD '${POSTGRESQL_USR_PASS}';
GRANT cipsrv_user TO cipsrv;

CREATE USER cipsrv_upload  WITH PASSWORD '${POSTGRESQL_UPL_PASS}';
GRANT cipsrv_upload TO cipsrv;

CREATE USER cipsrv_geoserver  WITH PASSWORD '${POSTGRESQL_GSV_PASS}';
GRANT cipsrv_geoserver TO cipsrv;

CREATE USER cipsrv_pgrest  WITH PASSWORD '${POSTGREST_PASS}';
GRANT cipsrv_pgrest TO cipsrv;
