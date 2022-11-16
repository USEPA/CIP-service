CREATE USER cipsrv         WITH PASSWORD '${POSTGRES_CIP_PASS}';

CREATE USER cipsrv_user    WITH PASSWORD '${POSTGRES_USR_PASS}';
GRANT cipsrv_user TO cipsrv;

CREATE USER cipsrv_upload  WITH PASSWORD '${POSTGRES_UPL_PASS}';
GRANT cipsrv_upload TO cipsrv;

CREATE USER cipsrv_pgrest  WITH PASSWORD '${POSTGREST_PASS}';
GRANT cipsrv_pgrest TO cipsrv;
