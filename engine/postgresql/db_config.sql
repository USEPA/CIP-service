CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgrouting;

GRANT ALL ON TABLE public.spatial_ref_sys TO cipsrv,cipsrv_user,cipsrv_upload;
