#/bin/sh

echo "altering postgresql.conf"
sed -i -e "s/^#*listen_addresses =.*$/listen_addresses = '*'/"                                                      /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*logging_collector = off/logging_collector = on/"                                                    /var/lib/postgresql/data/postgresql.conf

sed -i -e "s/^#*max_connections =.*$/max_connections = ${PG_MAX_CONNECTIONS}/"                                      /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*shared_buffers =.*$/shared_buffers = ${PG_SHARED_BUFFERS}/"                                         /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*effective_cache_size =.*$/effective_cache_size = ${PG_EFFECTIVE_CACHE_SIZE}/"                       /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*maintenance_work_mem =.*$/maintenance_work_mem = ${PG_MAINTENANCE_WORK_MEM}/"                       /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*checkpoint_completion_target =.*$/checkpoint_completion_target = ${PG_CHECKPOINT_COMP_TARG}/"       /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*wal_buffers =.*$/wal_buffers = ${PG_WAL_BUFFERS}/"                                                  /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*default_statistics_target =.*$/default_statistics_target = ${PG_DEF_STATISTICS_TARG}/"              /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*random_page_cost =.*$/random_page_cost = ${PG_RANDOM_PAGE_COST}/"                                   /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*effective_io_concurrency =.*$/effective_io_concurrency = ${PG_EFFECTIVE_IO_CONC}/"                  /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*work_mem =.*$/work_mem = ${PG_WORK_MEM}/"                                                           /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*min_wal_size =.*$/min_wal_size = ${PG_MIN_WAL_SIZE}/"                                               /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*max_wal_size =.*$/max_wal_size = ${PG_MAX_WAL_SIZE}/"                                               /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*max_worker_processes =.*$/max_worker_processes = ${PG_MAX_WORKER_PROCESSES}/"                       /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*max_parallel_workers_per_gather =.*$/max_parallel_workers_per_gather = ${PG_MAX_PAR_WORK_PER_GAT}/" /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*max_parallel_workers =.*$/max_parallel_workers = ${PG_MAX_PAR_WORKERS}/"                            /var/lib/postgresql/data/postgresql.conf
sed -i -e "s/^#*max_parallel_maintenance_workers =.*$/max_parallel_maintenance_workers = ${PG_MAX_PAR_MAINT_WORK}/" /var/lib/postgresql/data/postgresql.conf

echo "altering pg_hba.conf"
echo "host    all    all    0.0.0.0/0    md5" >> /var/lib/postgresql/data/pg_hba.conf

echo "creating users"
psql -c "CREATE USER cip20         WITH PASSWORD '${POSTGRES_CIP_PASS}';"
psql -c "CREATE USER cip20_user    WITH PASSWORD '${POSTGRES_USR_PASS}';"
psql -c "CREATE USER cip20_pgrest  WITH PASSWORD '${POSTGREST_PASS}';"

echo "creating database"
psql -c "CREATE DATABASE ${POSTGRES_DB};"
psql -c "CREATE EXTENSION IF NOT EXISTS hstore;"        ${POSTGRES_DB}
psql -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";" ${POSTGRES_DB}
psql -c "CREATE EXTENSION IF NOT EXISTS postgis;"       ${POSTGRES_DB}
psql -c "CREATE EXTENSION IF NOT EXISTS pgrouting;"     ${POSTGRES_DB}

psql -c "ALTER DATABASE ${POSTGRES_DB} OWNER TO cip20;"
psql -c "GRANT CREATE ON DATABASE ${POSTGRES_DB} TO cip20_user;"
if [ -e "/home/postgres/pgrst_watch.sql" ]; then
   psql -d ${POSTGRES_DB} -a -f /home/postgres/pgrst_watch.sql
fi

echo "creating schemas"
psql -c "GRANT ALL ON TABLE public.spatial_ref_sys TO cip20_user;" ${POSTGRES_DB}
psql -c "CREATE SCHEMA cip20 AUTHORIZATION cip20;"                 ${POSTGRES_DB}
psql -c "CREATE SCHEMA cip20_engine AUTHORIZATION cip20;"          ${POSTGRES_DB}
psql -c "CREATE SCHEMA cip20_nhdplus_m AUTHORIZATION cip20;"       ${POSTGRES_DB}
psql -c "CREATE SCHEMA cip20_nhdplus_h AUTHORIZATION cip20;"       ${POSTGRES_DB}
psql -c "CREATE SCHEMA cip20_support AUTHORIZATION cip20;"         ${POSTGRES_DB}
psql -c "CREATE SCHEMA cip20_pgrest AUTHORIZATION cip20;"          ${POSTGRES_DB}
psql -c "ALTER  SCHEMA cip20_pgrest OWNER TO cip20_pgrest;"        ${POSTGRES_DB}
psql -c "GRANT  USAGE ON SCHEMA cip20_engine TO cip20_pgrest;"     ${POSTGRES_DB}
psql -c "GRANT  USAGE ON SCHEMA cip20_support TO cip20_pgrest;"    ${POSTGRES_DB}
psql -c "GRANT  USAGE ON SCHEMA cip20_nhdplus_m TO cip20_pgrest;"  ${POSTGRES_DB}
psql -c "GRANT  USAGE ON SCHEMA cip20_nhdplus_h TO cip20_pgrest;"  ${POSTGRES_DB}

echo "adding postgREST test"
psql -c "CREATE FUNCTION cip20_pgrest.test() RETURNS JSON AS \$\$ BEGIN RETURN json_object_agg('works',TRUE); END; \$\$ LANGUAGE 'plpgsql';" ${POSTGRES_DB}
psql -c "ALTER  FUNCTION cip20_pgrest.test() OWNER TO cip20_pgrest;" ${POSTGRES_DB}

echo "db setup complete"

