#/bin/sh

echo "altering postgresql.conf"
envsubst < /home/postgres/alter_pg.sed > /home/postgres/alter_pg2.sed
sed -i -f /home/postgres/alter_pg2.sed /var/lib/postgresql/data/postgresql.conf
echo "altering pg_hba.conf"
echo "host    all    all    0.0.0.0/0    md5" >> /var/lib/postgresql/data/pg_hba.conf

echo "creating users"
envsubst < /home/postgres/user_creation.sql | psql 

echo "creating database"
envsubst < /home/postgres/db_creation.sql   | psql 

echo "configuring database"
envsubst < /home/postgres/db_config.sql     | psql -d ${POSTGRESQL_DB}

echo "configuring schemas"
envsubst < /home/postgres/schema_config.sql | psql -d ${POSTGRESQL_DB} -U cipsrv

if [ -e "/home/postgres/pgrst_watch.sql" ]; then
   psql -d ${POSTGRESQL_DB} -f /home/postgres/pgrst_watch.sql
fi

echo "db setup complete"
