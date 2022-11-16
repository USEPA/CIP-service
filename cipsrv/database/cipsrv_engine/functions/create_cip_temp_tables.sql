CREATE OR REPLACE FUNCTION cipsrv_engine.create_cip_temp_tables()
RETURNS INT4
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_cip temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_cip')
   THEN
      TRUNCATE TABLE tmp_cip;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_cip(
         nhdplusid             BIGINT
      );

      CREATE UNIQUE INDEX tmp_cip_pk 
      ON tmp_cip(nhdplusid);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_cip_out temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_cip_out')
   THEN
      TRUNCATE TABLE tmp_cip_out;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_cip_out(
          nhdplusid            BIGINT
         ,catchmentstatecode   VARCHAR(2)
         ,xwalk_huc12          VARCHAR(12)
         ,areasqkm             NUMERIC
         ,shape                GEOMETRY
      );

      CREATE UNIQUE INDEX tmp_cip_out_pk 
      ON tmp_cip_out(catchmentstatecode,nhdplusid);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.create_cip_temp_tables() OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.create_cip_temp_tables() TO PUBLIC;

