CREATE OR REPLACE FUNCTION cipsrv_engine.create_cip_batch_temp_tables()
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
          permid_joinkey       UUID
         ,nhdplusid            BIGINT
      );

      CREATE UNIQUE INDEX tmp_cip_pk 
      ON tmp_cip(
          permid_joinkey
         ,nhdplusid
      );

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_permid temp table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_permid')
   THEN
      TRUNCATE TABLE tmp_permid;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_permid(
          permid_joinkey       UUID
         ,cip_method           VARCHAR(255)
         ,cip_parms            VARCHAR(255) 
      );

      CREATE UNIQUE INDEX tmp_permid_pk 
      ON tmp_permid(
          permid_joinkey
      );

   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.create_cip_batch_temp_tables() OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.create_cip_batch_temp_tables() TO PUBLIC;

