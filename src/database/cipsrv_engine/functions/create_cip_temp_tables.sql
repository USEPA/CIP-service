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
          permid_joinkey       UUID
         ,catchmentstatecodes  VARCHAR[]
         ,nhdplusid            BIGINT    NOT NULL
         ,overlap_measure      NUMERIC
      );

      CREATE UNIQUE INDEX tmp_cip_pk
      ON tmp_cip(
          permid_joinkey
         ,catchmentstatecodes
         ,nhdplusid
      ) NULLS NOT DISTINCT;
      
      CREATE INDEX tmp_cip_i01
      ON tmp_cip(
         permid_joinkey
      );
      
      CREATE INDEX tmp_cip_i02
      ON tmp_cip(
         nhdplusid
      );

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
          nhdplusid            BIGINT      NOT NULL
         ,catchmentstatecode   VARCHAR(2)
         ,xwalk_huc12          VARCHAR(12)
         ,areasqkm             NUMERIC
         ,istribal             VARCHAR(1)  NOT NULL
         ,istribal_areasqkm    NUMERIC
         ,isnavigable          VARCHAR(1)  NOT NULL
         ,shape                GEOMETRY
      );

      CREATE UNIQUE INDEX tmp_cip_out_pk 
      ON tmp_cip_out(catchmentstatecode,nhdplusid);
      
      CREATE INDEX tmp_cip_out_01i
      ON tmp_cip_out(nhdplusid);

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

