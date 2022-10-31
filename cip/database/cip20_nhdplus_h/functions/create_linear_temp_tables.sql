CREATE OR REPLACE FUNCTION cip20_nhdplus_h.create_linear_temp_tables()
RETURNS INT4
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_linear temp table
   ----------------------------------------------------------------------------
   IF cip20_nhdplus_h.temp_table_exists('tmp_linear')
   THEN
      TRUNCATE TABLE tmp_linear;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_linear(
          nhdplusid               BIGINT
         ,areasqkm                NUMERIC
         ,overlapmeasure          NUMERIC
         ,eventpercentage         NUMERIC
         ,nhdpercentage           NUMERIC
         ,hydroseq                BIGINT
         ,levelpathi              BIGINT
         ,fromnode                BIGINT
         ,tonode                  BIGINT
         ,connector_fromnode      BIGINT
         ,connector_tonode        BIGINT
      );

      CREATE UNIQUE INDEX tmp_linear_pk 
      ON tmp_linear(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_linear_pk2
      ON tmp_linear(hydroseq);
      
      CREATE INDEX tmp_linear_01i
      ON tmp_linear(levelpathi);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_linear_dd temp table
   ----------------------------------------------------------------------------
   IF cip20_nhdplus_h.temp_table_exists('tmp_linear_levelpathi')
   THEN
      TRUNCATE TABLE tmp_linear_levelpathi;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_linear_levelpathi(
          levelpathi              BIGINT
         ,max_hydroseq            BIGINT
         ,min_hydroseq            BIGINT
         ,totaleventpercentage    NUMERIC
         ,totaloverlapmeasure     NUMERIC
         ,levelpathilengthkm      NUMERIC
         ,fromnode                BIGINT
         ,tonode                  BIGINT
         ,connector_fromnode      BIGINT
         ,connector_tonode        BIGINT
      );

      CREATE UNIQUE INDEX tmp_linear_levelpathi_pk 
      ON tmp_linear_levelpathi(levelpathi);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cip20_nhdplus_h.create_linear_temp_tables() OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_nhdplus_h.create_linear_temp_tables() TO PUBLIC;

