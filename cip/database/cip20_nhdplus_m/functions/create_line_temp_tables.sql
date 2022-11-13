CREATE OR REPLACE FUNCTION cip20_nhdplus_m.create_line_temp_tables()
RETURNS INT4
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_line temp table
   ----------------------------------------------------------------------------
   IF cip20_nhdplus_m.temp_table_exists('tmp_line')
   THEN
      TRUNCATE TABLE tmp_line;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_line(
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

      CREATE UNIQUE INDEX tmp_line_pk 
      ON tmp_line(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_line_pk2
      ON tmp_line(hydroseq);
      
      CREATE INDEX tmp_line_01i
      ON tmp_line(levelpathi);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_line_dd temp table
   ----------------------------------------------------------------------------
   IF cip20_nhdplus_m.temp_table_exists('tmp_line_levelpathi')
   THEN
      TRUNCATE TABLE tmp_line_levelpathi;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_line_levelpathi(
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

      CREATE UNIQUE INDEX tmp_line_levelpathi_pk 
      ON tmp_line_levelpathi(levelpathi);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cip20_nhdplus_m.create_line_temp_tables() OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_nhdplus_m.create_line_temp_tables() TO PUBLIC;

