CREATE OR REPLACE FUNCTION cipsrv_engine.create_delineation_temp_tables()
RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_catchments temp table
   ---------------------------------------------------------------------------- 
   IF cipsrv_engine.temp_table_exists('tmp_catchments')
   THEN
      TRUNCATE TABLE tmp_catchments;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_catchments(
          nhdplusid                   BIGINT
         ,sourcefc                    VARCHAR(40)
         ,gridcode                    INTEGER
         ,areasqkm                    NUMERIC
         ,vpuid                       VARCHAR(16)
         ,hydroseq                    BIGINT
         ,shape                       GEOMETRY
         ,shape_3338                  GEOMETRY
         ,shape_5070                  GEOMETRY
         ,shape_26904                 GEOMETRY
         ,shape_32161                 GEOMETRY
         ,shape_32655                 GEOMETRY
         ,shape_32702                 GEOMETRY
      );

      CREATE UNIQUE INDEX tmp_catchments_pk
      ON tmp_catchments(nhdplusid);
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.create_delineation_temp_tables()
OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.create_delineation_temp_tables()
TO PUBLIC;
