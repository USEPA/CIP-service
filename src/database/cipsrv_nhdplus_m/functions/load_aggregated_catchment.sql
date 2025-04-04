DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.load_aggregated_catchment';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.load_aggregated_catchment(
    IN  p_grid_srid               INTEGER
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   num_areasqkm    NUMERIC;
   sdo_output      GEOMETRY;
   
BEGIN

   IF p_grid_srid = 5070
   THEN
      SELECT 
      ST_Union(shape_5070)
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_5070
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;
   
   ELSIF p_grid_srid = 3338
   THEN
      SELECT 
      ST_Union(shape_3338)
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_3338
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;
   
   ELSIF p_grid_srid = 26904
   THEN
      SELECT 
      ST_Union(a.shape_26904)
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_26904
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;
      
   ELSIF p_grid_srid = 32161
   THEN
      SELECT 
      ST_Union(a.shape_32161)
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_32161
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;
      
   ELSIF p_grid_srid = 32655
   THEN
      SELECT 
      ST_Union(a.shape_32655)
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_32655
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;

   ELSIF p_grid_srid = 32702
   THEN
      SELECT 
      ST_Union(a.shape_32702)
      INTO sdo_output
      FROM (
         SELECT
         aa.shape_32702
         FROM
         tmp_catchments aa
         ORDER BY 
         aa.hydroseq
      ) a;
      
   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;
   
   IF sdo_output IS NULL
   THEN
      out_return_code    := -101;
      out_status_message := 'Catchment aggregation failed';
      RETURN;
      
   END IF;
   
   num_areasqkm := ST_Area(sdo_output) * 0.000001;
   
   sdo_output := ST_Transform(sdo_output,4269);
   
   INSERT INTO tmp_catchments(
       nhdplusid
      ,sourcefc
      ,areasqkm
      ,shape
   ) VALUES (
       -9999999
      ,'AGGR'
      ,num_areasqkm
      ,sdo_output
   );
   
   out_return_code := 0;
   
   RETURN;   
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.load_aggregated_catchment(
    INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.load_aggregated_catchment(
    INTEGER
) TO PUBLIC;
