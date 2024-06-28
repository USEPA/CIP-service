DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.determine_states';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_support.determine_states(
    IN  p_geometry             GEOMETRY
   ,IN  p_known_region         VARCHAR
   ,IN  p_linear_threshold     NUMERIC DEFAULT 0.01
   ,IN  p_area_threshold       NUMERIC DEFAULT 0.02
   ,OUT out_statecodes         VARCHAR[]
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$
DECLARE
   c_gitrelease    CONSTANT VARCHAR(255) := 'NULL';
   c_gitcommit     CONSTANT VARCHAR(255) := 'NULL';
   c_gitcommitdate CONSTANT VARCHAR(255) := 'NULL';
   c_gitcommitauth CONSTANT VARCHAR(255) := 'NULL';

   rec                RECORD;
   str_known_region   VARCHAR;
   int_srid           INTEGER;
   int_gridsize       INTEGER;
   geom_input         GEOMETRY;
   str_gtype          VARCHAR;
   int_gtype          INTEGER;

BEGIN

   out_return_code := 0;

   ----------------------------------------------------------------------------
   -- Step 10
   -- Determine the proper SRID
   ----------------------------------------------------------------------------
   rec := cipsrv_support.determine_grid_srid(
       p_geometry        := p_geometry
      ,p_known_region    := p_known_region
   );
   int_srid           := rec.out_srid;
   int_gridsize       := rec.out_grid_size;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;

   IF out_return_code != 0
   THEN
      RETURN;

   END IF;

   str_known_region := int_srid::VARCHAR;

   str_gtype := ST_GeometryType(p_geometry);

   IF str_gtype IN ('ST_Point','ST_MultiPoint')
   THEN
      int_gtype := 1;

   ELSIF str_gtype IN ('ST_LineString','ST_MultiLineString')
   THEN
      int_gtype := 2;

   ELSIF str_gtype IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      int_gtype := 3;

   END IF;
   
   IF str_gtype NOT IN ('ST_Point')
   THEN
      RAISE EXCEPTION 'unimplemented';
   
   END IF;

   geom_input := ST_Transform(p_geometry,int_srid);

   ----------------------------------------------------------------------------
   -- Step 20
   -- Search for matching states
   ----------------------------------------------------------------------------
   IF int_srid = 3338
   THEN
      out_statecodes := ARRAY(
         SELECT
         a.stusps
         FROM
         cipsrv_support.tiger_fedstatewaters_3338 a
         WHERE
         ST_Intersects(
             a.shape
            ,geom_input
         )
      );

   ELSIF int_srid = 5070
   THEN
      out_statecodes := ARRAY(
         SELECT
         a.stusps
         FROM
         cipsrv_support.tiger_fedstatewaters_5070 a
         WHERE
         ST_Intersects(
             a.shape
            ,geom_input
         )
      );

   ELSIF int_srid = 26904
   THEN
      out_statecodes := ARRAY(
         SELECT
         a.stusps
         FROM
         cipsrv_support.tiger_fedstatewaters_26904 a
         WHERE
         ST_Intersects(
             a.shape
            ,geom_input
         )
      );

   ELSIF int_srid = 32161
   THEN
      out_statecodes := ARRAY(
         SELECT
         a.stusps
         FROM
         cipsrv_support.tiger_fedstatewaters_32161 a
         WHERE
         ST_Intersects(
             a.shape
            ,geom_input
         )
      );

   ELSIF int_srid = 32655
   THEN
      out_statecodes := ARRAY(
         SELECT
         a.stusps
         FROM
         cipsrv_support.tiger_fedstatewaters_32655 a
         WHERE
         ST_Intersects(
             a.shape
            ,geom_input
         )
      );

   ELSIF int_srid = 32702
   THEN
      out_statecodes := ARRAY(
         SELECT
         a.stusps
         FROM
         cipsrv_support.tiger_fedstatewaters_32702 a
         WHERE
         ST_Intersects(
             a.shape
            ,geom_input
         )
      );

   ELSE
      RAISE EXCEPTION 'err %',int_srid;

   END IF;

   out_return_code := 0;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_support.determine_states(
    GEOMETRY
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_support.determine_states(
    GEOMETRY
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

