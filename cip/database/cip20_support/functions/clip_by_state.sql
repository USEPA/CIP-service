CREATE OR REPLACE FUNCTION cip20_support.clip_by_state(
    IN  p_geometry             GEOMETRY
   ,IN  p_known_region         VARCHAR
   ,IN  p_state_filter         VARCHAR
   ,OUT out_clipped_geometry   GEOMETRY
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
   sdo_input_geom     GEOMETRY;
   sdo_state_geom     GEOMETRY;
   str_gtype          VARCHAR;
   int_gtype          INTEGER;

BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Determine the proper SRID
   ----------------------------------------------------------------------------
   rec := cip20_support.determine_grid_srid(
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
   
   sdo_input_geom := ST_Transform(p_geometry,int_srid);
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Fetch the state clip geometry
   ----------------------------------------------------------------------------
   SELECT
   ST_Transform(a.shape,int_srid)
   INTO
   sdo_state_geom
   FROM
   cip20_support.tiger_fedstatewaters a
   WHERE
   a.stusps = p_state_filter;
   
   IF sdo_state_geom IS NULL
   THEN
      out_return_code      := -20;
      out_status_message   := 'Unknown US state code <' || p_state_filter || '>.';
      out_clipped_geometry := p_geometry;
      RETURN;
      
   END IF;
      
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return the intersection
   ----------------------------------------------------------------------------
   out_clipped_geometry := ST_CollectionExtract(
       ST_Intersection(
          sdo_state_geom
         ,sdo_input_geom
       )
      ,int_gtype
   );
   
   IF ST_IsEmpty(out_clipped_geometry)
   THEN
      out_clipped_geometry := NULL;
      out_return_code      := 0;
      out_status_message   := 'No results returned from clipping input geometry by state.';
      RETURN;
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_support.clip_by_state(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_support.clip_by_state(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

