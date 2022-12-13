CREATE OR REPLACE FUNCTION cipsrv_support.clip_by_tribe(
    IN  p_geometry             GEOMETRY
   ,IN  p_known_region         VARCHAR
   ,IN  p_geoid_filter         VARCHAR
   ,IN  p_comptype_filter      VARCHAR
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
   
   rec                 RECORD;
   str_known_region    VARCHAR;
   int_srid            INTEGER;
   int_gridsize        INTEGER;
   sdo_input_geom      GEOMETRY;
   sdo_tribal_geom     GEOMETRY;
   sdo_tribal_geom_r   GEOMETRY;
   sdo_tribal_geom_t   GEOMETRY;
   str_gtype           VARCHAR;
   int_gtype           INTEGER;
   str_comptype_filter VARCHAR;

BEGIN

   out_return_code := 0;
   
   IF p_comptype_filter IN ('R','T')
   THEN
      str_comptype_filter := UPPER(p_comptype_filter);
      
   END IF;      
   
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
   
   sdo_input_geom := ST_Transform(p_geometry,int_srid);
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Fetch the tribal clip geometries
   ----------------------------------------------------------------------------
   IF str_comptype_filter IS NULL 
   OR str_comptype_filter = 'R'
   THEN
      SELECT
      ST_Transform(a.shape,int_srid)
      INTO
      sdo_tribal_geom_r
      FROM
      cipsrv_support.tiger_aiannh a
      WHERE
      a.geoid = p_geoid_filter || 'R';
      
   END IF;
   
   IF str_comptype_filter IS NULL 
   OR str_comptype_filter = 'T'
   THEN
      SELECT
      ST_Transform(a.shape,int_srid)
      INTO
      sdo_tribal_geom_t
      FROM
      cipsrv_support.tiger_aiannh a
      WHERE
      a.geoid = p_geoid_filter || 'T';
      
   END IF;
   
   IF  sdo_tribal_geom_r IS NULL
   AND sdo_tribal_geom_t IS NULL
   THEN
      out_return_code      := -20;
      
      IF str_comptype_filter IS NULL 
      THEN
         out_status_message   := 'No results found for tribal GeoID code <' || p_geoid_filter || '>.';
      
      ELSE
         out_status_message   := 'No results found for tribal GeoID code <' || p_geoid_filter || '> with comptype <' || str_comptype_filter || '>.';
         
      END IF;
      
      out_clipped_geometry := p_geometry;
      RETURN;
      
   ELSIF sdo_tribal_geom_r IS NOT NULL
   AND   sdo_tribal_geom_t IS NULL
   THEN
      sdo_tribal_geom := sdo_tribal_geom_r;
      
   ELSIF sdo_tribal_geom_r IS NULL
   AND   sdo_tribal_geom_t IS NOT NULL
   THEN
      sdo_tribal_geom := sdo_tribal_geom_t;
   
   ELSE
      sdo_tribal_geom := ST_Union(
          sdo_tribal_geom_r
         ,sdo_tribal_geom_t
      );
   
   END IF;
      
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return the intersection
   ----------------------------------------------------------------------------
   out_clipped_geometry := ST_CollectionExtract(
       ST_Intersection(
          sdo_tribal_geom
         ,sdo_input_geom
       )
      ,int_gtype
   );
   
   IF ST_IsEmpty(out_clipped_geometry)
   THEN
      out_clipped_geometry := NULL;
      out_return_code      := 0;
      out_status_message   := 'No results returned from clipping input geometry by tribe.';
      RETURN;
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_support.clip_by_tribe(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_support.clip_by_tribe(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

