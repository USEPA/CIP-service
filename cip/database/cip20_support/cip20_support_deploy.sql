--******************************--
----- functions/generic_common_mbr.sql 

CREATE OR REPLACE FUNCTION cip20_support.generic_common_mbr(
   IN  p_input  VARCHAR
) RETURNS GEOMETRY 
IMMUTABLE
AS
$BODY$ 
DECLARE
   str_input VARCHAR(4000) := UPPER(p_input);
   
BEGIN
   
   IF str_input IN ('5070','CONUS','US','USA',
   'AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA',
   'IA','ID','IL','IN','KS','KY','LA','MA','MD','ME',
   'MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ',
   'NM','NV','NY','OH','OK','OR','PA','RI','SC','SD',
   'TN','TX','UT','VA','VT','WA','WI','WV','WY')
   THEN
      RETURN ST_PolygonFromText('POLYGON((-128.0 20.2,-64.0 20.2,-64.0 52.0,-128.0 52.0,-128.0 20.2))',4326)::geography;
      
   ELSIF str_input IN ('3338','AK','ALASKA')
   THEN
      RETURN ST_MPolyFromText('MULTIPOLYGON(((-180 48,-128 48,-128 90,-180 90,-180 48)),((168 48,180 48,180 90,168 90,168 48)))',4326)::geography;
      
   ELSIF str_input IN ('26904','HI','HAWAII')
   THEN
      RETURN ST_PolygonFromText('POLYGON((-180.0 10.0,-146.0 10.0,-146.0 35.0,-180.0 35.0,-180.0 10.0))',4326)::geography;
      
   ELSIF str_input IN ('32161','PR','VI','PR/VI','PRVI')
   THEN
      RETURN ST_PolygonFromText('POLYGON((-69.0 16.0,-63.0 16.0,-63.0 20.0,-69.0 20.0,-69.0 16.0))',4326)::geography;
   
   ELSIF str_input IN ('32655','GUMP','GUAM','MP','GU')
   THEN
      RETURN ST_PolyFromText('POLYGON((136.0 8.0,154.0 8.0,154.0 25.0,136.0 25.0,136.0 8.0))',4326)::geography;
         
   ELSIF str_input IN ('32702','SAMOA','AS')
   THEN
      RETURN ST_PolyFromText('POLYGON((-178.0 -20.0, -163.0 -20.0, -163.0 -5.0, -178.0 -5.0, -178.0 -20.0))',4326)::geography;
        
   ELSE
      RAISE EXCEPTION 'unknown generic mbr code';
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_support.generic_common_mbr(
   VARCHAR
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_support.generic_common_mbr(
   VARCHAR
) TO PUBLIC;

--******************************--
----- functions/query_generic_common_mbr.sql 

CREATE OR REPLACE FUNCTION cip20_support.query_generic_common_mbr(
   IN  p_input  GEOMETRY
) RETURNS VARCHAR
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_point GEOGRAPHY;
   
BEGIN

   IF p_input IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   sdo_point := ST_Transform(
       ST_PointOnSurface(p_input)
      ,4326
   )::GEOGRAPHY;
   
   IF ST_Intersects(
       sdo_point
      ,cip20_support.generic_common_mbr('CONUS')
   )
   THEN
      RETURN 'CONUS';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cip20_support.generic_common_mbr('HI')
   )
   THEN
      RETURN 'HI';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cip20_support.generic_common_mbr('PRVI')
   )
   THEN
      RETURN 'PRVI';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cip20_support.generic_common_mbr('AK')
   )
   THEN
      RETURN 'AK';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cip20_support.generic_common_mbr('GUMP')
   )
   THEN
      RETURN 'GUMP';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cip20_support.generic_common_mbr('SAMOA')
   )
   THEN
      RETURN 'SAMOA';
      
   END IF;
   
   RETURN NULL;  
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_support.query_generic_common_mbr(
   GEOMETRY
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_support.query_generic_common_mbr(
   GEOMETRY
) TO PUBLIC;

--******************************--
----- functions/determine_grid_srid.sql 

CREATE OR REPLACE FUNCTION cip20_support.determine_grid_srid(
    IN  p_geometry          GEOMETRY
   ,IN  p_known_region      VARCHAR DEFAULT NULL
   ,OUT p_srid              INTEGER
   ,OUT p_grid_size         NUMERIC
   ,OUT p_return_code       INTEGER
   ,OUT p_status_message    VARCHAR
)
IMMUTABLE
AS $BODY$ 
DECLARE
   sdo_results        GEOMETRY;
   str_region         VARCHAR(255) := p_known_region;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  p_geometry IS NULL
   AND str_region IS NULL
   THEN
      RAISE EXCEPTION 'input geometry and known region cannot both be null';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine the region from geometry if known region value not provided
   ----------------------------------------------------------------------------
   IF str_region IS NULL
   THEN
      str_region := cip20_support.query_generic_common_mbr(
         p_input := p_geometry
      );
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Validate region and determine srid
   ----------------------------------------------------------------------------
   IF str_region IS NULL
   THEN
      p_return_code    := -1;
      p_status_message := 'Geometry is outside nhdplus_h coverage.';
      RETURN;

   ELSIF str_region IN ('5070','CONUS','USA',
   'AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA',
   'IA','ID','IL','IN','KS','KY','LA','MA','MD','ME',
   'MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ',
   'NM','NV','NY','OH','OK','OR','PA','RI','SC','SD',
   'TN','TX','UT','VA','VT','WA','WI','WV','WY')
   THEN
      p_srid       := 5070;
      p_grid_size   := 30;
      
   ELSIF str_region IN ('3338','AK')
   THEN  
      p_srid       := 3338;
      p_grid_size   := NULL;
   
   ELSIF str_region IN ('32702','SAMOA','AS')
   THEN
      p_srid       := 32702;
      p_grid_size   := 10;
      
   ELSIF str_region IN ('32655','GUMP','GU','MP')
   THEN
      p_srid       := 32655;
      p_grid_size   := 10;
      
   ELSIF str_region IN ('26904','HI')
   THEN
      p_srid       := 26904;
      p_grid_size   := 10;
      
   ELSIF str_region IN ('32161','PRVI','PR','VI')
   THEN
      p_srid       := 32161;
      p_grid_size   := 10;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return what we got
   ----------------------------------------------------------------------------
   p_return_code := 0;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_support.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_support.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/clip_by_state.sql 

CREATE OR REPLACE FUNCTION cip20_support.clip_by_state(
    IN  p_geometry             GEOMETRY
   ,IN  p_known_region         VARCHAR
   ,IN  p_state_filter         VARCHAR
   ,OUT p_clipped_geometry     GEOMETRY
   ,OUT p_return_code          INTEGER
   ,OUT p_status_message       VARCHAR
) 
STABLE
AS $BODY$
DECLARE
   c_gitrelease    CONSTANT VARCHAR(255) := 'v0.1.0-10-gd4d33b8';
   c_gitcommit     CONSTANT VARCHAR(255) := 'd4d33b8cb5f60d5ae7329f3ff53905588a0034d2';
   c_gitcommitdate CONSTANT VARCHAR(255) := 'Sat Oct 22 09:18:00 2022 -0400';
   c_gitcommitauth CONSTANT VARCHAR(255) := 'Paul Dziemiela';
   
   rec                RECORD;
   str_known_region   VARCHAR;
   int_srid           INTEGER;
   int_gridsize       INTEGER;
   sdo_input_geom     GEOMETRY;
   sdo_state_geom     GEOMETRY;
   str_gtype          VARCHAR;
   int_gtype          INTEGER;

BEGIN

   p_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Determine the proper SRID
   ----------------------------------------------------------------------------
   rec := cip20_support.determine_grid_srid(
       p_geometry        := p_geometry
      ,p_known_region    := p_known_region
   );
   int_srid         := rec.p_srid;
   int_gridsize     := rec.p_grid_size;
   p_return_code    := rec.p_return_code;
   p_status_message := rec.p_status_message;
   
   IF p_return_code != 0
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
      p_return_code      := -20;
      p_status_message   := 'Unknown US state code.';
      p_clipped_geometry := p_geometry;
      RETURN;
      
   END IF;
      
   ----------------------------------------------------------------------------
   -- Step 30
   -- Return the intersection
   ----------------------------------------------------------------------------
   p_clipped_geometry := ST_CollectionExtract(
       ST_Intersection(
          sdo_state_geom
         ,sdo_input_geom
       )
      ,int_gtype
   );
   
   IF ST_IsEmpty(p_clipped_geometry)
   THEN
      p_return_code      := -30;
      p_status_message   := 'No results returned from clipping input event by state.';
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

