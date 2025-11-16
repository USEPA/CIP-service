--******************************--
----- functions/generic_common_mbr.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.generic_common_mbr';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;


CREATE OR REPLACE FUNCTION cipsrv_support.generic_common_mbr(
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

ALTER FUNCTION cipsrv_support.generic_common_mbr(
   VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_support.generic_common_mbr(
   VARCHAR
) TO PUBLIC;

--******************************--
----- functions/query_generic_common_mbr.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.query_generic_common_mbr';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_support.query_generic_common_mbr(
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
      ,cipsrv_support.generic_common_mbr('CONUS')
   )
   THEN
      RETURN 'CONUS';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_support.generic_common_mbr('HI')
   )
   THEN
      RETURN 'HI';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_support.generic_common_mbr('PRVI')
   )
   THEN
      RETURN 'PRVI';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_support.generic_common_mbr('AK')
   )
   THEN
      RETURN 'AK';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_support.generic_common_mbr('GUMP')
   )
   THEN
      RETURN 'GUMP';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_support.generic_common_mbr('SAMOA')
   )
   THEN
      RETURN 'SAMOA';
      
   END IF;
   
   RETURN NULL;  
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_support.query_generic_common_mbr(
   GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_support.query_generic_common_mbr(
   GEOMETRY
) TO PUBLIC;

--******************************--
----- functions/determine_grid_srid.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.determine_grid_srid';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;


CREATE OR REPLACE FUNCTION cipsrv_support.determine_grid_srid(
    IN  p_geometry          GEOMETRY
   ,IN  p_known_region      VARCHAR DEFAULT NULL
   ,OUT out_srid            INTEGER
   ,OUT out_grid_size       NUMERIC
   ,OUT out_return_code     INTEGER
   ,OUT out_status_message  VARCHAR
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
      str_region := cipsrv_support.query_generic_common_mbr(
         p_input := p_geometry
      );
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Validate region and determine srid
   ----------------------------------------------------------------------------
   IF str_region IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'Geometry is outside nhdplus_h coverage.';
      RETURN;

   ELSIF str_region IN ('5070','CONUS','USA',
   'AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA',
   'IA','ID','IL','IN','KS','KY','LA','MA','MD','ME',
   'MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ',
   'NM','NV','NY','OH','OK','OR','PA','RI','SC','SD',
   'TN','TX','UT','VA','VT','WA','WI','WV','WY')
   THEN
      out_srid       := 5070;
      out_grid_size   := 30;
      
   ELSIF str_region IN ('3338','AK')
   THEN  
      out_srid       := 3338;
      out_grid_size   := NULL;
   
   ELSIF str_region IN ('32702','SAMOA','AS')
   THEN
      out_srid       := 32702;
      out_grid_size   := 10;
      
   ELSIF str_region IN ('32655','GUMP','GU','MP')
   THEN
      out_srid       := 32655;
      out_grid_size   := 10;
      
   ELSIF str_region IN ('26904','HI')
   THEN
      out_srid       := 26904;
      out_grid_size   := 10;
      
   ELSIF str_region IN ('32161','PRVI','PR','VI')
   THEN
      out_srid       := 32161;
      out_grid_size   := 10;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return what we got
   ----------------------------------------------------------------------------
   out_return_code := 0;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_support.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_support.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/determine_states.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.determine_states';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
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
   c_gitrelease    CONSTANT VARCHAR(255) := 'v1.2.1-171-g24b8dac';
   c_gitcommit     CONSTANT VARCHAR(255) := '24b8dacbcdf1bc4f6fdc321c4ed56e7ef0ef21a5';
   c_gitcommitdate CONSTANT VARCHAR(255) := 'Sat Nov 15 23:12:08 2025 -0500';
   c_gitcommitauth CONSTANT VARCHAR(255) := 'Paul Dziemiela';

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

--******************************--
----- functions/clip_by_state.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.clip_by_state';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_support.clip_by_state(
    IN  p_geometry             GEOMETRY
   ,IN  p_known_region         VARCHAR
   ,IN  p_state_clip           VARCHAR
   ,OUT out_clipped_geometry   GEOMETRY
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
) 
STABLE
AS $BODY$
DECLARE
   c_gitrelease    CONSTANT VARCHAR(255) := 'v1.2.1-171-g24b8dac';
   c_gitcommit     CONSTANT VARCHAR(255) := '24b8dacbcdf1bc4f6fdc321c4ed56e7ef0ef21a5';
   c_gitcommitdate CONSTANT VARCHAR(255) := 'Sat Nov 15 23:12:08 2025 -0500';
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
   cipsrv_support.tiger_fedstatewaters a
   WHERE
   a.stusps = p_state_clip;

   IF sdo_state_geom IS NULL
   THEN
      out_return_code      := -20;
      out_status_message   := 'Unknown US state code <' || p_state_clip || '>.';
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

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.clip_by_state';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;

--******************************--
----- functions/clip_by_tribe.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.clip_by_tribe';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_support.clip_by_tribe(
    IN  p_geometry                  GEOMETRY
   ,IN  p_known_region              VARCHAR
   ,IN  p_tribal_clip_type          VARCHAR
   ,IN  p_tribal_clip               VARCHAR
   ,IN  p_tribal_comptype           VARCHAR
   ,OUT out_clipped_geometry        GEOMETRY
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
) 
STABLE
AS $BODY$
DECLARE
   c_gitrelease    CONSTANT VARCHAR(255) := 'v1.2.1-171-g24b8dac';
   c_gitcommit     CONSTANT VARCHAR(255) := '24b8dacbcdf1bc4f6fdc321c4ed56e7ef0ef21a5';
   c_gitcommitdate CONSTANT VARCHAR(255) := 'Sat Nov 15 23:12:08 2025 -0500';
   c_gitcommitauth CONSTANT VARCHAR(255) := 'Paul Dziemiela';
   
   rec                        RECORD;
   str_known_region           VARCHAR;
   int_srid                   INTEGER;
   int_gridsize               INTEGER;
   sdo_input_geom             GEOMETRY;
   sdo_results                GEOMETRY[];
   boo_all_tribal             BOOLEAN;
   str_gtype                  VARCHAR;
   int_gtype                  INTEGER;
   str_tiger_aiannhns         VARCHAR;
   str_geoid                  VARCHAR;
   int_epa_tribal_id          INTEGER;
   str_bia_tribal_code        VARCHAR;
   str_comptype_clip          VARCHAR;
   str_attains_organizationid VARCHAR;

BEGIN

   out_return_code := 0;

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   str_comptype_clip := p_tribal_comptype;
   
   IF p_tribal_clip_type IS NULL
   OR UPPER(p_tribal_clip_type) IN ('TRIBAL','ALLTRIBES')
   THEN
      boo_all_tribal := TRUE;
   
   ELSIF UPPER(p_tribal_clip_type) IN ('AIANNHNS')
   THEN
      str_tiger_aiannhns := p_tribal_clip;
      
   ELSIF UPPER(p_tribal_clip_type) IN ('GEOID','GEOID_STEM')
   THEN
      str_geoid := p_tribal_clip;
      
      IF LENGTH(str_geoid) = 5
      THEN
         str_geoid          := SUBSTR(str_geoid,1,4);
         str_comptype_clip  := SUBSTR(str_geoid,5,1);
      
      END IF;
      
   ELSIF UPPER(p_tribal_clip_type) IN ('EPA','EPA_ID')
   THEN
      int_epa_tribal_id := p_tribal_clip::INTEGER;
      
   ELSIF UPPER(p_tribal_clip_type) IN ('BIA','BIA_CODE')
   THEN
      str_bia_tribal_code := p_tribal_clip;
      
   ELSIF UPPER(p_tribal_clip_type) IN ('ATTAINS','ATTAINS_ORGANIZATIONID')
   THEN
      str_attains_organizationid := p_tribal_clip;
      
   ELSE
      out_return_code    := -1;
      out_status_message := 'tribal clip must be provided.';
      RETURN;
   
   END IF;
   
   IF UPPER(str_comptype_clip) IN ('R','T')
   THEN
      str_comptype_clip := UPPER(str_comptype_clip);
   
   ELSE
      str_comptype_clip := NULL;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
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
   
   sdo_input_geom := ST_SnapToGrid(ST_Transform(p_geometry,int_srid),0.001);
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Fetch the tribal clip geometries
   ----------------------------------------------------------------------------
   IF int_srid = 5070
   THEN
      SELECT 
      ARRAY(
         SELECT
         ST_Intersection(
             ST_SnapToGrid(a.shape,0.001)
            ,sdo_input_geom
          )
         FROM
         cipsrv_support.tiger_aiannha_5070 a
         LEFT JOIN
         cipsrv_support.tribal_crosswalk b
         ON
         SUBSTR(a.geoid,1,4) = b.aiannhns_stem 
         WHERE
         ST_Intersects(
            a.shape
           ,sdo_input_geom
         )
         AND (
               str_comptype_clip IS NULL
            OR ( str_comptype_clip = 'R' AND b.has_reservation_lands ) 
            OR ( str_comptype_clip = 'T' AND b.has_trust_lands )
         )
         AND (
               boo_all_tribal IS TRUE
            OR ( str_tiger_aiannhns IS NOT NULL         AND a.aiannhns               = str_tiger_aiannhns )
            OR ( str_geoid IS NOT NULL                  AND b.aiannhns_stem          = str_geoid )
            OR ( int_epa_tribal_id IS NOT NULL          AND b.epa_tribal_id          = int_epa_tribal_id )
            OR ( str_bia_tribal_code IS NOT NULL        AND b.bia_tribal_code        = str_bia_tribal_code )
            OR ( str_attains_organizationid IS NOT NULL AND b.attains_organizationid = str_attains_organizationid )
         )
      )
      INTO sdo_results;
   
   ELSIF int_srid = 3338
   THEN
      SELECT 
      ARRAY(
         SELECT
         ST_Intersection(
             ST_SnapToGrid(a.shape,0.001)
            ,sdo_input_geom
          )
         FROM
         cipsrv_support.tiger_aiannha_3338 a
         LEFT JOIN
         cipsrv_support.tribal_crosswalk b
         ON
         SUBSTR(a.geoid,1,4) = b.aiannha_geoid_stem
         WHERE
         ST_Intersects(
            a.shape
           ,sdo_input_geom
         )
         AND (
               str_comptype_clip IS NULL
            OR ( str_comptype_clip = 'R' AND b.has_reservation_lands ) 
            OR ( str_comptype_clip = 'T' AND b.has_trust_lands )
         )
         AND (
               boo_all_tribal IS TRUE
            OR ( str_tiger_aiannhns IS NOT NULL         AND b.aiannha_aiannhns   = str_tiger_aiannhns )
            OR ( str_geoid IS NOT NULL                  AND b.aiannha_geoid_stem = str_geoid )
            OR ( int_epa_tribal_id IS NOT NULL          AND b.epa_tribal_id = int_epa_tribal_id )
            OR ( str_bia_tribal_code IS NOT NULL        AND b.bia_tribal_code = str_bia_tribal_code )
            OR ( str_attains_organizationid IS NOT NULL AND b.attains_organizationid = str_attains_organizationid )
         )
      )
      INTO sdo_results;
   
   ELSIF int_srid = 26904
   THEN
      SELECT 
      ARRAY(
         SELECT
         ST_Intersection(
             ST_SnapToGrid(a.shape,0.001)
            ,sdo_input_geom
          )
         FROM
         cipsrv_support.tiger_aiannha_26904 a
         LEFT JOIN
         cipsrv_support.tribal_crosswalk b
         ON
         SUBSTR(a.geoid,1,4) = b.aiannha_geoid_stem
         WHERE
         ST_Intersects(
            a.shape
           ,sdo_input_geom
         )
         AND (
               str_comptype_clip IS NULL
            OR ( str_comptype_clip = 'R' AND b.has_reservation_lands ) 
            OR ( str_comptype_clip = 'T' AND b.has_trust_lands )
         )
         AND (
               boo_all_tribal IS TRUE
            OR ( str_tiger_aiannhns IS NOT NULL         AND b.aiannha_aiannhns   = str_tiger_aiannhns )
            OR ( str_geoid IS NOT NULL                  AND b.aiannha_geoid_stem = str_geoid )
            OR ( int_epa_tribal_id IS NOT NULL          AND b.epa_tribal_id      = int_epa_tribal_id )
            OR ( str_bia_tribal_code IS NOT NULL        AND b.bia_tribal_code    = str_bia_tribal_code )
            OR ( str_attains_organizationid IS NOT NULL AND b.attains_organizationid = str_attains_organizationid )
         )
      )
      INTO sdo_results;
   
   ELSIF int_srid = 32161
   THEN
      SELECT 
      ARRAY(
         SELECT
         ST_Intersection(
             ST_SnapToGrid(a.shape,0.001)
            ,sdo_input_geom
          )
         FROM
         cipsrv_support.tiger_aiannha_32161 a
         LEFT JOIN
         cipsrv_support.tribal_crosswalk b
         ON
         SUBSTR(a.geoid,1,4) = b.aiannha_geoid_stem
         WHERE
         ST_Intersects(
            a.shape
           ,sdo_input_geom
         )
         AND (
               str_comptype_clip IS NULL
            OR ( str_comptype_clip = 'R' AND b.has_reservation_lands ) 
            OR ( str_comptype_clip = 'T' AND b.has_trust_lands )
         )
         AND (
               boo_all_tribal IS TRUE
            OR ( str_tiger_aiannhns IS NOT NULL         AND b.aiannha_aiannhns   = str_tiger_aiannhns )
            OR ( str_geoid IS NOT NULL                  AND b.aiannha_geoid_stem = str_geoid )
            OR ( int_epa_tribal_id IS NOT NULL          AND b.epa_tribal_id = int_epa_tribal_id )
            OR ( str_bia_tribal_code IS NOT NULL        AND b.bia_tribal_code = str_bia_tribal_code )
            OR ( str_attains_organizationid IS NOT NULL AND b.attains_organizationid = str_attains_organizationid )
         )
      )
      INTO sdo_results;
   
   ELSIF int_srid = 32655
   THEN
      SELECT 
      ARRAY(
         SELECT
         ST_Intersection(
             ST_SnapToGrid(a.shape,0.001)
            ,sdo_input_geom
          )
         FROM
         cipsrv_support.tiger_aiannha_32655 a
         LEFT JOIN
         cipsrv_support.tribal_crosswalk b
         ON
         SUBSTR(a.geoid,1,4) = b.aiannha_geoid_stem
         WHERE
         ST_Intersects(
            a.shape
           ,sdo_input_geom
         )
         AND (
               str_comptype_clip IS NULL
            OR ( str_comptype_clip = 'R' AND b.has_reservation_lands ) 
            OR ( str_comptype_clip = 'T' AND b.has_trust_lands )
         )
         AND (
               boo_all_tribal IS TRUE
            OR ( str_tiger_aiannhns IS NOT NULL         AND b.aiannha_aiannhns   = str_tiger_aiannhns )
            OR ( str_geoid IS NOT NULL                  AND b.aiannha_geoid_stem = str_geoid )
            OR ( int_epa_tribal_id IS NOT NULL          AND b.epa_tribal_id = int_epa_tribal_id )
            OR ( str_bia_tribal_code IS NOT NULL        AND b.bia_tribal_code = str_bia_tribal_code )
            OR ( str_attains_organizationid IS NOT NULL AND b.attains_organizationid = str_attains_organizationid )
         )
      )
      INTO sdo_results;
      
   ELSIF int_srid = 32702
   THEN
      SELECT 
      ARRAY(
         SELECT
         ST_Intersection(
             ST_SnapToGrid(a.shape,0.001)
            ,sdo_input_geom
          )
         FROM
         cipsrv_support.tiger_aiannha_32702 a
         LEFT JOIN
         cipsrv_support.tribal_crosswalk b
         ON
         SUBSTR(a.geoid,1,4) = b.aiannha_geoid_stem
         WHERE
         ST_Intersects(
            a.shape
           ,sdo_input_geom
         )
         AND (
               str_comptype_clip IS NULL
            OR ( str_comptype_clip = 'R' AND b.has_reservation_lands ) 
            OR ( str_comptype_clip = 'T' AND b.has_trust_lands )
         )
         AND (
               boo_all_tribal IS TRUE
            OR ( str_tiger_aiannhns IS NOT NULL         AND b.aiannha_aiannhns   = str_tiger_aiannhns )
            OR ( str_geoid IS NOT NULL                  AND b.aiannha_geoid_stem = str_geoid )
            OR ( int_epa_tribal_id IS NOT NULL          AND b.epa_tribal_id = int_epa_tribal_id )
            OR ( str_bia_tribal_code IS NOT NULL        AND b.bia_tribal_code = str_bia_tribal_code )
            OR ( str_attains_organizationid IS NOT NULL AND b.attains_organizationid = str_attains_organizationid )
         )
      )
      INTO sdo_results;
      
   ELSE
      RAISE EXCEPTION 'err';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Aggregate results array into single geometry
   ----------------------------------------------------------------------------
   IF sdo_results IS NULL
   OR array_length(sdo_results,1) IS NULL
   OR array_length(sdo_results,1) = 0
   THEN
      out_return_code      := -20;
      out_status_message   := 'No results found for tribal code ' || p_tribal_clip || '.';
      RETURN;
  
   ELSE
      out_clipped_geometry := ST_CollectionExtract(
          ST_SnapToGrid(
             ST_Union(sdo_results)
            ,0.001
          )
         ,int_gtype
      );
      
   END IF;
   
   IF ST_IsEmpty(out_clipped_geometry)
   THEN
      out_clipped_geometry := NULL;
      out_return_code      := 0;
      out_status_message   := 'No results returned from clipping input geometry by ' || p_tribal_clip || '.';
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
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_support.clip_by_tribe(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/geometry_clip.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.geometry_clip';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_support.geometry_clip(
    IN  p_geometry             GEOMETRY
   ,IN  p_clippers             VARCHAR[]
   ,IN  p_known_region         VARCHAR
   ,OUT out_clipped_geometry   GEOMETRY
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
) 
STABLE
AS $BODY$
DECLARE
   c_gitrelease    CONSTANT VARCHAR(255) := 'v1.2.1-171-g24b8dac';
   c_gitcommit     CONSTANT VARCHAR(255) := '24b8dacbcdf1bc4f6fdc321c4ed56e7ef0ef21a5';
   c_gitcommitdate CONSTANT VARCHAR(255) := 'Sat Nov 15 23:12:08 2025 -0500';
   c_gitcommitauth CONSTANT VARCHAR(255) := 'Paul Dziemiela';
   
   rec                RECORD;
   str_known_region   VARCHAR;
   int_srid           INTEGER;
   int_gridsize       INTEGER;
   sdo_input_geom     GEOMETRY;
   sdo_results        GEOMETRY;
   ary_results        GEOMETRY[];
   str_gtype          VARCHAR;
   int_gtype          INTEGER;
   ary_clip           VARCHAR[];
   str_token1         VARCHAR;
   str_token2         VARCHAR;
   str_token3         VARCHAR;
   ary_states         VARCHAR[];
   str_state          VARCHAR;
   str_comptype       VARCHAR;

BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   ary_states := ARRAY[
       'AL','AK','AS','AZ','AR','CA','CO','CT','DE','DC','FL','GA','GU','HI'
      ,'ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MO','MP'
      ,'MS','MT','NE','NV','NH','NJ','NM','NY','NC','ND','MP','OH','OK','OR'
      ,'PA','PR','RI','SC','SD','TN','TX','UT','VT','VI','VA','WA','WV','WI'
      ,'WY'
   ];
   
   ----------------------------------------------------------------------------
   -- Step 20
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
   -- Step 30
   -- Parse the clippers
   ----------------------------------------------------------------------------
   FOR i IN 1 .. array_length(p_clippers,1)
   LOOP
      ary_clip := string_to_array(UPPER(p_clippers[i]),':');
      
      IF array_length(ary_clip,1) > 0
      THEN
         str_token1 := ary_clip[1];
      
      END IF;
      
      IF array_length(ary_clip,1) > 1
      THEN
         str_token2 := ary_clip[2];
      
      END IF;
      
      IF array_length(ary_clip,1) > 2
      THEN
         str_token3 := ary_clip[3];
      
      END IF;
      
      -- Check for all tribal clip
      IF str_token1 IN ('TRIBAL','ALLTRIBES')
      THEN
         IF str_token2 IN ('R','T')
         THEN
            str_comptype := str_token2;
            
         END IF;
         
         rec := cipsrv_support.clip_by_tribe(
             p_geometry           := sdo_input_geom
            ,p_known_region       := str_known_region
            ,p_tribal_clip_type   := 'ALLTRIBES'
            ,p_tribal_clip        := NULL
            ,p_tribal_comptype    := str_comptype
         );
         sdo_results        := rec.out_clipped_geometry;
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         
         IF out_return_code != 0
         THEN
            RETURN;
            
         END IF;
         
         IF  sdo_results IS NOT NULL
         AND NOT ST_IsEmpty(sdo_results)
         THEN
            ary_results := array_append(ary_results,sdo_results);
            
         END IF;
        
      -- check for state code as naked or USPS:WI form        
      ELSIF str_token1 = ANY(ary_states)
      OR str_token1 = 'USPS' AND str_token2 = ANY(ary_states)
      THEN
         IF str_token1 = 'USPS'
         THEN
            str_state := str_token2;
            
         ELSE
            str_state := str_token1;
            
         END IF;
         
         rec := cipsrv_support.clip_by_state(
             p_geometry           := sdo_input_geom
            ,p_known_region       := str_known_region
            ,p_state_clip         := str_state
         );
         sdo_results        := rec.out_clipped_geometry;
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         
         IF out_return_code != 0
         THEN
            RETURN;
            
         END IF;
         
         IF  sdo_results IS NOT NULL
         AND NOT ST_IsEmpty(sdo_results)
         THEN
            ary_results := array_append(ary_results,sdo_results);
            
         END IF;
      
      ELSIF str_token1 IN ('AIANNHNS','GEOID','GEOID_STEM','EPA','EPA_ID','BIA','BIA_CODE','ATTAINS','ATTAINS_ORGANIZATIONID')
      THEN
         IF str_token3 IN ('R','T')
         THEN
            str_comptype := str_token3;
            
         END IF;
         
         rec := cipsrv_support.clip_by_tribe(
             p_geometry           := sdo_input_geom
            ,p_known_region       := str_known_region
            ,p_tribal_clip_type   := str_token1
            ,p_tribal_clip        := str_token2
            ,p_tribal_comptype    := str_comptype
         );
         sdo_results        := rec.out_clipped_geometry;
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         
         IF out_return_code != 0
         THEN
            RETURN;
            
         END IF;
         
         IF  sdo_results IS NOT NULL
         AND NOT ST_IsEmpty(sdo_results)
         THEN
            ary_results := array_append(ary_results,sdo_results);
            
         END IF;
         
      END IF;
   
   END LOOP;
      
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return the intersection
   ----------------------------------------------------------------------------
   IF ary_results IS NULL 
   OR array_length(ary_results,1) IS NULL
   OR array_length(ary_results,1) = 0
   THEN
      out_clipped_geometry := NULL;
      out_return_code      := 0;
      out_status_message   := 'No results returned from clipping input geometry.';
      RETURN;
      
   END IF;
   
   FOR i IN 1 .. array_length(ary_results,1)
   LOOP
      IF out_clipped_geometry IS NULL
      THEN
         out_clipped_geometry := ary_results[i];
         
      ELSE
         out_clipped_geometry := ST_CollectionExtract(
             ST_Intersection(
                out_clipped_geometry
               ,ary_results[i]
             )
            ,int_gtype
         );
         
      END IF;
      
   END LOOP;
   
   IF out_clipped_geometry IS NULL OR ST_IsEmpty(out_clipped_geometry)
   THEN
      out_clipped_geometry := NULL;
      out_return_code      := 0;
      out_status_message   := 'No results returned from clipping input geometry.';
      RETURN;
      
   END IF;  
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_support.geometry_clip(
    GEOMETRY
   ,VARCHAR[]
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_support.geometry_clip(
    GEOMETRY
   ,VARCHAR[]
   ,VARCHAR
) TO PUBLIC;

