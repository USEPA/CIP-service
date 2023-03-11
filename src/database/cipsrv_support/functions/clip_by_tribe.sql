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
   c_gitrelease    CONSTANT VARCHAR(255) := 'NULL';
   c_gitcommit     CONSTANT VARCHAR(255) := 'NULL';
   c_gitcommitdate CONSTANT VARCHAR(255) := 'NULL';
   c_gitcommitauth CONSTANT VARCHAR(255) := 'NULL';
   
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
      str_comptype_clip := UPPER(p_comptype_clip);
   
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

