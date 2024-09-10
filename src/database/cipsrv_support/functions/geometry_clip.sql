DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_support.geometry_clip';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s()',a);END IF;
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
   c_gitrelease    CONSTANT VARCHAR(255) := 'NULL';
   c_gitcommit     CONSTANT VARCHAR(255) := 'NULL';
   c_gitcommitdate CONSTANT VARCHAR(255) := 'NULL';
   c_gitcommitauth CONSTANT VARCHAR(255) := 'NULL';
   
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

