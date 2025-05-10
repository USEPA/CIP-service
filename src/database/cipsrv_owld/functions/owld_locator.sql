DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.owld_locator';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_owld.owld_locator(
    IN  p_nhdplus_version               VARCHAR
   ,IN  p_source_featureid              VARCHAR
   ,IN  p_source_featureid2             VARCHAR
   ,IN  p_source_originator             VARCHAR
   ,IN  p_source_series                 VARCHAR
   ,IN  p_start_date                    DATE
   ,IN  p_end_date                      DATE
   ,IN  p_linked_data_program           VARCHAR
   ,IN  p_source_joinkey                VARCHAR
   ,IN  p_permid_joinkey                VARCHAR
   ,IN  p_cip_joinkey                   VARCHAR
   ,IN  p_search_direction              VARCHAR
   ,IN  p_reference_catchment_nhdplusid BIGINT
   ,IN  p_reference_reachcode           VARCHAR
   ,IN  p_reference_flowline_nhdplusid  BIGINT
   ,IN  p_reference_hydroseq            BIGINT
   ,IN  p_reference_measure             NUMERIC
   ,IN  p_reference_point               GEOMETRY
   ,IN  p_search_precision              VARCHAR
   ,IN  p_push_rad_for_permid           BOOLEAN
   ,IN  p_return_shape                  BOOLEAN DEFAULT FALSE
   ,IN  p_known_region                  VARCHAR DEFAULT NULL
   ,OUT out_catchment_nhdplusid         BIGINT
   ,OUT out_catchmentstatecode          VARCHAR
   ,OUT out_permanent_identifier        VARCHAR
   ,OUT out_reachcode                   VARCHAR
   ,OUT out_flowline_nhdplusid          BIGINT
   ,OUT out_hydroseq                    BIGINT
   ,OUT out_measure                     NUMERIC
   ,OUT out_shape                       GEOMETRY
   ,OUT out_source_featureid            VARCHAR
   ,OUT out_source_featureid2           VARCHAR
   ,OUT out_source_originator           VARCHAR
   ,OUT out_source_series               VARCHAR
   ,OUT out_source_subdivision          VARCHAR
   ,OUT out_start_date                  DATE
   ,OUT out_end_date                    DATE
   ,OUT out_source_joinkey              VARCHAR
   ,OUT out_permid_joinkey              VARCHAR
   ,OUT out_nhdplus_version             VARCHAR
   ,OUT out_cip_joinkey                 VARCHAR
   ,OUT out_cat_joinkey                 VARCHAR
   ,OUT out_linked_data_program         VARCHAR
   ,OUT out_return_code                 NUMERIC
   ,OUT out_status_message              VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                       RECORD;
   str_query_type            VARCHAR;
   ary_programs_abbrvs       VARCHAR[];
   ary_programs_eventtypes   INTEGER[];
   ary_programs_resolutions  VARCHAR[][];
   str_program_abbrv         VARCHAR;
   int_program_eventtype     INTEGER;
   int_count                 INTEGER;
   ary_searchable_programs   cipsrv_owld.owld_locator_query[];
   ary_targeted_programs     cipsrv_owld.owld_locator_query[];
   ary_targeted_programs2    cipsrv_owld.owld_locator_query[];
   str_nhdplus_version       VARCHAR;
   str_search_precision      VARCHAR;
   geom_shape                GEOMETRY;

BEGIN

   out_return_code := 0;

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   IF p_search_precision IS NULL
   THEN      
      str_search_precision := 'CATCHMENT';
   
   ELSE
      IF UPPER(p_search_precision) IN ('CATCHMENT')
      THEN
         str_search_precision := 'CATCHMENT';
      
      ELSIF UPPER(p_search_precision) IN ('REACH','REACHED')
      THEN
         str_search_precision := 'REACH';
      
      ELSE
         out_return_code := -10;
         out_status_message := 'Unknown precision parameter: ' || p_search_precision || '.';
         RETURN;
      
      END IF;
      
   END IF;
   
   IF p_nhdplus_version IS NULL
   OR p_nhdplus_version IN ('MR','nhdplus_m')
   THEN
      str_nhdplus_version := 'MR';
      
   ELSIF p_nhdplus_version IN ('HR','nhdplus_h')
   THEN
      str_nhdplus_version := 'HR';
      
   ELSE
      out_return_code := -10;
      out_status_message := 'Unknown resolution parameter: ' || p_nhdplus_version || '.';
      RETURN;
   
   END IF;

   --------------------------------------------------------------------------
   -- Step 20
   -- Determine basic query types to drive logic
   --------------------------------------------------------------------------
   IF p_cip_joinkey IS NOT NULL
   THEN
      str_query_type := 'cip_joinkey';
      
   ELSIF p_permid_joinkey IS NOT NULL
   THEN
      str_query_type := 'permid_joinkey';
      
   ELSIF p_source_joinkey IS NOT NULL
   THEN
      str_query_type := 'source_joinkey';
      
   ELSIF p_source_featureid IS NOT NULL
   THEN
      IF str_search_precision = 'CATCHMENT'
      THEN
         str_query_type := 'source_featureid CATCHMENT';
         
      ELSIF str_search_precision = 'REACH'
      THEN
         str_query_type := 'source_featureid REACH';
         
      END IF;
      
   ELSE
      out_return_code := -20;
      out_status_message := 'Unable to determine query parameters for OWLD locator search. '
                         || 'Provide either a SFID-combination or permid/source/cat joinkeys.';
      RETURN;

   END IF;
   
   IF str_search_precision = 'REACH'
   AND str_query_type = 'cip_joinkey'
   THEN
      out_return_code := -10;
      out_status_message := 'CIP JoinKey request invalid combined with REACH search precision.';
      RETURN;
   
   END IF;

   --------------------------------------------------------------------------
   -- Step 30
   -- Check for lack of programs to query on system
   --------------------------------------------------------------------------
   ary_searchable_programs := cipsrv_owld.owld_programs_ary();
   
   IF ARRAY_LENGTH(ary_searchable_programs,1) = 0
   THEN
      out_return_code := -30;
      out_status_message := 'No OWLD programs are currently loaded in this CIPSRV system.';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Try to filter down by provided program id if provided
   --------------------------------------------------------------------------
   ary_targeted_programs := NULL;
   IF p_linked_data_program IS NOT NULL
   THEN
      FOR i IN 1 .. ARRAY_LENGTH(ary_searchable_programs,1)
      LOOP
         IF p_linked_data_program = ary_searchable_programs[i].program_id
         OR p_linked_data_program = ary_searchable_programs[i].program_eventtype::VARCHAR
         THEN
            ary_targeted_programs[1] := ary_searchable_programs[i];
   
         END IF;
         
      END LOOP;
      
      IF ary_targeted_programs IS NULL
      OR ARRAY_LENGTH(ary_targeted_programs,1) = 0
      THEN
         out_return_code := -40;
         out_status_message := 'Requested program ' || p_linked_data_program || ' not found on this system.';
         RETURN;
         
      END IF;
      
   ELSE
      ary_targeted_programs := ary_searchable_programs;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Try to filter down by resolution if provided
   --------------------------------------------------------------------------
   ary_targeted_programs2 := NULL;
   IF str_nhdplus_version IS NOT NULL
   THEN
      int_count := 1;
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         IF str_nhdplus_version = ANY(ary_targeted_programs[i].program_resolutions)
         THEN
            ary_targeted_programs2[int_count] := ary_targeted_programs[i];
            ary_targeted_programs2[int_count].program_resolutions := ARRAY[str_nhdplus_version::VARCHAR];
            int_count := int_count + 1;
            
         END IF;
      
      END LOOP;
      
      IF ary_targeted_programs2 IS NULL
      OR ARRAY_LENGTH(ary_targeted_programs2,1) = 0
      THEN
         out_return_code := -50;
         out_status_message := 'Requested resolution ' || str_nhdplus_version || ' excludes all requested programs.';
         RETURN;
         
      END IF;
   
   ELSE
      ary_targeted_programs2 := ary_targeted_programs;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 60
   -- Try to filter down by precision if provided
   --------------------------------------------------------------------------
   ary_targeted_programs := NULL;
   IF str_search_precision IS NOT NULL
   THEN
      int_count := 1;
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs2,1)
      LOOP
         IF LOWER(str_search_precision) = ANY(ary_targeted_programs2[i].program_precisions)
         OR UPPER(str_search_precision) = ANY(ary_targeted_programs2[i].program_precisions)
         THEN
            ary_targeted_programs[int_count] := ary_targeted_programs2[i];
            int_count := int_count + 1;
            
         END IF;
      
      END LOOP;
      
      IF ary_targeted_programs IS NULL
      OR ARRAY_LENGTH(ary_targeted_programs,1) = 0
      THEN
         out_return_code := -60;
         
         IF str_nhdplus_version IS NOT NULL
         THEN
            out_status_message := 'Requested precision ' || str_search_precision || ' for resolution ' || str_nhdplus_version || ' excludes all requested programs.';
            
         ELSE
            out_status_message := 'Requested precision ' || str_search_precision || ' excludes all requested programs.';
         
         END IF;
         
         RETURN;
         
      END IF;
   
   ELSE
      ary_targeted_programs := ary_targeted_programs2;

   END IF;
   
   --------------------------------------------------------------------------
   -- Step 70
   -- Start with simple CIP JoinKey request
   --------------------------------------------------------------------------
   IF str_query_type = 'cip_joinkey'
   THEN
      <<outer_loop>>
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         FOR j IN 1 .. ARRAY_LENGTH(ary_targeted_programs[i].program_resolutions,1)
         LOOP
            EXECUTE '
               SELECT
                a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.nhdplusid
               ,a.catchmentstatecode
               ,a.cat_joinkey
               FROM
               cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
               WHERE
                   a.catchmentresolution = $1
               AND a.cip_joinkey = $2         
            ' USING
             ary_targeted_programs[i].program_resolutions[j]
            ,p_cip_joinkey
            INTO
             out_permid_joinkey
            ,out_source_originator
            ,out_source_featureid
            ,out_source_featureid2
            ,out_source_series
            ,out_source_subdivision
            ,out_source_joinkey
            ,out_start_date
            ,out_end_date
            ,out_catchment_nhdplusid
            ,out_catchmentstatecode
            ,out_cat_joinkey;
            
            IF out_permid_joinkey IS NOT NULL
            THEN
               out_nhdplus_version     := ary_targeted_programs[i].program_resolutions[j];
               out_linked_data_program := ary_targeted_programs[i].program_id;
               
               IF p_return_shape
               THEN
                  EXECUTE '
                     SELECT
                     a.shape
                     FROM
                     cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip_geo a
                     WHERE
                     a.cat_joinkey = $1
                  ' USING
                  out_cat_joinkey
                  INTO
                  out_shape;
               
               END IF;
               
            END IF;
            
            IF out_permid_joinkey IS NOT NULL
            THEN
               EXIT outer_loop;
            
            END IF;
            
         END LOOP;
   
      END LOOP;
      
      IF out_permid_joinkey IS NULL
      THEN
         out_return_code := -70;
         out_status_message := 'CIP Joinkey ' || p_cip_joinkey || ' not found in requested program set.';
         RETURN;
      
      END IF;
   
   END IF;

   --------------------------------------------------------------------------
   -- Step 80
   -- Next do PermID JoinKey request
   --------------------------------------------------------------------------
   IF str_query_type = 'permid_joinkey'
   THEN
      <<outer_loop>>
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         EXECUTE '
            SELECT
             a.permid_joinkey
            ,a.source_originator
            ,a.source_featureid
            ,a.source_featureid2
            ,a.source_series
            ,a.source_subdivision
            ,a.source_joinkey
            ,a.start_date
            ,a.end_date
            ,CASE WHEN $1 
             THEN
               a.shape
             ELSE
               CAST(NULL AS GEOMETRY)
             END AS shape 
            FROM
            cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_p a
            WHERE
            a.permid_joinkey = $2
         ' USING
          p_return_shape
         ,p_permid_joinkey
         INTO
          out_permid_joinkey
         ,out_source_originator
         ,out_source_featureid
         ,out_source_featureid2
         ,out_source_series
         ,out_source_subdivision
         ,out_source_joinkey
         ,out_start_date
         ,out_end_date
         ,out_shape;

         IF out_permid_joinkey IS NULL
         THEN
            EXECUTE '
               SELECT
                a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,CASE WHEN $1 
                THEN
                  a.shape
                ELSE
                  CAST(NULL AS GEOMETRY)
                END AS shape 
               FROM
               cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_l a
               WHERE
               a.permid_joinkey = $2
            ' USING
             p_return_shape
            ,p_permid_joinkey
            INTO
             out_permid_joinkey
            ,out_source_originator
            ,out_source_featureid
            ,out_source_featureid2
            ,out_source_series
            ,out_source_subdivision
            ,out_source_joinkey
            ,out_start_date
            ,out_end_date
            ,out_shape;
            
            IF out_permid_joinkey IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,CASE WHEN $1 
                   THEN
                     a.shape
                   ELSE
                     CAST(NULL AS GEOMETRY)
                   END AS shape 
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_a a
                  WHERE
                  a.permid_joinkey = $2
               ' USING
                p_return_shape
               ,p_permid_joinkey
               INTO
                out_permid_joinkey
               ,out_source_originator
               ,out_source_featureid
               ,out_source_featureid2
               ,out_source_series
               ,out_source_subdivision
               ,out_source_joinkey
               ,out_start_date
               ,out_end_date
               ,out_shape;
            
            END IF;

         END IF;
         
         IF out_permid_joinkey IS NOT NULL
         THEN
            out_linked_data_program := ary_targeted_programs[i].program_id;
            
            IF p_push_rad_for_permid
            THEN
               EXECUTE '
                  SELECT
                   a.reachresolution
                  ,a.reachcode
                  ,a.measure
                  ,a.feature_permanent_identifier::BIGINT
                  ,CASE WHEN $1 
                   THEN
                     a.shape
                   ELSE
                     CAST(NULL AS GEOMETRY)
                   END AS shape                
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_rad_p a
                  WHERE
                      a.reachresolution = $2
                  AND a.permid_joinkey  = $3
                  LIMIT 1
               ' USING
                p_return_shape
               ,str_nhdplus_version
               ,out_permid_joinkey
               INTO
                out_nhdplus_version
               ,out_reachcode
               ,out_measure
               ,out_flowline_nhdplusid
               ,out_shape;
            
            END IF;
            
            IF out_nhdplus_version IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.catchmentresolution
                  ,a.nhdplusid
                  ,a.catchmentstatecode
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
                  WHERE
                      a.catchmentresolution = $1
                  AND a.permid_joinkey      = $2
                  LIMIT 1
               ' USING
                str_nhdplus_version
               ,out_permid_joinkey
               INTO
                out_nhdplus_version
               ,out_catchment_nhdplusid
               ,out_catchmentstatecode;
               
               IF out_catchment_nhdplusid IS NULL
               THEN
                  EXECUTE '
                     SELECT
                      a.catchmentresolution
                     ,a.nhdplusid
                     ,a.catchmentstatecode
                     FROM
                     cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
                     WHERE
                         a.catchmentresolution = $1
                     AND a.source_joinkey      = $2
                     LIMIT 1
                  ' USING
                   str_nhdplus_version
                  ,out_source_joinkey
                  INTO
                   out_nhdplus_version
                  ,out_catchment_nhdplusid
                  ,out_catchmentstatecode;
               
               END IF;
               
            END IF;

         END IF;
         
         IF out_nhdplus_version IS NOT NULL
         THEN
            EXIT outer_loop;
         
         END IF;
  
      END LOOP;
      
      IF out_permid_joinkey IS NULL
      THEN
         out_return_code := -80;
         out_status_message := 'PermID Joinkey ' || p_permid_joinkey || ' not found in requested program set.';
         RETURN;
      
      END IF;
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 90
   -- Next try naked Source JoinKey request
   --------------------------------------------------------------------------
   IF str_query_type = 'source_joinkey'
   THEN
      <<outer_loop>>
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         EXECUTE '
            SELECT
             a.permid_joinkey
            ,a.source_originator
            ,a.source_featureid
            ,a.source_featureid2
            ,a.source_series
            ,a.source_subdivision
            ,a.source_joinkey
            ,a.start_date
            ,a.end_date
            ,CASE WHEN $1 
             THEN
               a.shape
             ELSE
               CAST(NULL AS GEOMETRY)
             END AS shape 
            FROM
            cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_p a
            WHERE
            a.source_joinkey = $2
            LIMIT 1
         ' USING
          p_return_shape
         ,p_source_joinkey
         INTO
          out_permid_joinkey
         ,out_source_originator
         ,out_source_featureid
         ,out_source_featureid2
         ,out_source_series
         ,out_source_subdivision
         ,out_source_joinkey
         ,out_start_date
         ,out_end_date
         ,out_shape;

         IF out_permid_joinkey IS NULL
         THEN
            EXECUTE '
               SELECT
                a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,CASE WHEN $1 
                THEN
                  a.shape
                ELSE
                  CAST(NULL AS GEOMETRY)
                END AS shape 
               FROM
               cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_l a
               WHERE
               a.source_joinkey = $2
               LIMIT 1
            ' USING
             p_return_shape
            ,p_source_joinkey
            INTO
             out_permid_joinkey
            ,out_source_originator
            ,out_source_featureid
            ,out_source_featureid2
            ,out_source_series
            ,out_source_subdivision
            ,out_source_joinkey
            ,out_start_date
            ,out_end_date
            ,out_shape;
            
            IF out_permid_joinkey IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.permid_joinkey
                  ,a.source_originator
                  ,a.source_featureid
                  ,a.source_featureid2
                  ,a.source_series
                  ,a.source_subdivision
                  ,a.source_joinkey
                  ,a.start_date
                  ,a.end_date
                  ,CASE WHEN $1 
                   THEN
                     a.shape
                   ELSE
                     CAST(NULL AS GEOMETRY)
                   END AS shape 
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_src_a a
                  WHERE
                  a.source_joinkey = $2
                  LIMIT 1
               ' USING
                p_return_shape
               ,p_source_joinkey
               INTO
                out_permid_joinkey
               ,out_source_originator
               ,out_source_featureid
               ,out_source_featureid2
               ,out_source_series
               ,out_source_subdivision
               ,out_source_joinkey
               ,out_start_date
               ,out_end_date
               ,out_shape;
            
            END IF;

         END IF;

         IF out_source_joinkey IS NOT NULL
         THEN
            out_linked_data_program := ary_targeted_programs[i].program_id;

            IF p_push_rad_for_permid
            THEN
               EXECUTE '
                  SELECT
                   a.reachresolution
                  ,a.reachcode
                  ,a.measure
                  ,a.feature_permanent_identifier::BIGINT
                  ,CASE WHEN $1 
                   THEN
                     a.shape
                   ELSE
                     CAST(NULL AS GEOMETRY)
                   END AS shape                
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_rad_p a
                  WHERE
                      a.reachresolution = $2
                  AND a.permid_joinkey  = $3
                  LIMIT 1
               ' USING
                p_return_shape
               ,str_nhdplus_version
               ,out_permid_joinkey
               INTO
                out_nhdplus_version
               ,out_reachcode
               ,out_measure
               ,out_flowline_nhdplusid
               ,geom_shape;
               
               IF geom_shape IS NOT NULL
               THEN
                  out_shape := geom_shape;

               END IF;
               
            END IF;

            IF out_nhdplus_version IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.reachresolution
                  ,a.reachcode
                  ,a.measure
                  ,a.feature_permanent_identifier::BIGINT
                  ,CASE WHEN $1 
                   THEN
                     a.shape
                   ELSE
                     CAST(NULL AS GEOMETRY)
                   END AS shape                
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_rad_p a
                  WHERE
                      a.reachresolution = $2
                  AND a.source_joinkey  = $3
                  LIMIT 1
               ' USING
                p_return_shape
               ,str_nhdplus_version
               ,out_source_joinkey
               INTO
                out_nhdplus_version
               ,out_reachcode
               ,out_measure
               ,out_flowline_nhdplusid
               ,geom_shape;
               
               IF geom_shape IS NOT NULL
               THEN
                  out_shape := geom_shape;

               END IF;

            END IF;
         
            IF out_nhdplus_version IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.catchmentresolution
                  ,a.nhdplusid
                  ,a.catchmentstatecode
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
                  WHERE
                      a.catchmentresolution = $1
                  AND a.permid_joinkey      = $2
                  LIMIT 1
               ' USING
                str_nhdplus_version
               ,out_permid_joinkey
               INTO
                out_nhdplus_version
               ,out_catchment_nhdplusid
               ,out_catchmentstatecode;
               
            END IF;
               
            IF out_catchment_nhdplusid IS NULL
            THEN
               EXECUTE '
                  SELECT
                   a.catchmentresolution
                  ,a.nhdplusid
                  ,a.catchmentstatecode
                  FROM
                  cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
                  WHERE
                      a.catchmentresolution = $1
                  AND a.source_joinkey      = $2
                  LIMIT 1
               ' USING
                str_nhdplus_version
               ,out_source_joinkey
               INTO
                out_nhdplus_version
               ,out_catchment_nhdplusid
               ,out_catchmentstatecode;
            
            END IF;
            
         END IF;
         
         IF out_nhdplus_version IS NOT NULL
         THEN
            EXIT outer_loop;
         
         END IF;
         
      END LOOP;
      
      IF out_permid_joinkey IS NULL
      THEN
         out_return_code := -90;
         out_status_message := 'Source Joinkey ' || p_source_joinkey || ' not found in requested program set.';
         RETURN;
      
      END IF;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 100
   -- next query for catchment by sfid parts
   --------------------------------------------------------------------------
   IF str_query_type = 'source_featureid CATCHMENT'
   THEN
      <<outer_loop>>
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         FOR j IN 1 .. ARRAY_LENGTH(ary_targeted_programs[i].program_resolutions,1)
         LOOP
            EXECUTE '
               SELECT
                a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.nhdplusid
               ,a.catchmentstatecode
               ,a.cat_joinkey
               FROM
               cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip a
               WHERE
                   a.catchmentresolution = $1
               AND ($2  IS NULL OR a.source_originator = $3)
               AND ($4  IS NULL OR a.source_featureid  = $5)
               AND ($6  IS NULL OR a.source_featureid2 = $7)
               AND ($8  IS NULL OR a.source_series     = $9)
               AND ($10 IS NULL OR a.start_date        = $11)
               AND ($12 IS NULL OR a.end_date          = $13)               
            ' USING
             ary_targeted_programs[i].program_resolutions[j]
            ,p_source_originator
            ,p_source_originator
            ,p_source_featureid
            ,p_source_featureid
            ,p_source_featureid2
            ,p_source_featureid2
            ,p_source_series
            ,p_source_series
            ,p_start_date
            ,p_start_date
            ,p_end_date
            ,p_end_date
            INTO
             out_permid_joinkey
            ,out_source_originator
            ,out_source_featureid
            ,out_source_featureid2
            ,out_source_series
            ,out_source_subdivision
            ,out_source_joinkey
            ,out_start_date
            ,out_end_date
            ,out_catchment_nhdplusid
            ,out_catchmentstatecode
            ,out_cat_joinkey;
            
            IF out_permid_joinkey IS NOT NULL
            THEN
               out_nhdplus_version     := ary_targeted_programs[i].program_resolutions[j];
               out_linked_data_program := ary_targeted_programs[i].program_id;
               
               IF p_return_shape
               THEN
                  EXECUTE '
                     SELECT
                     a.shape
                     FROM
                     cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_cip_geo a
                     WHERE
                     a.cat_joinkey = $1
                  ' USING
                  out_cat_joinkey
                  INTO
                  out_shape;
               
               END IF;
               
               RETURN;
               
            END IF;
         
            IF out_permid_joinkey IS NOT NULL
            THEN
               EXIT outer_loop;
            
            END IF;
            
         END LOOP;
   
      END LOOP;
      
      IF out_permid_joinkey IS NULL
      THEN
         out_return_code := -100;
         out_status_message := 'SFID query returned no results in requested CIP indexed program set.';
         RETURN;
      
      END IF;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 110
   -- Finally query for reached by sfid parts
   --------------------------------------------------------------------------
   IF str_query_type = 'source_featureid REACH'
   THEN
      <<outer_loop>>
      FOR i IN 1 .. ARRAY_LENGTH(ary_targeted_programs,1)
      LOOP
         FOR j IN 1 .. ARRAY_LENGTH(ary_targeted_programs[i].program_resolutions,1)
         LOOP
            EXECUTE '
               SELECT
                a.permid_joinkey
               ,a.source_originator
               ,a.source_featureid
               ,a.source_featureid2
               ,a.source_series
               ,a.source_subdivision
               ,a.source_joinkey
               ,a.start_date
               ,a.end_date
               ,a.feature_permanent_identifier::BIGINT
               ,a.geogstate
               ,a.geogstate || a.feature_permanent_identifier
               ,CASE WHEN $1 
                THEN
                  a.shape
                ELSE
                  CAST(NULL AS GEOMETRY)
                END AS shape
               FROM
               cipsrv_owld.' || LOWER(ary_targeted_programs[i].program_id) || '_rad_p a
               WHERE
                   a.reachresolution = $2
               AND ($3  IS NULL OR a.source_originator = $4)
               AND ($5  IS NULL OR a.source_featureid  = $6)
               AND ($7  IS NULL OR a.source_featureid2 = $8)
               AND ($9  IS NULL OR a.source_series     = $10)
               AND ($11 IS NULL OR a.start_date        = $12)
               AND ($13 IS NULL OR a.end_date          = $14)               
            ' USING
             p_return_shape
            ,ary_targeted_programs[i].program_resolutions[j]
            ,p_source_originator
            ,p_source_originator
            ,p_source_featureid
            ,p_source_featureid
            ,p_source_featureid2
            ,p_source_featureid2
            ,p_source_series
            ,p_source_series
            ,p_start_date
            ,p_start_date
            ,p_end_date
            ,p_end_date
            INTO
             out_permid_joinkey
            ,out_source_originator
            ,out_source_featureid
            ,out_source_featureid2
            ,out_source_series
            ,out_source_subdivision
            ,out_source_joinkey
            ,out_start_date
            ,out_end_date
            ,out_catchment_nhdplusid
            ,out_catchmentstatecode
            ,out_cat_joinkey;
            
            IF out_permid_joinkey IS NOT NULL
            THEN
               out_nhdplus_version     := ary_targeted_programs[i].program_resolutions[j];
               out_linked_data_program := ary_targeted_programs[i].program_id;
               
               RETURN;
             
            END IF;
         
            IF out_permid_joinkey IS NOT NULL
            THEN
               EXIT outer_loop;
            
            END IF;
            
         END LOOP;
   
      END LOOP;
      
      IF out_permid_joinkey IS NULL
      THEN
         out_return_code := -110;
         out_status_message := 'SFID query returned no results in requested reached indexed program set.';
         RETURN;
      
      END IF;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 120
   -- Error out with no results
   --------------------------------------------------------------------------
   IF out_permid_joinkey IS NULL
   THEN
      out_return_code := -120;
      out_status_message := 'Inputs failed to query.';
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 130
   -- Try to value add
   --------------------------------------------------------------------------
   IF  out_nhdplus_version IS NOT NULL
   AND out_catchment_nhdplusid IS NOT NULL
   AND out_measure IS NULL
   THEN
      rec := cipsrv_engine.get_measure(
          p_nhdplus_version  := out_nhdplus_version
         ,p_direction        := p_search_direction
         ,p_nhdplusid        := out_catchment_nhdplusid
      );
      
      out_measure              := rec.out_measure;
      out_permanent_identifier := rec.out_permanent_identifier;
      out_reachcode            := rec.out_reachcode;
      out_hydroseq             := rec.out_hydroseq;
      
   END IF;
         
END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_owld.owld_locator';
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
