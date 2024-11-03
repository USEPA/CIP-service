DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.pointindexing';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.pointindexing(
    IN  p_point                        GEOMETRY
   ,IN  p_indexing_engine              VARCHAR
   ,IN  p_fcode_allow                  INTEGER[]
   ,IN  p_fcode_deny                   INTEGER[]
   ,IN  p_distance_max_distkm          NUMERIC
   ,IN  p_raindrop_snap_max_distkm     NUMERIC
   ,IN  p_raindrop_path_max_distkm     NUMERIC
   ,IN  p_limit_innetwork              BOOLEAN
   ,IN  p_limit_navigable              BOOLEAN
   ,IN  p_fallback_fcode_allow         INTEGER[]
   ,IN  p_fallback_fcode_deny          INTEGER[]
   ,IN  p_fallback_distance_max_distkm NUMERIC
   ,IN  p_fallback_limit_innetwork     BOOLEAN
   ,IN  p_fallback_limit_navigable     BOOLEAN
   ,IN  p_return_link_path             BOOLEAN
   ,IN  p_known_region                 VARCHAR
   ,IN  p_known_catchment_nhdplusid    BIGINT   DEFAULT NULL
   ,OUT out_flowlines                  cipsrv_nhdplus_m.flowline[]
   ,OUT out_path_distance_km           NUMERIC
   ,OUT out_end_point                  GEOMETRY
   ,OUT out_indexing_line              GEOMETRY
   ,OUT out_region                     VARCHAR
   ,OUT out_nhdplusid                  BIGINT
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                 RECORD;
   str_indexing_engine VARCHAR(4000) := UPPER(p_indexing_engine);
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   -------------------------------------------------------------------------
   -- Step 20
   -- Check distance engines first
   -------------------------------------------------------------------------
   IF str_indexing_engine IN ('DISTANCE')
   THEN
      rec := cipsrv_nhdplus_m.distance_index(
          p_point                     => p_point
         ,p_fcode_allow               => p_fcode_allow
         ,p_fcode_deny                => p_fcode_deny
         ,p_max_dist_km               => p_distance_max_distkm
         ,p_limit_innetwork           => p_limit_innetwork
         ,p_limit_navigable           => p_limit_navigable
         ,p_return_link_path          => p_return_link_path
         ,p_known_region              => p_known_region
      );
     
   -------------------------------------------------------------------------
   -- Step 30
   -- Check cat constrained 
   -------------------------------------------------------------------------
   ELSIF str_indexing_engine IN ('CATCONSTRAINED')
   THEN
      rec := cipsrv_nhdplus_m.catconstrained_index(
          p_point                     => p_point
         ,p_return_link_path          => p_return_link_path
         ,p_known_region              => p_known_region
         ,p_known_catchment_nhdplusid => p_known_catchment_nhdplusid
      );

   -------------------------------------------------------------------------
   -- Step 40
   -- Check raindrop
   -------------------------------------------------------------------------
   ELSIF str_indexing_engine = 'RAINDROP'
   THEN
      rec := cipsrv_nhdplus_m.raindrop_index(
          p_point                     => p_point
         ,p_fcode_allow               => p_fcode_allow
         ,p_fcode_deny                => p_fcode_deny
         ,p_raindrop_snap_max_distkm  => p_raindrop_snap_max_distkm
         ,p_raindrop_path_max_distkm  => p_raindrop_path_max_distkm
         ,p_limit_innetwork           => p_limit_innetwork
         ,p_limit_navigable           => p_limit_navigable
         ,p_return_link_path          => p_return_link_path
         ,p_known_region              => p_known_region
      );
      
   ELSE
      out_return_code    := -1;
      out_status_message := 'Unknown indexing engine type.';
      RETURN;
      
   END IF;
   
   -------------------------------------------------------------------------
   -- Step 50
   -- Return results
   -------------------------------------------------------------------------
   out_return_code      := rec.out_return_code;
   out_status_message   := rec.out_status_message;
   
   out_flowlines        := rec.out_flowlines;
   out_path_distance_km := rec.out_path_distance_km;
   out_end_point        := rec.out_end_point;
   out_indexing_line    := rec.out_indexing_line;
   out_region           := rec.out_region;
   out_nhdplusid        := rec.out_nhdplusid;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.pointindexing(
    GEOMETRY
   ,VARCHAR
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,BIGINT
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.pointindexing(
    GEOMETRY
   ,VARCHAR
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,INTEGER[]
   ,INTEGER[]
   ,NUMERIC
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,BIGINT
) TO PUBLIC;
