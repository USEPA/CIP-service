DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.nav_ut_extended';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.nav_ut_extended(
    IN  obj_start_flowline       cipsrv_nhdplus_h.flowline
   ,IN  num_maximum_distancekm   NUMERIC
   ,IN  num_maximum_flowtimeday  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   rec                      RECORD;
   ary_branches             BIGINT[];
   int_check                INTEGER;
   int_count                INTEGER;
   int_return_code          INTEGER;
   str_status_message       VARCHAR;
   int_sanity               INTEGER;
   int_sanity_check         INTEGER := 100;
   int_curr_branch_id       INTEGER;
   boo_search               BOOLEAN;
   ary_working              cipsrv_nhdplus_h.flowline[];
   num_pathlength_adj       NUMERIC;
   num_pathtimema_adj       NUMERIC;
   boo_check                BOOLEAN;
   num_init_baselengthkm    NUMERIC;
   num_init_baseflowtimeday NUMERIC;
   
BEGIN

   int_count          := 0;
   int_sanity         := 0;
   
   int_curr_branch_id := 0;
   
   num_pathlength_adj := 0;
   num_pathtimema_adj := 0;
   
   num_init_baselengthkm    := obj_start_flowline.pathlengthkm    + (obj_start_flowline.lengthkm    - obj_start_flowline.out_lengthkm);
   num_init_baseflowtimeday := obj_start_flowline.pathflowtimeday + (obj_start_flowline.flowtimeday - obj_start_flowline.out_flowtimeday);
   
   ary_working := ARRAY[obj_start_flowline]::cipsrv_nhdplus_h.flowline[];
   ary_working[1].nav_order := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Run a single upstream navigation excluding dnstream minor divergences
   ----------------------------------------------------------------------------
   boo_search := TRUE;
   
   WHILE boo_search
   LOOP
      RAISE WARNING 'start %, adj % nd %',ary_working[1].hydroseq,ary_working[1].pathlength_adj,ary_working[1].network_distancekm;
      RAISE WARNING '%',TO_JSON(ary_working[1]);
      
      num_pathlength_adj := ary_working[1].pathlength_adj;
      num_pathtimema_adj := ary_working[1].pathflowtime_adj;
      rec := cipsrv_nhdplus_h.nav_ut_extended_branch(
          p_start_flowline       := ary_working[1]
         ,p_maximum_distancekm   := num_maximum_distancekm
         ,p_maximum_flowtimeday  := num_maximum_flowtimeday
         ,p_init_baselengthkm    := num_init_baselengthkm
         ,p_init_baseflowtimeday := num_init_baseflowtimeday 
         ,p_base_arbolatesu      := obj_start_flowline.arbolatesu
         ,p_branch_id            := int_curr_branch_id
      );
      ary_working        := ary_working[2:];
      ary_branches       := rec.out_branches;
      int_check          := rec.out_flowline_count;
      int_return_code    := rec.out_return_code;
      str_status_message := rec.out_status_message;
      
      RAISE WARNING 'branch count %',ARRAY_LENGTH(ary_branches,1); 
      IF int_check > 0
      THEN
         int_count          := int_count + int_check;
         int_curr_branch_id := int_curr_branch_id + 1;
      
      END IF;  

      ----------------------------------------------------------------------------
      -- Search for open minor divergences
      ----------------------------------------------------------------------------
      rec := cipsrv_nhdplus_h.nav_ut_extended_minordiv(
          p_maximum_distancekm     := num_maximum_distancekm
         ,p_maximum_flowtimeday    := num_maximum_flowtimeday
         ,p_source_pathlength_adj  := num_pathlength_adj
         ,p_source_pathtimema_adj  := num_pathtimema_adj
      );
      IF COALESCE(ARRAY_LENGTH(rec.out_minor_divs,1),0) = 0
      THEN
         NULL;
         
      ELSE      
         IF COALESCE(ARRAY_LENGTH(ary_working,1),0) = 0
         THEN
            ary_working := rec.out_minor_divs;
            
         ELSE
            FOR i IN 1 .. ARRAY_LENGTH(rec.out_minor_divs,1)
            LOOP
               boo_check := TRUE;
               
               FOR j IN 1 .. ARRAY_LENGTH(ary_working,1)
               LOOP
                  IF ary_working[j].hydroseq = rec.out_minor_divs[i].hydroseq
                  THEN
                     boo_check := FALSE;
                     
                  END IF;
               
               END LOOP;
               
               IF boo_check
               THEN
                  ary_working := ARRAY_APPEND(ary_working,rec.out_minor_divs[i]);
               
               END IF;
            
            END LOOP;
         
         END IF;
         
      END IF;
      
      RAISE WARNING 'minors %',TO_JSON(ary_working); 
      
      

      int_sanity := int_sanity + 1;
      IF int_sanity > int_sanity_check
      THEN
         RAISE EXCEPTION 'sanity check';
         
      END IF;
      
      IF ARRAY_LENGTH(ary_working,1) IS NULL
      OR ARRAY_LENGTH(ary_working,1) = 0
      THEN
         boo_search := FALSE;
         
      END IF;
      select count(*) into int_check from tmp_navigation_working30;
      raise warning 'bottom %',int_check;
   END LOOP;
   raise warning '<><> %',int_count;
   ----------------------------------------------------------------------------
   -- Step 20
   -- Tag the upstream mainline nav termination flags
   ----------------------------------------------------------------------------
   WITH cte AS ( 
      SELECT
       a.hydroseq
      ,b.ary_upstream_hydroseq
      ,b.headwater
      ,b.coastal_connection
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_h.nhdplusflowlinevaa_nav b
      ON
      a.hydroseq = b.hydroseq
      WHERE
      a.navtermination_flag IS NULL
   )
   UPDATE tmp_navigation_working30 a
   SET navtermination_flag = CASE
   WHEN EXISTS ( SELECT 1 FROM tmp_navigation_working30 d WHERE ARRAY[d.hydroseq] @> cte.ary_upstream_hydroseq )
   THEN
      0
   ELSE
      CASE
      WHEN cte.headwater
      THEN
         4
      ELSE
         1
      END
   END
   FROM cte
   WHERE
   a.hydroseq = cte.hydroseq;

   ----------------------------------------------------------------------------
   -- Step 30
   -- Return total count of results
   ----------------------------------------------------------------------------
   RETURN int_count;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.nav_ut_extended';
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

