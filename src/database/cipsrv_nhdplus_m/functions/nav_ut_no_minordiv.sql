DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.nav_ut_no_minordiv';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.nav_ut_no_minordiv(
    IN  obj_start_flowline       cipsrv_nhdplus_m.flowline
   ,IN  num_maximum_distancekm   NUMERIC
   ,IN  num_maximum_flowtimeday  NUMERIC
) RETURNS INTEGER
VOLATILE
AS $BODY$
DECLARE
   rec                      RECORD;
   ary_branches             cipsrv_nhdplus_m.flowline[];
   int_check                INTEGER;
   int_count                INTEGER;
   int_return_code          INTEGER;
   str_status_message       VARCHAR;
   int_sanity               INTEGER;
   int_sanity_check         INTEGER := 400;
   int_curr_branch_id       INTEGER;
   boo_search               BOOLEAN;
   ary_working              cipsrv_nhdplus_m.flowline[];
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
   
   ary_working := ARRAY[obj_start_flowline]::cipsrv_nhdplus_m.flowline[];
   ary_working[1].nav_order           := 0;
   ary_working[1].network_distancekm  := obj_start_flowline.out_lengthkm;
   ary_working[1].network_flowtimeday := obj_start_flowline.out_flowtimeday;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Run a single upstream navigation excluding dnstream minor divergences
   ----------------------------------------------------------------------------
   boo_search := TRUE;
   
   WHILE boo_search
   LOOP
      num_pathlength_adj := ary_working[1].pathlength_adj;
      num_pathtimema_adj := ary_working[1].pathflowtime_adj;
      
      rec := cipsrv_nhdplus_m.nav_ut_search(
          p_start_flowline       := ary_working[1]
         ,p_maximum_distancekm   := num_maximum_distancekm
         ,p_maximum_flowtimeday  := num_maximum_flowtimeday
         ,p_init_baselengthkm    := num_init_baselengthkm
         ,p_init_baseflowtimeday := num_init_baseflowtimeday 
         ,p_base_arbolatesu      := obj_start_flowline.arbolatesu
         ,p_branch_id            := int_curr_branch_id
      );
      ary_branches       := rec.out_branches;
      int_check          := rec.out_flowline_count;
      int_return_code    := rec.out_return_code;
      str_status_message := rec.out_status_message;

      IF int_check > 0
      THEN
         int_count          := int_count + int_check - COALESCE(ARRAY_LENGTH(ary_branches,1),0);
         int_curr_branch_id := int_curr_branch_id + 1;
      
         IF int_check > 10000
         THEN
            RAISE WARNING '% hydroseq: %',int_check,ary_working[1].hydroseq;
            
         END IF;
         
      END IF;
      
      ary_working        := ary_working[2:];
      
      ----------------------------------------------------------------------------
      -- Prioritize following branches if found
      ----------------------------------------------------------------------------
      IF COALESCE(ARRAY_LENGTH(ary_branches,1),0) > 0
      THEN
         ary_working := cipsrv_nhdplus_m.append_flowlines(
             p_input  := ary_branches
            ,p_target := ary_working
            ,p_sort_by_ordering_key := TRUE
         );
      
      END IF;

      ----------------------------------------------------------------------------
      -- Check for loops or other problems
      ----------------------------------------------------------------------------
      int_sanity := int_sanity + 1;
      IF int_sanity > int_sanity_check
      THEN
         RAISE EXCEPTION 'sanity check % for hydroseq %: %',int_sanity,obj_start_flowline.hydroseq,TO_JSON(ary_working);
         
      END IF;
      
      IF COALESCE(ARRAY_LENGTH(ary_working,1),0) = 0
      THEN
         boo_search := FALSE;
         
      END IF;

   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Tag remaining upstream mainline nav termination flags
   ----------------------------------------------------------------------------
   FOR rec IN
      SELECT
       a.hydroseq
      ,b.ary_upstream_hydroseq
      ,b.headwater
      FROM
      tmp_navigation_working30 a
      JOIN
      cipsrv_nhdplus_m.nhdplusflowlinevaa_nav b
      ON
      b.hydroseq = a.hydroseq
      WHERE
      a.navtermination_flag IS NULL
   LOOP
      UPDATE tmp_navigation_working30 a
      SET navtermination_flag = CASE
      WHEN EXISTS ( SELECT 1 FROM tmp_navigation_working30 d WHERE d.hydroseq = ANY(rec.ary_upstream_hydroseq) )
      THEN
         0
      ELSE
         CASE
         WHEN rec.headwater
         THEN
            4
         ELSE
            1
         END
      END
      WHERE
      a.hydroseq = rec.hydroseq;
   
   END LOOP;

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
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.nav_ut_no_minordiv';
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

