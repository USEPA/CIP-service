DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.append_flowlines';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.append_flowlines(
    IN  p_input                        cipsrv_nhdplus_h.flowline[]
   ,IN  p_target                       cipsrv_nhdplus_h.flowline[]
   ,IN  p_sort_by_ordering_key         BOOLEAN
) RETURNS cipsrv_nhdplus_h.flowline[]
IMMUTABLE
AS $BODY$ 
DECLARE
   rez cipsrv_nhdplus_h.flowline[];
   boo_check BOOLEAN;
   
BEGIN

   IF COALESCE(ARRAY_LENGTH(p_input,1),0) = 0
   THEN
      RETURN p_target;
      
   ELSIF COALESCE(ARRAY_LENGTH(p_target,1),0) = 0
   THEN
      RETURN p_input;
      
   ELSE
      rez := p_target;
      
      FOR i IN 1 .. ARRAY_LENGTH(p_input,1)
      LOOP
         boo_check := TRUE;
         
         FOR j IN 1 .. ARRAY_LENGTH(rez,1)
         LOOP
            IF rez[j].hydroseq = p_input[i].hydroseq
            THEN
               boo_check := FALSE;
               
            END IF;
         
         END LOOP;
         
         IF boo_check
         THEN
            rez := ARRAY_APPEND(rez,p_input[i]);
         
         END IF;
      
      END LOOP;
      
   END IF;
   
   IF COALESCE(ARRAY_LENGTH(rez,1),0) = 1
   THEN
      RETURN rez;
      
   ELSE
   
      IF p_sort_by_ordering_key
      THEN
         SELECT 
         ARRAY_AGG(ct ORDER BY ct.ordering_key DESC)
         INTO rez
         FROM
         UNNEST(rez) AS ct;
         
      END IF;
      
      RETURN rez;
      
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.append_flowlines';
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

