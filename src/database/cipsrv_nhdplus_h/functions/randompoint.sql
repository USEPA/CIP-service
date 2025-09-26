DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.randompoint';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.randompoint(
    IN  p_region               VARCHAR DEFAULT NULL
   ,IN  p_include_extended     BOOLEAN DEFAULT FALSE
   ,OUT out_shape              GEOMETRY
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec             RECORD;
   sdo_random_pts  public.GEOMETRY;
   boo_search      BOOLEAN;
   int_sanity      INTEGER;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Get a random point from random catchment
   --------------------------------------------------------------------------
   boo_search := TRUE;
   int_sanity := 0;
   WHILE boo_search
   LOOP
      rec := cipsrv_nhdplus_h.randomcatchment(
          p_region           := p_region
         ,p_include_extended := p_include_extended
         ,p_return_geometry  := TRUE
      );
      
      IF rec.out_return_code != 0
      THEN
         out_return_code    := rec.out_return_code;
         out_status_message := rec.out_status_message;
         RETURN;
      
      END IF;
   
      sdo_random_pts := public.ST_GENERATEPOINTS(rec.out_shape,1);
      
      IF sdo_random_pts IS NOT NULL
      AND NOT public.ST_ISEMPTY(sdo_random_pts)
      THEN
         EXIT;
         
      END IF;
      
      int_sanity := int_sanity + 1;
      IF int_sanity > 25
      THEN
         out_return_code    := -9;
         out_status_message := 'Unable to process ' || rec.out_nhdplusid::VARCHAR || '.';
         RETURN;
         
      END IF;
   
   END LOOP;
   
   out_shape := public.ST_TRANSFORM(
       public.ST_GEOMETRYN(sdo_random_pts,1)
      ,4269
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.randompoint(
    VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.randompoint(
    VARCHAR
   ,BOOLEAN
) TO PUBLIC;

