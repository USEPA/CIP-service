DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.randompoint';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.randompoint(
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
   sdo_box         GEOMETRY;
   int_count       INTEGER;
   num_min_x       NUMERIC;
   num_min_y       NUMERIC;
   num_max_x       NUMERIC;
   num_max_y       NUMERIC;
   num_rand_x      NUMERIC;
   num_rand_y      NUMERIC;
   boo_search      BOOLEAN;
   int_sanity      INTEGER;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Select a random catchment
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.randomcatchment(
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
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Box the catchment and get bounds
   --------------------------------------------------------------------------
   sdo_box   := ST_Extent(rec.out_shape);
   num_min_x := ST_XMin(sdo_box);
   num_min_y := ST_YMin(sdo_box);
   num_max_x := ST_XMax(sdo_box);
   num_max_y := ST_YMax(sdo_box);
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Limit the point to being within the catchment
   --------------------------------------------------------------------------
   boo_search := TRUE;
   int_sanity := 0;
   WHILE boo_search
   LOOP
      num_rand_x := RANDOM() * (num_max_x - num_min_x) + num_min_x;
      num_rand_y := RANDOM() * (num_max_y - num_min_y) + num_min_y;
      out_shape  := ST_SetSRID(ST_Point(num_rand_x,num_rand_y),ST_SRID(rec.out_shape));
   
      IF ST_Within(out_shape,rec.out_shape)
      THEN
         boo_search := FALSE;
      
      END IF;
      
      int_sanity := int_sanity + 1;
      IF int_sanity > 25
      THEN
         out_return_code    := -9;
         out_status_message := 'Unable to process ' || rec.out_nhdplusid::VARCHAR || '.';
         RETURN;
         
      END IF;
   
   END LOOP;
   
   out_shape := ST_Transform(out_shape,4269);
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.randompoint(
    VARCHAR
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.randompoint(
    VARCHAR
   ,BOOLEAN
) TO PUBLIC;

