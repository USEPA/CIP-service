DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.feature_clip';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.feature_clip(
    IN  p_features                      cipsrv_engine.cip_feature[]
   ,IN  p_clippers                      VARCHAR[]
   ,IN  p_known_region                  VARCHAR
   ,OUT out_return_code                 INTEGER
   ,OUT out_status_message              VARCHAR
   ,OUT out_features                    cipsrv_engine.cip_feature[]
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                  RECORD;
   obj_rez cipsrv_engine.cip_feature;
   ary_rez cipsrv_engine.cip_feature[];
   str_known_region     VARCHAR := p_known_region;
   sdo_output           GEOMETRY;
   
BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_features IS NULL
   OR array_length(p_features,1) = 0
   THEN
      RETURN;
      
   END IF;
   
   IF p_clippers IS NULL
   OR array_length(p_clippers,1) = 0
   THEN
      out_features := p_features;
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Loop over the features
   ----------------------------------------------------------------------------
   FOR i IN 1 .. array_length(p_features,1)
   LOOP
      obj_rez := p_features[i];
      
      rec := cipsrv_support.geometry_clip(
          p_geometry      := (obj_rez).geometry
         ,p_clippers      := p_clippers
         ,p_known_region  := str_known_region
      );
      sdo_output         := rec.out_clipped_geometry;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
      
      IF out_return_code != 0
      THEN
         RETURN;
         
      END IF;
      
      IF sdo_output IS NULL
      OR ST_IsEmpty(sdo_output)
      THEN
         NULL;
         
      ELSE
         obj_rez.geometry := sdo_output;
         out_features     := array_append(out_features,obj_rez);
         
      END IF;
         
   END LOOP;
   
   RETURN;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.feature_clip(
    cipsrv_engine.cip_feature[]
   ,VARCHAR[]
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.feature_clip(
    cipsrv_engine.cip_feature[]
   ,VARCHAR[]
   ,VARCHAR
) TO PUBLIC;

