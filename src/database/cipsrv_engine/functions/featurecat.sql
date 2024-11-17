DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.featurecat';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.featurecat(
    IN  p_features           cipsrv_engine.cip_feature[]
   ,IN  p_cat                cipsrv_engine.cip_feature[]
) RETURNS cipsrv_engine.cip_feature[]
IMMUTABLE
AS $BODY$ 
DECLARE
   ary_features cipsrv_engine.cip_feature[];
   ary_cat      cipsrv_engine.cip_feature[];
   ary_rez      cipsrv_engine.cip_feature[];
   
BEGIN

   ary_features := array_remove(p_features,NULL);
   ary_cat      := array_remove(p_cat,NULL);
   ary_rez      := NULL::cipsrv_engine.cip_feature[];
   
   IF ary_features IS NOT NULL
   AND array_length(ary_features,1) > 0
   THEN
      FOR i IN 1 .. array_length(ary_features,1)
      LOOP
         ary_rez := array_append(ary_rez,ary_features[i]);
         
      END LOOP;
      
   END IF;
   
   IF ary_cat IS NOT NULL 
   AND array_length(ary_cat,1) > 0
   THEN
      FOR i IN 1 .. array_length(ary_cat,1)
      LOOP
         ary_rez := array_append(ary_rez,ary_cat[i]);
         
      END LOOP;
   
   END IF;

   RETURN ary_rez;   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.featurecat(
    cipsrv_engine.cip_feature[]
   ,cipsrv_engine.cip_feature[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.featurecat(
    cipsrv_engine.cip_feature[]
   ,cipsrv_engine.cip_feature[]
) TO PUBLIC;

