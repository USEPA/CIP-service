CREATE OR REPLACE FUNCTION cipsrv_engine.feature_batch_clip(
    IN  p_keyword                       VARCHAR
   ,IN  p_clippers                      VARCHAR[]
   ,IN  p_known_region                  VARCHAR
   ,OUT out_return_code                 INTEGER
   ,OUT out_status_message              VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   rec                  RECORD;
   str_sql              VARCHAR;
   
BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_clippers IS NULL
   OR array_length(p_clippers,1) = 0
   THEN
      RETURN;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Clip the point features
   ----------------------------------------------------------------------------
   str_sql := 'UPDATE cipsrv_upload.' || p_keyword || '_points a '
           || 'SET shape = ( '
           || '   SELECT '
           || '   b.out_clipped_geometry '
           || '   FROM '
           || '   cipsrv_support.geometry_clip( '
           || '       p_geometry      := a.shape '
           || '      ,p_clippers      := ? '
           || '      ,p_known_region  := ? '
           || '   ) '
           || ') ';
           
   EXECUTE str_sql USING p_clippers,p_known_region;
   
   str_sql := 'DELETE FROM cipsrv_upload.' || p_keyword || '_points a '
           || 'WHERE '
           || 'a.shape IS NULL ';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Clip the line features
   ----------------------------------------------------------------------------
   str_sql := 'UPDATE cipsrv_upload.' || p_keyword || '_lines a '
           || 'SET shape = ( '
           || '   SELECT '
           || '   b.out_clipped_geometry '
           || '   FROM '
           || '   cipsrv_support.geometry_clip( '
           || '       p_geometry      := a.shape '
           || '      ,p_clippers      := ? '
           || '      ,p_known_region  := ? '
           || '   ) '
           || ') ';
           
   EXECUTE str_sql USING p_clippers,p_known_region;
   
   str_sql := 'DELETE FROM cipsrv_upload.' || p_keyword || '_lines a '
           || 'WHERE '
           || 'a.shape IS NULL ';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Clip the area features
   ----------------------------------------------------------------------------
   str_sql := 'UPDATE cipsrv_upload.' || p_keyword || '_areas a '
           || 'SET shape = ( '
           || '   SELECT '
           || '   b.out_clipped_geometry '
           || '   FROM '
           || '   cipsrv_support.geometry_clip( '
           || '       p_geometry      := a.shape '
           || '      ,p_clippers      := ? '
           || '      ,p_known_region  := ? '
           || '   ) '
           || ') ';
           
   EXECUTE str_sql USING p_clippers,p_known_region;
   
   str_sql := 'DELETE FROM cipsrv_upload.' || p_keyword || '_areas a '
           || 'WHERE '
           || 'a.shape IS NULL ';
           
   EXECUTE str_sql;
   
   RETURN;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.feature_batch_clip(
    VARCHAR
   ,VARCHAR[]
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.feature_batch_clip(
    VARCHAR
   ,VARCHAR[]
   ,VARCHAR
) TO PUBLIC;

