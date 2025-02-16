DROP VIEW IF EXISTS cipsrv_gis.tiger_aiannha;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_support'
      AND   a.table_name   = 'tiger_aiannha'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.tiger_aiannha
      AS
      SELECT
       a.objectid
      ,a.aiannhns
      ,a.geoid
      ,a.namelsad
      ,a.classfp
      ,a.comptyp
      ,a.aiannhr
      ,a.mtfcc
      ,a.funcstat
      ,a.aland
      ,a.awater
      ,a.intptlat
      ,a.intptlon
      ,a.shape
      FROM
      cipsrv_support.tiger_aiannha a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.tiger_aiannha OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.tiger_aiannha TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping tiger_aiannha';
   
   END IF;

END$$;
