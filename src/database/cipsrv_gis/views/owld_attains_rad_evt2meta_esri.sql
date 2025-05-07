DROP VIEW IF EXISTS cipsrv_gis.owld_attains_rad_evt2meta_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','attains_rad_evt2meta') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_attains_rad_evt2meta_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.meta_processid
      ,a.globalid
      FROM
      cipsrv_owld.attains_rad_evt2meta a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_attains_rad_evt2meta_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_attains_rad_evt2meta_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_attains_rad_evt2meta';
   
   END IF;

END$$;
