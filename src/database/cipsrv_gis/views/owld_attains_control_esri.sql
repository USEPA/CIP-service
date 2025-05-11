DROP VIEW IF EXISTS cipsrv_gis.owld_attains_control_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','attains_control') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_attains_control_esri
      AS
      SELECT
       (ROW_NUMBER() OVER())::INTEGER AS objectid
      ,a.keyword
      ,a.value_str
      ,a.value_num
      ,a.value_date
      FROM
      cipsrv_owld.attains_control a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_attains_control_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_attains_control_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_attains_control_esri';
   
   END IF;

END$$;

