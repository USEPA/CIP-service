DROP VIEW IF EXISTS cipsrv_gis.epageofab_m_catchment_fabric_huc12_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_m','catchment_fabric_huc12_f3') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric_huc12_f3_esri
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_huc12_version
      ,a.areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_huc12_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric_huc12_f3_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric_huc12_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_m_catchment_fabric_huc12_f3_esri';
   
   END IF;

END$$;
