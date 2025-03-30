DROP VIEW IF EXISTS cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_m','catchment_fabric_huc12_np21') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_huc12_version
      ,a.areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_huc12_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21 OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21 TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21';
   
   END IF;

END$$;
