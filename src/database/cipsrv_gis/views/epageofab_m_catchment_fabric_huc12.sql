DROP VIEW IF EXISTS cipsrv_gis.epageofab_m_catchment_fabric_huc12;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_epageofab_m'
      AND   a.table_name   = 'catchment_fabric_huc12'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric_huc12
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_huc12_version
      ,a.areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_huc12 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric_huc12 OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric_huc12 TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_m_catchment_fabric_huc12';
   
   END IF;

END$$;
