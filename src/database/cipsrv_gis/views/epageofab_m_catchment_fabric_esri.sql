DROP VIEW IF EXISTS cipsrv_gis.epageofab_m_catchment_fabric_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_m','catchment_fabric') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric_esri
      AS
      SELECT
       a.objectid
      ,a.catchmentstatecode
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.istribal
      ,a.istribal_areasqkm
      ,a.sourcefc
      ,a.gridcode
      ,a.areasqkm
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,CAST(a.state_count AS SMALLINT) AS state_count
      ,a.vpuid
      ,a.sourcedataset
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_m.catchment_fabric a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_m_catchment_fabric_esri';
   
   END IF;

END$$;
