DROP VIEW IF EXISTS cipsrv_gis.epageofab_h_catchment_fabric;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_h','catchment_fabric') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_h_catchment_fabric
      AS
      SELECT
       a.objectid
      ,a.catchmentstatecode
      ,a.nhdplusid
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
      ,a.state_count
      ,a.vpuid
      ,a.sourcedataset
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_h.catchment_fabric a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_h_catchment_fabric OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_h_catchment_fabric TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_h_catchment_fabric';
   
   END IF;

END$$;
