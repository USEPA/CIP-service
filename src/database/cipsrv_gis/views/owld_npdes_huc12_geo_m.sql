DROP VIEW IF EXISTS cipsrv_gis.owld_npdes_huc12_geo_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','npdes_huc12_geo') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_npdes_huc12_geo_m
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_catresolution
      ,a.xwalk_huc12_version
      ,a.xwalk_huc12_areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.npdes_huc12_geo a
      WHERE
      a.xwalk_catresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_npdes_huc12_geo_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_npdes_huc12_geo_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.npdes_huc12_geo_m';
   
   END IF;

END$$;
