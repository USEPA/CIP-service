DROP VIEW IF EXISTS cipsrv_gis.owld_frspub_huc12_geo_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','frspub_huc12_geo') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_frspub_huc12_geo_h
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
      cipsrv_owld.frspub_huc12_geo a
      WHERE
      a.xwalk_catresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_frspub_huc12_geo_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_frspub_huc12_geo_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.frspub_huc12_geo_h';
   
   END IF;

END$$;
