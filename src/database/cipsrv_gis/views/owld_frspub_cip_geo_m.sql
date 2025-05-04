DROP VIEW IF EXISTS cipsrv_gis.owld_frspub_cip_geo_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','frspub_cip_geo') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_frspub_cip_geo_m
      AS
      SELECT
       a.objectid
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,a.nhdplusid
      ,a.catchmentresolution
      ,a.catchmentareasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.frspub_cip_geo a
      WHERE
      a.catchmentresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_frspub_cip_geo_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_frspub_cip_geo_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_frspub_cip_geo_m';
   
   END IF;

END$$;

