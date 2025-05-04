DROP VIEW IF EXISTS cipsrv_gis.owld_frspub_cip_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','frspub_cip') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_frspub_cip_h
      AS
      SELECT
       a.objectid
      ,a.cip_joinkey
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,a.nhdplusid
      ,a.istribal
      ,a.istribal_areasqkm
      ,a.catchmentresolution
      ,a.catchmentareasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.globalid
      FROM
      cipsrv_owld.frspub_cip a
      WHERE
      a.catchmentresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_frspub_cip_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_frspub_cip_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_frspub_cip_h';
   
   END IF;

END$$;
