DROP VIEW IF EXISTS cipsrv_gis.owld_attains_huc12_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','attains_huc12') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_attains_huc12_h
      AS
      SELECT
       a.objectid
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.xwalk_huc12
      ,a.xwalk_catresolution
      ,a.xwalk_huc12_version
      ,a.xwalk_huc12_areasqkm
      ,a.globalid
      FROM
      cipsrv_owld.attains_huc12 a
      WHERE
      a.xwalk_catresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_attains_huc12_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_attains_huc12_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_attains_huc12_h';
   
   END IF;

END$$;
