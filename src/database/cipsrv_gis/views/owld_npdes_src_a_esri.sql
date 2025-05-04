DROP VIEW IF EXISTS cipsrv_gis.owld_npdes_src_a_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','npdes_src_a') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_npdes_src_a_esri
      AS
      SELECT
       a.objectid
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.npdes_src_a a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_npdes_src_a_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_npdes_src_a_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_npdes_src_a';
   
   END IF;

END$$;

