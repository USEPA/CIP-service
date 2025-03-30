DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_l_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_l') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_l_m
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.eventdate
      ,a.reachcode
      ,a.reachsmdate
      ,a.reachresolution
      ,a.feature_permanent_identifier
      ,a.featureclassref
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_datadesc
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.fmeasure
      ,a.tmeasure
      ,a.eventtype
      ,a.eventoffset
      ,a.event_lengthkm
      ,a.geogstate
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
      ,a.shape
      FROM
      cipsrv_owld.wqp_rad_l a
      WHERE
      a.reachresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_l_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_l_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_l_m';
   
   END IF;

END$$;
