DROP VIEW IF EXISTS cipsrv_gis.owld_frspub_src2cip;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','frspub_src2cip') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_frspub_src2cip
      AS
      SELECT
       a.objectid
      ,a.src2cip_joinkey
      ,a.cip_joinkey
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,a.nhdplusid
      ,a.catchmentresolution
      ,a.cip_action
      ,a.overlap_measure
      ,a.cip_method
      ,a.cip_parms
      ,a.cip_date
      ,a.cip_version
      ,a.globalid
      FROM
      cipsrv_owld.frspub_src2cip a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_frspub_src2cip OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_frspub_src2cip TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_frspub_src2cip';
   
   END IF;

END$$;
