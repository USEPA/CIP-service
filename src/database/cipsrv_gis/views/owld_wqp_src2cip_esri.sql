DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src2cip_esri;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_owld'
      AND   a.table_name   = 'wqp_src2cip'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src2cip_esri
      AS
      SELECT
       a.objectid
      ,a.src2cip_joinkey
      ,a.cip_joinkey
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.catchmentresolution
      ,a.cip_action
      ,a.overlap_measure
      ,a.cip_method
      ,a.cip_parms
      ,a.cip_date
      ,a.cip_version
      ,a.globalid
      FROM
      cipsrv_owld.wqp_src2cip a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_src2cip_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_src2cip_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_src2cip';
   
   END IF;

END$$;
