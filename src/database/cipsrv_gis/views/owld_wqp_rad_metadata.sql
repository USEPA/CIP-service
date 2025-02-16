DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_metadata;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_owld'
      AND   a.table_name   = 'wqp_rad_metadata'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_metadata
      AS
      SELECT
       a.objectid
      ,a.meta_processid
      ,a.processdescription
      ,a.processdate
      ,a.attributeaccuracyreport
      ,a.logicalconsistencyreport
      ,a.completenessreport
      ,a.horizpositionalaccuracyreport
      ,a.vertpositionalaccuracyreport
      ,a.metadatastandardname
      ,a.metadatastandardversion
      ,a.metadatadate
      ,a.datasetcredit
      ,a.contactorganization
      ,a.addresstype
      ,a.address
      ,a.city
      ,a.stateorprovince
      ,a.postalcode
      ,a.contactvoicetelephone
      ,a.contactinstructions
      ,a.contactemailaddress
      ,a.globalid
      FROM
      cipsrv_owld.wqp_rad_metadata a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_metadata OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_metadata TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_metadata';
   
   END IF;

END$$;
