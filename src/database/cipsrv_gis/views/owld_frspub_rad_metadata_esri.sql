DROP VIEW IF EXISTS cipsrv_gis.owld_frspub_rad_metadata_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','frspub_rad_metadata') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_frspub_rad_metadata_esri
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
      cipsrv_owld.frspub_rad_metadata a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_frspub_rad_metadata_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_frspub_rad_metadata_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_frspub_rad_metadata_esri';
   
   END IF;

END$$;
