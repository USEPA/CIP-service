DROP VIEW IF EXISTS cipsrv_gis.owld_npdes_rad_srccit;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','npdes_rad_srccit') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_npdes_rad_srccit
      AS
      SELECT
       a.objectid
      ,a.title
      ,a.source_datasetid
      ,a.sourcecitationabbreviation
      ,a.originator
      ,a.publicationdate
      ,a.beginningdate
      ,a.endingdate
      ,a.sourcecontribution
      ,a.sourcescaledenominator
      ,a.typeofsourcemedia
      ,a.calendardate
      ,a.sourcecurrentnessreference
      ,a.meta_processid
      ,a.globalid
      FROM
      cipsrv_owld.npdes_rad_srccit a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_npdes_rad_srccit OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_npdes_rad_srccit TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_npdes_rad_srccit';
   
   END IF;

END$$;
