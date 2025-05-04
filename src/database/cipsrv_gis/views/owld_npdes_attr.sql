DROP VIEW IF EXISTS cipsrv_gis.owld_npdes_attr;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','npdes_attr') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_npdes_attr
      AS
      SELECT
       a.objectid
      ,a.source_joinkey
      ,a.external_permit_nmbr
      ,a.permit_name
      ,a.registry_id
      ,a.primary_name
      ,a.state_code
      ,a.agency_type_code
      ,a.issue_date
      ,a.issuing_agency
      ,a.original_issue_date
      ,a.permit_status_code
      ,a.permit_type_code
      ,a.retirement_date
      ,a.termination_date
      ,a.globalid
      FROM
      cipsrv_owld.npdes_attr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_npdes_attr OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_npdes_attr TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_npdes_attr';
   
   END IF;

END$$;

