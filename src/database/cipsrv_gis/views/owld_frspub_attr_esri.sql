DROP VIEW IF EXISTS cipsrv_gis.owld_frspub_attr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','frspub_attr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_frspub_attr_esri
      AS
      SELECT
       a.objectid
      ,a.source_joinkey
      ,a.registry_id
      ,a.primary_name
      ,a.city_name
      ,a.county_name
      ,a.fips_code
      ,a.state_code
      ,a.state_name
      ,a.country_name
      ,a.postal_code
      ,a.tribal_land_code
      ,a.tribal_land_name
      ,a.us_mexico_border_ind
      ,a.pgm_sys_id
      ,a.pgm_sys_acrnm
      ,a.globalid
      FROM
      cipsrv_owld.frspub_attr a
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_frspub_attr_esri OWNER TO cipsrv_gis
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_frspub_attr_esri TO public
      $q$;
      
   ELSE
      RAISE WARNING 'skipping owld_frspub_attr_esri';
   
   END IF;

END$$;
