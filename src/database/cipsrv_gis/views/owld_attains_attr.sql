DROP VIEW IF EXISTS cipsrv_gis.owld_attains_attr;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','attains_attr') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_attains_attr
      AS
      SELECT
       a.objectid
      ,a.source_joinkey
      ,a.state
      ,a.region
      ,a.organizationid
      ,a.orgtype
      ,a.tas303d
      ,a.organizationname
      ,a.reportingcycle
      ,a.assessmentunitidentifier
      ,a.assessmentunitname
      ,a.waterbodyreportlink
      ,a.ircategory
      ,a.overallstatus
      ,a.isassessed
      ,a.isimpaired
      ,a.isthreatened
      ,a.on303dlist
      ,a.hastmdl
      ,a.has4bplan
      ,a.hasalternativeplan
      ,a.hasprotectionplan
      ,a.visionpriority303d
      ,a.cultural_use
      ,a.drinkingwater_use
      ,a.ecological_use
      ,a.fishconsumption_use
      ,a.recreation_use
      ,a.other_use
      ,a.algal_growth
      ,a.ammonia
      ,a.biotoxins
      ,a.cause_unknown
      ,a.cause_unknown_fish_kills
      ,a.cause_unknown_impaired_biota
      ,a.chlorine
      ,a.dioxins
      ,a.fish_consumption_advisory
      ,a.flow_alterations
      ,a.habitat_alterations
      ,a.hydrologic_alteration
      ,a.mercury
      ,a.metals_other_than_mercury
      ,a.noxious_aquatic_plants
      ,a.nuisance_exotic_species
      ,a.nuisance_native_species
      ,a.nutrients
      ,a.oil_and_grease
      ,a.oxygen_depletion
      ,a.other_cause
      ,a.pathogens
      ,a.pesticides
      ,a.pfas
      ,a.ph_acidity_caustic_conditions
      ,a.polychlorinated_biphenyls_pcbs
      ,a.radiation
      ,a.solids_chlorides_sulfates
      ,a.sediment
      ,a.taste_color_and_odor
      ,a.temperature
      ,a.total_toxics
      ,a.toxic_inorganics
      ,a.toxic_organics
      ,a.trash
      ,a.turbidity
      ,a.globalid
      FROM
      cipsrv_owld.attains_attr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_attains_attr OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_attains_attr TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_attains_attr';
   
   END IF;

END$$;

