DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_attr;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_attr') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_attr
      AS
      SELECT
       a.objectid
      ,a.source_joinkey
      ,a.organizationidentifier
      ,a.organizationformalname
      ,a.monitoringlocationidentifier
      ,a.monitoringlocationname
      ,a.monitoringlocationtypename
      ,a.monitoringlocationdescription
      ,a.huceightdigitcode
      ,a.drainageareameasure_measureval
      ,a.drainageareameasure_measureunt
      ,a.contributingdrainageareameasva
      ,a.contributingdrainageareameasun
      ,a.latitudemeasure
      ,a.longitudemeasure
      ,a.sourcemapscalenumeric
      ,a.horizontalaccuracymeasureval
      ,a.horizontalaccuracymeasureunit
      ,a.horizontalcollectionmethodname
      ,a.horizontalcoordinatereferences
      ,a.verticalmeasure_measurevalue
      ,a.verticalmeasure_measureunit
      ,a.verticalaccuracymeasurevalue
      ,a.verticalaccuracymeasureunit
      ,a.verticalcollectionmethodname
      ,a.verticalcoordinatereferencesys
      ,a.countrycode
      ,a.statecode
      ,a.countycode
      ,a.aquifername
      ,a.formationtypetext
      ,a.aquifertypename
      ,a.localaqfrname
      ,a.constructiondatetext
      ,a.welldepthmeasure_measurevalue
      ,a.welldepthmeasure_measureunit
      ,a.wellholedepthmeasure_measureva
      ,a.wellholedepthmeasure_measureun
      ,a.providername
      ,a.globalid
      FROM
      cipsrv_owld.wqp_attr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_attr OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_attr TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_attr';
   
   END IF;

END$$;

