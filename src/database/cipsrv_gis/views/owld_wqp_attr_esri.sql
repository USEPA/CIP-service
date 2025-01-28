DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_attr_esri;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_owld'
      AND   a.table_name   = 'wqp_attr'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_attr_esri
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
      cipsrv_owld.wqp_attr a
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_attr_esri OWNER TO cipsrv_gis
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_attr_esri TO public
      $q$;
      
   ELSE
      RAISE WARNING 'skipping owld_wqp_attr_esri';
   
   END IF;

END$$;
