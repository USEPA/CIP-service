DROP VIEW IF EXISTS cipsrv_gis.tiger_fedstatewaters_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_support','tiger_fedstatewaters')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.tiger_fedstatewaters_esri
      AS
      SELECT
       a.objectid
      ,a.statens
      ,a.geoid
      ,a.stusps
      ,a.name
      ,a.aland
      ,a.awater
      ,a.intptlat
      ,a.intptlon
      ,a.shape
      FROM
      cipsrv_support.tiger_fedstatewaters a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.tiger_fedstatewaters_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.tiger_fedstatewaters_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping tiger_fedstatewaters_esri';
   
   END IF;

END$$;
