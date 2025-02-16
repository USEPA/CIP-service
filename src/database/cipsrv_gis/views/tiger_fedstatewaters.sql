DROP VIEW IF EXISTS cipsrv_gis.tiger_fedstatewaters;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_support'
      AND   a.table_name   = 'tiger_fedstatewaters'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.tiger_fedstatewaters
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
      cipsrv_support.tiger_fedstatewaters a
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.tiger_fedstatewaters_esri OWNER TO cipsrv_gis
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.tiger_fedstatewaters TO public
      $q$;
      
   ELSE
      RAISE WARNING 'skipping tiger_fedstatewaters';
   
   END IF;

END$$;
