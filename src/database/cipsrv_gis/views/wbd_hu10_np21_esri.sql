DROP VIEW IF EXISTS cipsrv_gis.wbd_hu10_np21_esri;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_wbd'
      AND   a.table_name   = 'wbd_hu10_np21'
   )
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu10_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc10
      ,a.hutype
      ,a.humod
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu10_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu10_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu10_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu10_np21_esri';
   
   END IF;

END$$;
