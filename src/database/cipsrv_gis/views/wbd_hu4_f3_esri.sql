DROP VIEW IF EXISTS cipsrv_gis.wbd_hu4_f3_esri;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_wbd'
      AND   a.table_name   = 'wbd_hu4_f3'
   )
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu4_np21_f3
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
      ,a.huc4
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu4_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu4_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu4_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu4_f3_esri';
   
   END IF;

END$$;
