DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12sp_f3;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12sp_f3') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12sp_f3
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
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12sp_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12sp_f3 OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12sp_f3 TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12sp_f3';
   
   END IF;

END$$;
