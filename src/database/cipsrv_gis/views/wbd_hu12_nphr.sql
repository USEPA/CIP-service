DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12_nphr;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_support'
      AND   a.table_name   = 'wbd_hu12_nphr'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12_nphr
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
      cipsrv_support.wbd_hu12_nphr a
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12_nphr OWNER TO cipsrv_gis
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12_nphr TO public
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12_nphr';
   
   END IF;

END$$;
