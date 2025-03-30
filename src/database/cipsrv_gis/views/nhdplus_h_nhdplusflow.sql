DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplusflow;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdplusflow') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplusflow
      AS
      SELECT
       a.objectid
      ,a.fromnhdpid
      ,a.tonhdpid
      ,a.nodenumber
      ,a.deltalevel
      ,a.direction
      ,a.gapdistkm
      ,a.hasgeo
      ,a.fromvpuid
      ,a.tovpuid
      ,a.frompermid
      ,a.topermid
      ,a.fromhydroseq
      ,a.tohydroseq
      ,a.globalid
      FROM
      cipsrv_nhdplus_h.nhdplusflow a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdplusflow OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplusflow TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdplusflow';
   
   END IF;

END$$;

