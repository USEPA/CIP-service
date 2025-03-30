DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_evt2meta_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_evt2meta') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_evt2meta_m
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.meta_processid
      ,a.globalid
      FROM
      cipsrv_owld.wqp_rad_evt2meta a
      WHERE
      a.reachresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_evt2meta_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_evt2meta_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_evt2meta_m';
   
   END IF;

END$$;
