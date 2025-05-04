DROP VIEW IF EXISTS cipsrv_gis.owld_npdes_control;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','npdes_control') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_npdes_control
      AS
      SELECT
       (ROW_NUMBER() OVER())::INTEGER AS objectid
      ,a.keyword
      ,a.value_str
      ,a.value_num
      ,a.value_date
      FROM
      cipsrv_owld.npdes_control a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_npdes_control OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_npdes_control TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_npdes_control';
   
   END IF;

END$$;

