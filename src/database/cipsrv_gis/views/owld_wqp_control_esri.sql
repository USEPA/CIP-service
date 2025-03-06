DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_control_esri;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_owld'
      AND   a.table_name   = 'wqp_control'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_control_esri
      AS
      SELECT
       (ROW_NUMBER() OVER())::INTEGER AS objectid
      ,a.keyword
      ,a.value_str
      ,a.value_num
      ,a.value_date
      FROM
      cipsrv_owld.wqp_control a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_control_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_control_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_control_esri';
   
   END IF;

END$$;

