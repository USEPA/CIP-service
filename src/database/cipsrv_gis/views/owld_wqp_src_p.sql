DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src_p;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_owld'
      AND   a.table_name   = 'wqp_src_p'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src_p
      AS
      SELECT
       a.objectid
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_src_p a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_src_p OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_src_p TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_src_p';
   
   END IF;

END$$;
