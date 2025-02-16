DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdplussink;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_nhdplus_m'
      AND   a.table_name   = 'nhdplussink'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdplussink
      AS
      SELECT
       a.objectid
      ,a.nhdplusid
      ,a.gridcode
      ,a.purpcode
      ,a.featureid
      ,a.sourcefc
      ,a.rpuid
      ,a.statusflag
      ,a.catchment
      ,a.burn
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdplussink a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdplussink OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdplussink TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdplussink';
   
   END IF;

END$$;
