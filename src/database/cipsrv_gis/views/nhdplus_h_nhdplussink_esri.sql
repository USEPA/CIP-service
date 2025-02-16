DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplussink_esri;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_nhdplus_h'
      AND   a.table_name   = 'nhdplussink'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplussink_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.gridcode
      ,a.purpcode
      ,CAST(a.featureid AS NUMERIC) AS featureid
      ,a.sourcefc
      ,a.rpuid
      ,a.statusflag
      ,CAST(a.catchment AS SMALLINT) AS catchment
      ,CAST(a.burn      AS SMALLINT) AS burn
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdplussink a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdplussink_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplussink_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdplussink_esri';
   
   END IF;

END$$;
