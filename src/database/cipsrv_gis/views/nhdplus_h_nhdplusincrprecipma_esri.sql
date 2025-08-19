DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplusincrprecipma_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdplusincrprecipma') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplusincrprecipma_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.nhdplusid AS NUMERIC)   AS nhdplusid
      ,a.misspma
      ,a.precipma
      ,CAST(a.hydroseq AS NUMERIC)  AS hydroseq
      ,a.vpuid
      ,a.globalid
      FROM
      cipsrv_nhdplus_h.nhdplusincrprecipma a;
      $q$;

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdplusincrprecipma_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplusincrprecipma_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdplusincrprecipma_esri';
   
   END IF;

END$$;
