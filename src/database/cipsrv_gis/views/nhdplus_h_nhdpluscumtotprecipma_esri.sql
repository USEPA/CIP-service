DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdpluscumtotprecipma_esri;

DO $$DECLARE
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdpluscumtotprecipma')
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdpluscumtotprecipma_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.nhdplusid AS NUMERIC)   AS nhdplusid
      ,a.misspcma
      ,a.precipcma
      ,CAST(a.hydroseq AS NUMERIC)  AS hydroseq
      ,a.vpuid
      ,a.globalid
      FROM
      cipsrv_nhdplus_h.nhdpluscumtotprecipma a;
      $q$;

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdpluscumtotprecipma_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdpluscumtotprecipma_esri TO public;
      $q$;

   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdpluscumtotprecipma_esri';

   END IF;

END$$;
