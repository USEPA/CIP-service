DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdpluscumtotnlcd2011_esri;

DO $$DECLARE
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdpluscumtotnlcd2011')
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdpluscumtotnlcd2011_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.nhdplusid AS NUMERIC)   AS nhdplusid
      ,a.missdataa
      ,a.nlcd11act
      ,a.nlcd11ac
      ,a.nlcd11pct
      ,a.nlcd11pc
      ,a.nlcd12act
      ,a.nlcd12ac
      ,a.nlcd12pct
      ,a.nlcd12pc
      ,a.nlcd21act
      ,a.nlcd21ac
      ,a.nlcd21pct
      ,a.nlcd21pc
      ,a.nlcd22act
      ,a.nlcd22ac
      ,a.nlcd22pct
      ,a.nlcd22pc
      ,a.nlcd23act
      ,a.nlcd23ac
      ,a.nlcd23pct
      ,a.nlcd23pc
      ,a.nlcd24act
      ,a.nlcd24ac
      ,a.nlcd24pct
      ,a.nlcd24pc
      ,a.nlcd31act
      ,a.nlcd31ac
      ,a.nlcd31pct
      ,a.nlcd31pc
      ,a.nlcd41act
      ,a.nlcd41ac
      ,a.nlcd41pct
      ,a.nlcd41pc
      ,a.nlcd42act
      ,a.nlcd42ac
      ,a.nlcd42pct
      ,a.nlcd42pc
      ,a.nlcd43act
      ,a.nlcd43ac
      ,a.nlcd43pct
      ,a.nlcd43pc
      ,a.nlcd51act
      ,a.nlcd51ac
      ,a.nlcd51pct
      ,a.nlcd51pc
      ,a.nlcd52act
      ,a.nlcd52ac
      ,a.nlcd52pct
      ,a.nlcd52pc
      ,a.nlcd71act
      ,a.nlcd71ac
      ,a.nlcd71pct
      ,a.nlcd71pc
      ,a.nlcd72act
      ,a.nlcd72ac
      ,a.nlcd72pct
      ,a.nlcd72pc
      ,a.nlcd73act
      ,a.nlcd73ac
      ,a.nlcd73pct
      ,a.nlcd73pc
      ,a.nlcd74act
      ,a.nlcd74ac
      ,a.nlcd74pct
      ,a.nlcd74pc
      ,a.nlcd81act
      ,a.nlcd81ac
      ,a.nlcd81pct
      ,a.nlcd81pc
      ,a.nlcd82act
      ,a.nlcd82ac
      ,a.nlcd82pct
      ,a.nlcd82pc
      ,a.nlcd90act
      ,a.nlcd90ac
      ,a.nlcd90pct
      ,a.nlcd90pc
      ,a.nlcd95act
      ,a.nlcd95ac
      ,a.nlcd95pct
      ,a.nlcd95pc
      ,CAST(a.hydroseq AS NUMERIC)  AS hydroseq
      ,a.vpuid
      ,a.globalid
      FROM
      cipsrv_nhdplus_h.nhdpluscumtotnlcd2011 a;
      $q$;

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdpluscumtotnlcd2011_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdpluscumtotnlcd2011_esri TO public;
      $q$;

   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdpluscumtotnlcd2011_esri';

   END IF;

END$$;
