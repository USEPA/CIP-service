DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplusincrnlcd2011_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdplusincrnlcd2011') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplusincrnlcd2011_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.nhdplusid AS NUMERIC)   AS nhdplusid
      ,a.missdataa
      ,a.nlcd11at
      ,a.nlcd11a
      ,a.nlcd11pt
      ,a.nlcd11p
      ,a.nlcd12at
      ,a.nlcd12a
      ,a.nlcd12pt
      ,a.nlcd12p
      ,a.nlcd21at
      ,a.nlcd21a
      ,a.nlcd21pt
      ,a.nlcd21p
      ,a.nlcd22at
      ,a.nlcd22a
      ,a.nlcd22pt
      ,a.nlcd22p
      ,a.nlcd23at
      ,a.nlcd23a
      ,a.nlcd23pt
      ,a.nlcd23p
      ,a.nlcd24at
      ,a.nlcd24a
      ,a.nlcd24pt
      ,a.nlcd24p
      ,a.nlcd31at
      ,a.nlcd31a
      ,a.nlcd31pt
      ,a.nlcd31p
      ,a.nlcd41at
      ,a.nlcd41a
      ,a.nlcd41pt
      ,a.nlcd41p
      ,a.nlcd42at
      ,a.nlcd42a
      ,a.nlcd42pt
      ,a.nlcd42p
      ,a.nlcd43at
      ,a.nlcd43a
      ,a.nlcd43pt
      ,a.nlcd43p
      ,a.nlcd51at
      ,a.nlcd51a
      ,a.nlcd51pt
      ,a.nlcd51p
      ,a.nlcd52at
      ,a.nlcd52a
      ,a.nlcd52pt
      ,a.nlcd52p
      ,a.nlcd71at
      ,a.nlcd71a
      ,a.nlcd71pt
      ,a.nlcd71p
      ,a.nlcd72at
      ,a.nlcd72a
      ,a.nlcd72pt
      ,a.nlcd72p
      ,a.nlcd73at
      ,a.nlcd73a
      ,a.nlcd73pt
      ,a.nlcd73p
      ,a.nlcd74at
      ,a.nlcd74a
      ,a.nlcd74pt
      ,a.nlcd74p
      ,a.nlcd81at
      ,a.nlcd81a
      ,a.nlcd81pt
      ,a.nlcd81p
      ,a.nlcd82at
      ,a.nlcd82a
      ,a.nlcd82pt
      ,a.nlcd82p
      ,a.nlcd90at
      ,a.nlcd90a
      ,a.nlcd90pt
      ,a.nlcd90p
      ,a.nlcd95at
      ,a.nlcd95a
      ,a.nlcd95pt
      ,a.nlcd95p
      ,CAST(a.hydroseq AS NUMERIC)  AS hydroseq
      ,a.vpuid
      ,a.globalid
      FROM
      cipsrv_nhdplus_h.nhdplusincrnlcd2011 a;
      $q$;

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdplusincrnlcd2011_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplusincrnlcd2011_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdplusincrnlcd2011_esri';
   
   END IF;

END$$;
