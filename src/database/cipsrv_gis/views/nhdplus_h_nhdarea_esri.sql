DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdarea_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdarea') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdarea_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.areasqkm
      ,a.elevation
      ,CAST(a.ftype AS INTEGER)  AS ftype
      ,CAST(a.fcode AS INTEGER)  AS fcode
      ,a.visibilityfilter
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,CAST(a.onoffnet AS SMALLINT) AS onoffnet 
      ,a.purpcode
      ,CAST(a.burn     AS SMALLINT) AS burn
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdarea a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdarea_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdarea_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdarea_esri';
   
   END IF;

END$$;
