DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_wbdhu12_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','wbdhu12') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_wbdhu12_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.sourcedatadesc
      ,a.sourceoriginator
      ,a.sourcefeatureid
      ,a.loaddate
      ,a.referencegnis_ids
      ,a.areaacres
      ,a.areasqkm
      ,a.states
      ,a.huc12
      ,a.name
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.wbdhu12 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_wbdhu12_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_wbdhu12_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_wbdhu12_esri';
   
   END IF;

END$$;

