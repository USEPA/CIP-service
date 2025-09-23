DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdplusincrtempma_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdplusincrtempma') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdplusincrtempma_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.nhdplusid AS NUMERIC)   AS nhdplusid
      ,a.misstma
      ,a.tempma
      ,CAST(a.hydroseq AS NUMERIC)  AS hydroseq
      ,a.vpuid
      ,a.globalid
      FROM
      cipsrv_nhdplus_m.nhdplusincrtempma a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdplusincrtempma_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdplusincrtempma_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdplusincrtempma_esri';
   
   END IF;

END$$;
