DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdplusflow_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdplusflow') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdplusflow_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.fromnhdpid AS NUMERIC)  AS fromnhdpid
      ,CAST(a.tonhdpid   AS NUMERIC)  AS tonhdpid
      ,CAST(a.nodenumber AS NUMERIC)  AS nodenumber
      ,CAST(a.deltalevel AS SMALLINT) AS deltalevel
      ,CAST(a.direction  AS SMALLINT) AS direction
      ,a.gapdistkm
      ,CAST(a.hasgeo     AS SMALLINT) AS hasgeo
      ,a.fromvpuid
      ,a.tovpuid
      ,a.frompermid
      ,a.topermid
      ,CAST(a.fromhydroseq AS NUMERIC)  AS fromhydroseq
      ,CAST(a.tohydroseq   AS NUMERIC)  AS tohydroseq
      ,a.globalid
      FROM
      cipsrv_nhdplus_m.nhdplusflow a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdplusflow_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdplusflow_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdplusflow_esri';
   
   END IF;

END$$;
