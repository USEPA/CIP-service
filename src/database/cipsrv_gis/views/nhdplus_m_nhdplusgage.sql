DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdplusgage;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdplusgage') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdplusgage
      AS
      SELECT
       a.objectid
      ,a.hydroaddressid
      ,a.addressdate
      ,a.featuretype
      ,a.onnetwork
      ,a.sourceid
      ,a.sourceagency
      ,a.sourcedataset
      ,a.sourcefeatureurl
      ,a.catchment
      ,a.hu
      ,a.reachcode
      ,a.measure
      ,a.reachsmdate
      ,a.nhdplusid
      ,a.vpuid
      ,a.station_nm
      ,a.state_cd
      ,a.state
      ,a.latsite
      ,a.lonsite
      ,a.dasqmi
      ,a.referencegage
      ,a.class
      ,a.classmod
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdplusgage a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdplusgage OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdplusgage TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdplusgage';
   
   END IF;

END$$;
