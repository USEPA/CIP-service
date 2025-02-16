DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplusgage;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_nhdplus_h'
      AND   a.table_name   = 'nhdplusgage'
   ) 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplusgage
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
      cipsrv_nhdplus_h.nhdplusgage a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdplusgage OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplusgage TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdplusgage';
   
   END IF;

END$$;

