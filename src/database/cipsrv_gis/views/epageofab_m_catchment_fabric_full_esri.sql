DROP VIEW IF EXISTS cipsrv_gis.epageofab_m_catchment_fabric_full_esri;

DO $$DECLARE 
BEGIN

   IF  cipsrv_gis.resource_exists('cipsrv_nhdplus_m','catchment_5070')
   AND cipsrv_gis.resource_exists('cipsrv_nhdplus_m','catchment_3338')
   AND cipsrv_gis.resource_exists('cipsrv_nhdplus_m','catchment_26904')
   AND cipsrv_gis.resource_exists('cipsrv_nhdplus_m','catchment_32161')
   AND cipsrv_gis.resource_exists('cipsrv_nhdplus_m','catchment_32655')
   AND cipsrv_gis.resource_exists('cipsrv_nhdplus_m','catchment_32702')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric_full_esri
      AS
      SELECT
       CAST(ROW_NUMBER() OVER() AS INTEGER) AS objectid
      ,CAST(NULL AS VARCHAR(2))      AS catchmentstatecode
      ,CAST(a.nhdplusid AS NUMERIC)  AS nhdplusid
      ,a.istribal
      ,a.istribal_areasqkm
      ,a.areasqkm
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,CAST(a.state_count AS SMALLINT) AS state_count
      ,a.vpuid
      ,ST_TRANSFORM(a.shape,4269) AS shape
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.istribal
         ,aa.istribal_areasqkm
         ,aa.areasqkm
         ,aa.isnavigable
         ,aa.hasvaa
         ,aa.issink
         ,aa.isheadwater
         ,aa.iscoastal
         ,aa.isocean
         ,aa.isalaskan
         ,aa.h3hexagonaddr
         ,ARRAY_LENGTH(aa.catchmentstatecodes,1) AS state_count
         ,aa.vpuid
         ,aa.shape
         FROM
         cipsrv_nhdplus_m.catchment_5070 aa
         WHERE
         aa.statesplit IN (0,2)
         UNION ALL
         SELECT
          bb.nhdplusid
         ,bb.istribal
         ,bb.istribal_areasqkm
         ,bb.areasqkm
         ,bb.isnavigable
         ,bb.hasvaa
         ,bb.issink
         ,bb.isheadwater
         ,bb.iscoastal
         ,bb.isocean
         ,bb.isalaskan
         ,bb.h3hexagonaddr
         ,ARRAY_LENGTH(bb.catchmentstatecodes,1) AS state_count
         ,bb.vpuid
         ,bb.shape
         FROM
         cipsrv_nhdplus_m.catchment_3338 bb
         WHERE
         bb.statesplit IN (0,2)
         UNION ALL
         SELECT
          cc.nhdplusid
         ,cc.istribal
         ,cc.istribal_areasqkm
         ,cc.areasqkm
         ,cc.isnavigable
         ,cc.hasvaa
         ,cc.issink
         ,cc.isheadwater
         ,cc.iscoastal
         ,cc.isocean
         ,cc.isalaskan
         ,cc.h3hexagonaddr
         ,ARRAY_LENGTH(cc.catchmentstatecodes,1) AS state_count
         ,cc.vpuid
         ,cc.shape
         FROM
         cipsrv_nhdplus_m.catchment_26904 cc
         WHERE
         cc.statesplit IN (0,2)
         UNION ALL
         SELECT
          dd.nhdplusid
         ,dd.istribal
         ,dd.istribal_areasqkm
         ,dd.areasqkm
         ,dd.isnavigable
         ,dd.hasvaa
         ,dd.issink
         ,dd.isheadwater
         ,dd.iscoastal
         ,dd.isocean
         ,dd.isalaskan
         ,dd.h3hexagonaddr
         ,ARRAY_LENGTH(dd.catchmentstatecodes,1) AS state_count
         ,dd.vpuid
         ,dd.shape
         FROM
         cipsrv_nhdplus_m.catchment_32161 dd
         WHERE
         dd.statesplit IN (0,2)
         UNION ALL
         SELECT
          ee.nhdplusid
         ,ee.istribal
         ,ee.istribal_areasqkm
         ,ee.areasqkm
         ,ee.isnavigable
         ,ee.hasvaa
         ,ee.issink
         ,ee.isheadwater
         ,ee.iscoastal
         ,ee.isocean
         ,ee.isalaskan
         ,ee.h3hexagonaddr
         ,ARRAY_LENGTH(ee.catchmentstatecodes,1) AS state_count
         ,ee.vpuid
         ,ee.shape
         FROM
         cipsrv_nhdplus_m.catchment_32655 ee
         WHERE
         ee.statesplit IN (0,2)
         UNION ALL
         SELECT
          ff.nhdplusid
         ,ff.istribal
         ,ff.istribal_areasqkm
         ,ff.areasqkm
         ,ff.isnavigable
         ,ff.hasvaa
         ,ff.issink
         ,ff.isheadwater
         ,ff.iscoastal
         ,ff.isocean
         ,ff.isalaskan
         ,ff.h3hexagonaddr
         ,ARRAY_LENGTH(ff.catchmentstatecodes,1) AS state_count
         ,ff.vpuid
         ,ff.shape
         FROM
         cipsrv_nhdplus_m.catchment_32702 ff
         WHERE
         ff.statesplit IN (0,2)
      ) a
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric_full_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric_full_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_m_catchment_fabric_full_esri';
   
   END IF;

END$$;
