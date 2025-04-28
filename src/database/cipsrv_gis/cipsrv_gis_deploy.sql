--******************************--
----- functions/resource_exists.sql 

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_gis.resource_exists';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_gis.resource_exists(
    IN  p_schema_name VARCHAR
   ,IN  p_table_name  VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_table_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for table
   ----------------------------------------------------------------------------
   SELECT 
   c.relname
   INTO str_table_name
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   WHERE  
       n.nspname = p_schema_name
   AND c.relname = p_table_name
   AND c.relkind IN ('r','m','v','p');

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_table_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ 
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_gis.resource_exists';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv_gis',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv_gis',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;


--******************************--
----- views/epageofab_h_catchment_fabric.sql 

DROP VIEW IF EXISTS cipsrv_gis.epageofab_h_catchment_fabric;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_h','catchment_fabric') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_h_catchment_fabric
      AS
      SELECT
       a.objectid
      ,a.catchmentstatecode
      ,a.nhdplusid
      ,a.istribal
      ,a.istribal_areasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.sourcefc
      ,a.gridcode
      ,a.areasqkm
      ,a.areasqkm_geo
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.state_count
      ,a.vpuid
      ,a.sourcedataset
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_h.catchment_fabric a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_h_catchment_fabric OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_h_catchment_fabric TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_h_catchment_fabric';
   
   END IF;

END$$;
--******************************--
----- views/epageofab_h_catchment_fabric_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.epageofab_h_catchment_fabric_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_h','catchment_fabric') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_h_catchment_fabric_esri
      AS
      SELECT
       a.objectid
      ,a.catchmentstatecode
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.istribal
      ,a.istribal_areasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.sourcefc
      ,a.gridcode
      ,a.areasqkm
      ,a.areasqkm_geo
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
      ,a.sourcedataset
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_h.catchment_fabric a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_h_catchment_fabric_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_h_catchment_fabric_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_h_catchment_fabric_esri';
   
   END IF;

END$$;
--******************************--
----- views/epageofab_h_catchment_fabric_huc12_np21.sql 

DROP VIEW IF EXISTS cipsrv_gis.epageofab_h_catchment_fabric_huc12_np21;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_h','catchment_fabric_huc12_np21') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_h_catchment_fabric_huc12_np21
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_huc12_version
      ,a.areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_h.catchment_fabric_huc12_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_h_catchment_fabric_huc12_np21 OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_h_catchment_fabric_huc12_np21 TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21';
   
   END IF;

END$$;
--******************************--
----- views/epageofab_h_catchment_fabric_huc12_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.epageofab_h_catchment_fabric_huc12_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_h','catchment_fabric_huc12_np21') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_h_catchment_fabric_huc12_np21_esri
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_huc12_version
      ,a.areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_h.catchment_fabric_huc12_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_h_catchment_fabric_huc12_np21_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_h_catchment_fabric_huc12_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_h_catchment_fabric_huc12_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/epageofab_m_catchment_fabric.sql 

DROP VIEW IF EXISTS cipsrv_gis.epageofab_m_catchment_fabric;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_m','catchment_fabric') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric
      AS
      SELECT
       a.objectid
      ,a.catchmentstatecode
      ,a.nhdplusid
      ,a.istribal
      ,a.istribal_areasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.sourcefc
      ,a.gridcode
      ,a.areasqkm
      ,a.areasqkm_geo
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.state_count
      ,a.vpuid
      ,a.sourcedataset
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_m.catchment_fabric a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_m_catchment_fabric';
   
   END IF;

END$$;
--******************************--
----- views/epageofab_m_catchment_fabric_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.epageofab_m_catchment_fabric_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_m','catchment_fabric') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric_esri
      AS
      SELECT
       a.objectid
      ,a.catchmentstatecode
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.istribal
      ,a.istribal_areasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.sourcefc
      ,a.gridcode
      ,a.areasqkm
      ,a.areasqkm_geo
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
      ,a.sourcedataset
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_m.catchment_fabric a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_m_catchment_fabric_esri';
   
   END IF;

END$$;
--******************************--
----- views/epageofab_m_catchment_fabric_huc12_np21.sql 

DROP VIEW IF EXISTS cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_m','catchment_fabric_huc12_np21') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_huc12_version
      ,a.areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_huc12_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21 OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21 TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21';
   
   END IF;

END$$;
--******************************--
----- views/epageofab_m_catchment_fabric_huc12_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_epageofab_m','catchment_fabric_huc12_np21') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21_esri
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_huc12_version
      ,a.areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_epageofab_m.catchment_fabric_huc12_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_flow_direction.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_flow_direction;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','networknhdflowline') 
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_flow_direction
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,CAST(DEGREES(
          ST_Azimuth(
             a.penul_point
            ,a.final_point
          )
       ) AS NUMERIC) AS angle_direction
      ,CAST(ROUND(DEGREES(
          ST_Azimuth(
             a.penul_point
            ,a.final_point
          )
       ) / 5 ) * 5 AS NUMERIC ) AS angle_rounded
      ,a.ftype
      ,a.fcode
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.final_point AS shape
      FROM (
         SELECT  
          aa.objectid
         ,aa.permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,ST_PointN(aa.shape,-2) AS penul_point
         ,ST_PointN(aa.shape,-1) AS final_point
         FROM (
            SELECT
             aaa.objectid
            ,aaa.permanent_identifier
            ,aaa.ftype
            ,aaa.fcode
            ,aaa.visibilityfilter
            ,aaa.nhdplusid
            ,ST_Force2D(aaa.shape) AS shape
            FROM
            cipsrv_nhdplus_h.networknhdflowline aaa
         ) aa
      ) a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_flow_direction OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_flow_direction TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_flow_direction';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_flow_direction_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_flow_direction_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','networknhdflowline') 
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_flow_direction_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,CAST(DEGREES(
          ST_Azimuth(
             a.penul_point
            ,a.final_point
          )
       ) AS NUMERIC) AS angle_direction
      ,CAST(ROUND(DEGREES(
          ST_Azimuth(
             a.penul_point
            ,a.final_point
          )
       ) / 5 ) * 5 AS NUMERIC ) AS angle_rounded
      ,CAST(a.ftype      AS INTEGER)  AS ftype
      ,CAST(a.fcode      AS INTEGER)  AS fcode
      ,a.visibilityfilter
      ,CAST(a.nhdplusid  AS NUMERIC) AS nhdplusid
      ,a.final_point AS shape
      FROM (
         SELECT  
          aa.objectid
         ,aa.permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,ST_PointN(aa.shape,-2) AS penul_point
         ,ST_PointN(aa.shape,-1) AS final_point
         FROM (
            SELECT
             aaa.objectid
            ,aaa.permanent_identifier
            ,aaa.ftype
            ,aaa.fcode
            ,aaa.visibilityfilter
            ,aaa.nhdplusid
            ,ST_Force2D(aaa.shape) AS shape
            FROM
            cipsrv_nhdplus_h.networknhdflowline aaa
         ) aa
      ) a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_flow_direction_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_flow_direction_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_flow_direction_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_networknhdflowline.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_networknhdflowline;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','networknhdflowline') 
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_networknhdflowline
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.totma
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.streamleve
      ,a.streamorde
      ,a.streamcalc
      ,a.fromnode
      ,a.tonode
      ,a.hydroseq
      ,a.levelpathi
      ,a.pathlength
      ,a.pathtimema
      ,a.terminalpa
      ,a.arbolatesu
      ,a.divergence
      ,a.startflag
      ,a.terminalfl
      ,a.uplevelpat
      ,a.uphydroseq
      ,a.dnlevel
      ,a.dnlevelpat
      ,a.dnhydroseq
      ,a.dnminorhyd
      ,a.dndraincou
      ,a.frommeas
      ,a.tomeas
      ,a.rtndiv
      ,a.thinner
      ,a.vpuin
      ,a.vpuout
      ,a.areasqkm
      ,a.totdasqkm
      ,a.divdasqkm
      ,a.maxelevraw
      ,a.minelevraw
      ,a.maxelevsmo
      ,a.minelevsmo
      ,a.slope
      ,a.slopelenkm
      ,a.elevfixed
      ,a.hwtype
      ,a.hwnodesqkm
      ,a.statusflag
      ,a.qama
      ,a.vama
      ,a.qincrama
      ,a.qbma
      ,a.vbma
      ,a.qincrbma
      ,a.qcma
      ,a.vcma
      ,a.qincrcma
      ,a.qdma
      ,a.vdma
      ,a.qincrdma
      ,a.qema
      ,a.vema
      ,a.qincrema
      ,a.qfma
      ,a.qincrfma
      ,a.arqnavma
      ,a.petma
      ,a.qlossma
      ,a.qgadjma
      ,a.qgnavma
      ,a.gageadjma
      ,a.avgqadjma
      ,a.gageidma
      ,a.gageqma
      ,a.globalid
      ,ST_Force2D(a.shape) AS shape
      FROM
      cipsrv_nhdplus_h.networknhdflowline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_networknhdflowline OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_networknhdflowline TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_networknhdflowline';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_networknhdflowline_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_networknhdflowline_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','networknhdflowline') 
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_networknhdflowline_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.totma
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,CAST(a.ftype      AS INTEGER)  AS ftype
      ,CAST(a.fcode      AS INTEGER)  AS fcode
      ,a.mainpath
      ,CAST(a.innetwork  AS SMALLINT) AS innetwork
      ,a.visibilityfilter
      ,CAST(a.nhdplusid  AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,CAST(a.streamleve AS SMALLINT) AS streamleve
      ,CAST(a.streamorde AS SMALLINT) AS streamorde
      ,CAST(a.streamcalc AS SMALLINT) AS streamcalc
      ,CAST(a.fromnode   AS NUMERIC)  AS fromnode
      ,CAST(a.tonode     AS NUMERIC)  AS tonode
      ,CAST(a.hydroseq   AS NUMERIC)  AS hydroseq
      ,CAST(a.levelpathi AS NUMERIC)  AS levelpathi
      ,a.pathlength
      ,a.pathtimema
      ,CAST(a.terminalpa AS NUMERIC)  AS terminalpa
      ,a.arbolatesu
      ,CAST(a.divergence AS SMALLINT) AS divergence
      ,CAST(a.startflag  AS SMALLINT) AS startflag
      ,CAST(a.terminalfl AS SMALLINT) AS terminalfl
      ,CAST(a.uplevelpat AS NUMERIC)  AS uplevelpat
      ,CAST(a.uphydroseq AS NUMERIC)  AS uphydroseq
      ,CAST(a.dnlevel    AS SMALLINT) AS dnlevel
      ,CAST(a.dnlevelpat AS NUMERIC)  AS dnlevelpat
      ,CAST(a.dnhydroseq AS NUMERIC)  AS dnhydroseq
      ,CAST(a.dnminorhyd AS NUMERIC)  AS dnminorhyd
      ,CAST(a.dndraincou AS SMALLINT) AS dndraincou
      ,a.frommeas
      ,a.tomeas
      ,CAST(a.rtndiv     AS SMALLINT) AS rtndiv
      ,CAST(a.thinner    AS SMALLINT) AS thinner
      ,CAST(a.vpuin      AS SMALLINT) AS vpuin
      ,CAST(a.vpuout     AS SMALLINT) AS vpuout
      ,a.areasqkm
      ,a.totdasqkm
      ,a.divdasqkm
      ,a.maxelevraw
      ,a.minelevraw
      ,a.maxelevsmo
      ,a.minelevsmo
      ,a.slope
      ,a.slopelenkm
      ,CAST(a.elevfixed  AS SMALLINT) AS elevfixed
      ,CAST(a.hwtype     AS SMALLINT) AS hwtype
      ,a.hwnodesqkm
      ,a.statusflag
      ,a.qama
      ,a.vama
      ,a.qincrama
      ,a.qbma
      ,a.vbma
      ,a.qincrbma
      ,a.qcma
      ,a.vcma
      ,a.qincrcma
      ,a.qdma
      ,a.vdma
      ,a.qincrdma
      ,a.qema
      ,a.vema
      ,a.qincrema
      ,a.qfma
      ,a.qincrfma
      ,a.arqnavma
      ,a.petma
      ,a.qlossma
      ,a.qgadjma
      ,a.qgnavma
      ,CAST(a.gageadjma  AS SMALLINT) AS gageadjma
      ,a.avgqadjma
      ,a.gageidma
      ,a.gageqma
      ,a.globalid
      ,ST_Force2D(a.shape) AS shape
      FROM
      cipsrv_nhdplus_h.networknhdflowline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_networknhdflowline_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_networknhdflowline_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_networknhdflowline_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nhdarea.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdarea;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdarea') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdarea
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
      ,a.ftype
      ,a.fcode
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.onoffnet
      ,a.purpcode
      ,a.burn
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdarea a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdarea OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdarea TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdarea';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nhdarea_esri.sql 

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
--******************************--
----- views/nhdplus_h_nhdline.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdline;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdline') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdline
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.ftype
      ,a.fcode
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdline OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdline TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdline';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nhdline_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdline_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdline') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdline_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,CAST(a.ftype AS INTEGER)  AS ftype
      ,CAST(a.fcode AS INTEGER)  AS fcode
      ,a.visibilityfilter
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdline_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdline_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdline_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nhdpluscatchment.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdpluscatchment;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdpluscatchment') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdpluscatchment
      AS
      SELECT
       a.objectid
      ,a.nhdplusid
      ,a.sourcefc
      ,a.gridcode
      ,a.areasqkm
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdpluscatchment a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdpluscatchment OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdpluscatchment TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdpluscatchment';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nhdpluscatchment_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdpluscatchment_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdpluscatchment') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdpluscatchment_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.sourcefc
      ,a.gridcode
      ,a.areasqkm
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdpluscatchment a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdpluscatchment_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdpluscatchment_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdpluscatchment_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nhdplusflow.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplusflow;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdplusflow') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplusflow
      AS
      SELECT
       a.objectid
      ,a.fromnhdpid
      ,a.tonhdpid
      ,a.nodenumber
      ,a.deltalevel
      ,a.direction
      ,a.gapdistkm
      ,a.hasgeo
      ,a.fromvpuid
      ,a.tovpuid
      ,a.frompermid
      ,a.topermid
      ,a.fromhydroseq
      ,a.tohydroseq
      ,a.globalid
      FROM
      cipsrv_nhdplus_h.nhdplusflow a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdplusflow OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplusflow TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdplusflow';
   
   END IF;

END$$;

--******************************--
----- views/nhdplus_h_nhdplusflow_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplusflow_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdplusflow') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplusflow_esri
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
      ,CAST(a.fromhydroseq AS NUMERIC) AS fromhydroseq
      ,CAST(a.tohydroseq AS NUMERIC)   AS tohydroseq
      ,a.globalid
      FROM
      cipsrv_nhdplus_h.nhdplusflow a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdplusflow_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplusflow_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdplusflow_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nhdplusgage.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplusgage;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdplusgage') 
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

--******************************--
----- views/nhdplus_h_nhdplusgage_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplusgage_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdplusgage') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplusgage_esri
      AS
      SELECT
       a.objectid
      ,a.hydroaddressid
      ,a.addressdate
      ,a.featuretype
      ,CAST(a.onnetwork AS SMALLINT) AS onnetwork
      ,a.sourceid
      ,a.sourceagency
      ,a.sourcedataset
      ,a.sourcefeatureurl
      ,CAST(a.catchment AS NUMERIC) AS catchment
      ,a.hu
      ,a.reachcode
      ,a.measure
      ,a.reachsmdate
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,a.station_nm
      ,a.state_cd
      ,a.state
      ,a.latsite
      ,a.lonsite
      ,a.dasqmi
      ,CAST(a.referencegage AS SMALLINT) AS referencegage
      ,a.class
      ,a.classmod
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdplusgage a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdplusgage_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplusgage_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdplusgage_esri';
   
   END IF;

END$$;

--******************************--
----- views/nhdplus_h_nhdplussink.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplussink;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdplussink') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplussink
      AS
      SELECT
       a.objectid
      ,a.nhdplusid
      ,a.gridcode
      ,a.purpcode
      ,a.featureid
      ,a.sourcefc
      ,a.rpuid
      ,a.statusflag
      ,a.catchment
      ,a.burn
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdplussink a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdplussink OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplussink TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdplussink';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nhdplussink_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdplussink_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdplussink') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdplussink_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.gridcode
      ,a.purpcode
      ,CAST(a.featureid AS NUMERIC) AS featureid
      ,a.sourcefc
      ,a.rpuid
      ,a.statusflag
      ,CAST(a.catchment AS SMALLINT) AS catchment
      ,CAST(a.burn      AS SMALLINT) AS burn
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdplussink a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdplussink_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdplussink_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdplussink_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nhdpoint.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdpoint;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdpoint') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdpoint
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.ftype
      ,a.fcode
      ,a.nhdplusid
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdpoint a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdpoint OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdpoint TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdpoint';
   
   END IF;

END$$;

--******************************--
----- views/nhdplus_h_nhdpoint_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdpoint_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdpoint') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdpoint_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,CAST(a.ftype AS INTEGER)  AS ftype
      ,CAST(a.fcode AS INTEGER)  AS fcode
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdpoint a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdpoint_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdpoint_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdpoint_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nhdwaterbody.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdwaterbody;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdwaterbody') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdwaterbody
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
      ,a.reachcode
      ,a.ftype
      ,a.fcode
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.onoffnet
      ,a.purpcode
      ,a.burn
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.nhdwaterbody a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdwaterbody OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdwaterbody TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdwaterbody';
   
   END IF;

END$$;

--******************************--
----- views/nhdplus_h_nhdwaterbody_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nhdwaterbody_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nhdwaterbody') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nhdwaterbody_esri
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
      ,a.reachcode
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
      cipsrv_nhdplus_h.nhdwaterbody a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nhdwaterbody_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nhdwaterbody_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nhdwaterbody_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nonnetworknhdflowline.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nonnetworknhdflowline;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nonnetworknhdflowline') 
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nonnetworknhdflowline
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.globalid
      ,ST_Force2D(a.shape) AS shape
      FROM
      cipsrv_nhdplus_h.nonnetworknhdflowline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nonnetworknhdflowline OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nonnetworknhdflowline TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nonnetworknhdflowline_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_nonnetworknhdflowline_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_nonnetworknhdflowline_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','nonnetworknhdflowline') 
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_nonnetworknhdflowline_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,CAST(a.ftype      AS INTEGER)  AS ftype
      ,CAST(a.fcode      AS INTEGER)  AS fcode
      ,a.mainpath
      ,CAST(a.innetwork  AS SMALLINT) AS innetwork
      ,a.visibilityfilter
      ,CAST(a.nhdplusid  AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,a.globalid
      ,ST_Force2D(a.shape) AS shape
      FROM
      cipsrv_nhdplus_h.nonnetworknhdflowline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_nonnetworknhdflowline_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_nonnetworknhdflowline_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_nonnetworknhdflowline_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_wbdhu12.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_wbdhu12;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','wbdhu12') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_wbdhu12
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
      ,a.nhdplusid
      ,a.vpuid
      ,a.shape
      FROM
      cipsrv_nhdplus_h.wbdhu12 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_wbdhu12 OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_wbdhu12 TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_wbdhu12_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_h_wbdhu12_esri.sql 

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

--******************************--
----- views/nhdplus_m_flow_direction.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_flow_direction;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','networknhdflowline') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_flow_direction
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,CAST(DEGREES(
          ST_Azimuth(
             a.penul_point
            ,a.final_point
          )
       ) AS NUMERIC) AS angle_direction
      ,CAST(ROUND(DEGREES(
          ST_Azimuth(
             a.penul_point
            ,a.final_point
          )
       ) / 5 ) * 5 AS NUMERIC ) AS angle_rounded
      ,a.ftype
      ,a.fcode
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.final_point AS shape
      FROM (
         SELECT  
          aa.objectid
         ,aa.permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,ST_PointN(aa.shape,-2) AS penul_point
         ,ST_PointN(aa.shape,-1) AS final_point
         FROM (
            SELECT
             aaa.objectid
            ,aaa.permanent_identifier
            ,aaa.ftype
            ,aaa.fcode
            ,aaa.visibilityfilter
            ,aaa.nhdplusid
            ,ST_Force2D(aaa.shape) AS shape
            FROM
            cipsrv_nhdplus_m.networknhdflowline aaa
         ) aa
      ) a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_flow_direction OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_flow_direction TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_flow_direction';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_flow_direction_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_flow_direction_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','networknhdflowline') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_flow_direction_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,CAST(DEGREES(
          ST_Azimuth(
             a.penul_point
            ,a.final_point
          )
       ) AS NUMERIC) AS angle_direction
      ,CAST(ROUND(DEGREES(
          ST_Azimuth(
             a.penul_point
            ,a.final_point
          )
       ) / 5 ) * 5 AS NUMERIC ) AS angle_rounded
      ,CAST(a.ftype      AS INTEGER)  AS ftype
      ,CAST(a.fcode      AS INTEGER)  AS fcode
      ,a.visibilityfilter
      ,CAST(a.nhdplusid  AS NUMERIC) AS nhdplusid
      ,a.final_point AS shape
      FROM (
         SELECT  
          aa.objectid
         ,aa.permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,ST_PointN(aa.shape,-2) AS penul_point
         ,ST_PointN(aa.shape,-1) AS final_point
         FROM (
            SELECT
             aaa.objectid
            ,aaa.permanent_identifier
            ,aaa.ftype
            ,aaa.fcode
            ,aaa.visibilityfilter
            ,aaa.nhdplusid
            ,ST_Force2D(aaa.shape) AS shape
            FROM
            cipsrv_nhdplus_m.networknhdflowline aaa
         ) aa
      ) a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_flow_direction_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_flow_direction_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_flow_direction_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_networknhdflowline.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_networknhdflowline;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','networknhdflowline') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_networknhdflowline
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.totma
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.streamleve
      ,a.streamorde
      ,a.streamcalc
      ,a.fromnode
      ,a.tonode
      ,a.hydroseq
      ,a.levelpathi
      ,a.pathlength
      ,a.pathtimema
      ,a.terminalpa
      ,a.arbolatesu
      ,a.divergence
      ,a.startflag
      ,a.terminalfl
      ,a.uplevelpat
      ,a.uphydroseq
      ,a.dnlevel
      ,a.dnlevelpat
      ,a.dnhydroseq
      ,a.dnminorhyd
      ,a.dndraincou
      ,a.frommeas
      ,a.tomeas
      ,a.rtndiv
      ,a.thinner
      ,a.vpuin
      ,a.vpuout
      ,a.areasqkm
      ,a.totdasqkm
      ,a.divdasqkm
      ,a.maxelevraw
      ,a.minelevraw
      ,a.maxelevsmo
      ,a.minelevsmo
      ,a.slope
      ,a.slopelenkm
      ,a.elevfixed
      ,a.hwtype
      ,a.hwnodesqkm
      ,a.statusflag
      ,a.qama
      ,a.vama
      ,a.qincrama
      ,a.qbma
      ,a.vbma
      ,a.qincrbma
      ,a.qcma
      ,a.vcma
      ,a.qincrcma
      ,a.qdma
      ,a.vdma
      ,a.qincrdma
      ,a.qema
      ,a.vema
      ,a.qincrema
      ,a.qfma
      ,a.qincrfma
      ,a.arqnavma
      ,a.petma
      ,a.qlossma
      ,a.qgadjma
      ,a.qgnavma
      ,a.gageadjma
      ,a.avgqadjma
      ,a.gageidma
      ,a.gageqma
      ,a.globalid
      ,ST_Force2D(a.shape) AS shape
      FROM
      cipsrv_nhdplus_m.networknhdflowline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_networknhdflowline OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_networknhdflowline TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_networknhdflowline';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_networknhdflowline_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_networknhdflowline_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','networknhdflowline') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_networknhdflowline_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.totma
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,CAST(a.ftype      AS INTEGER)  AS ftype
      ,CAST(a.fcode      AS INTEGER)  AS fcode
      ,a.mainpath
      ,CAST(a.innetwork  AS SMALLINT) AS innetwork
      ,a.visibilityfilter
      ,CAST(a.nhdplusid  AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,CAST(a.streamleve AS SMALLINT) AS streamleve
      ,CAST(a.streamorde AS SMALLINT) AS streamorde
      ,CAST(a.streamcalc AS SMALLINT) AS streamcalc
      ,CAST(a.fromnode   AS NUMERIC)  AS fromnode
      ,CAST(a.tonode     AS NUMERIC)  AS tonode
      ,CAST(a.hydroseq   AS NUMERIC)  AS hydroseq
      ,CAST(a.levelpathi AS NUMERIC)  AS levelpathi
      ,a.pathlength
      ,a.pathtimema
      ,CAST(a.terminalpa AS NUMERIC)  AS terminalpa
      ,a.arbolatesu
      ,CAST(a.divergence AS SMALLINT) AS divergence
      ,CAST(a.startflag  AS SMALLINT) AS startflag
      ,CAST(a.terminalfl AS SMALLINT) AS terminalfl
      ,CAST(a.uplevelpat AS NUMERIC)  AS uplevelpat
      ,CAST(a.uphydroseq AS NUMERIC)  AS uphydroseq
      ,CAST(a.dnlevel    AS SMALLINT) AS dnlevel
      ,CAST(a.dnlevelpat AS NUMERIC)  AS dnlevelpat
      ,CAST(a.dnhydroseq AS NUMERIC)  AS dnhydroseq
      ,CAST(a.dnminorhyd AS NUMERIC)  AS dnminorhyd
      ,CAST(a.dndraincou AS SMALLINT) AS dndraincou
      ,a.frommeas
      ,a.tomeas
      ,CAST(a.rtndiv     AS SMALLINT) AS rtndiv
      ,CAST(a.thinner    AS SMALLINT) AS thinner
      ,CAST(a.vpuin      AS SMALLINT) AS vpuin
      ,CAST(a.vpuout     AS SMALLINT) AS vpuout
      ,a.areasqkm
      ,a.totdasqkm
      ,a.divdasqkm
      ,a.maxelevraw
      ,a.minelevraw
      ,a.maxelevsmo
      ,a.minelevsmo
      ,a.slope
      ,a.slopelenkm
      ,CAST(a.elevfixed  AS SMALLINT) AS elevfixed
      ,CAST(a.hwtype     AS SMALLINT) AS hwtype
      ,a.hwnodesqkm
      ,a.statusflag
      ,a.qama
      ,a.vama
      ,a.qincrama
      ,a.qbma
      ,a.vbma
      ,a.qincrbma
      ,a.qcma
      ,a.vcma
      ,a.qincrcma
      ,a.qdma
      ,a.vdma
      ,a.qincrdma
      ,a.qema
      ,a.vema
      ,a.qincrema
      ,a.qfma
      ,a.qincrfma
      ,a.arqnavma
      ,a.petma
      ,a.qlossma
      ,a.qgadjma
      ,a.qgnavma
      ,CAST(a.gageadjma  AS SMALLINT) AS gageadjma
      ,a.avgqadjma
      ,a.gageidma
      ,a.gageqma
      ,a.globalid
      ,ST_Force2D(a.shape) AS shape
      FROM
      cipsrv_nhdplus_m.networknhdflowline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_networknhdflowline_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_networknhdflowline_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_networknhdflowline_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdarea.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdarea;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdarea') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdarea
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
      ,a.ftype
      ,a.fcode
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.onoffnet
      ,a.purpcode
      ,a.burn
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdarea a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdarea OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdarea TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdarea';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdarea_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdarea_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdarea') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdarea_esri
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
      cipsrv_nhdplus_m.nhdarea a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdarea_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdarea_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdarea_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdline.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdline;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdline') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdline
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.ftype
      ,a.fcode
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdline OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdline TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdline';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdline_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdline_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdline') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdline_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,CAST(a.ftype AS INTEGER)  AS ftype
      ,CAST(a.fcode AS INTEGER)  AS fcode
      ,a.visibilityfilter
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdline_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdline_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdline_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdpluscatchment.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdpluscatchment;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdpluscatchment') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdpluscatchment
      AS
      SELECT
       a.objectid
      ,a.nhdplusid
      ,a.sourcefc
      ,a.gridcode
      ,a.areasqkm
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdpluscatchment a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdpluscatchment OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdpluscatchment TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdpluscatchment';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdpluscatchment_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdpluscatchment_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdpluscatchment') 
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdpluscatchment_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.sourcefc
      ,a.gridcode
      ,a.areasqkm
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdpluscatchment a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdpluscatchment_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdpluscatchment_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdpluscatchment_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdplusflow.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdplusflow;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdplusflow') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdplusflow
      AS
      SELECT
       a.objectid
      ,a.fromnhdpid
      ,a.tonhdpid
      ,a.nodenumber
      ,a.deltalevel
      ,a.direction
      ,a.gapdistkm
      ,a.hasgeo
      ,a.fromvpuid
      ,a.tovpuid
      ,a.frompermid
      ,a.topermid
      ,a.fromhydroseq
      ,a.tohydroseq
      ,a.globalid
      FROM
      cipsrv_nhdplus_m.nhdplusflow a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdplusflow OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdplusflow TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdplusflow';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdplusflow_esri.sql 

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
--******************************--
----- views/nhdplus_m_nhdplusgage.sql 

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
--******************************--
----- views/nhdplus_m_nhdplusgage_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdplusgage_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdplusgage') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdplusgage_esri
      AS
      SELECT
       a.objectid
      ,a.hydroaddressid
      ,a.addressdate
      ,a.featuretype
      ,CAST(a.onnetwork AS SMALLINT) AS onnetwork
      ,a.sourceid
      ,a.sourceagency
      ,a.sourcedataset
      ,a.sourcefeatureurl
      ,CAST(a.catchment AS NUMERIC) AS catchment
      ,a.hu
      ,a.reachcode
      ,a.measure
      ,a.reachsmdate
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,a.station_nm
      ,a.state_cd
      ,a.state
      ,a.latsite
      ,a.lonsite
      ,a.dasqmi
      ,CAST(a.referencegage AS SMALLINT) AS referencegage
      ,a.class
      ,a.classmod
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdplusgage a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdplusgage_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdplusgage_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdplusgage_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdplussink.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdplussink;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdplussink') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdplussink
      AS
      SELECT
       a.objectid
      ,a.nhdplusid
      ,a.gridcode
      ,a.purpcode
      ,a.featureid
      ,a.sourcefc
      ,a.rpuid
      ,a.statusflag
      ,a.catchment
      ,a.burn
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdplussink a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdplussink OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdplussink TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdplussink';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdplussink_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdplussink_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdplussink') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdplussink_esri
      AS
      SELECT
       a.objectid
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.gridcode
      ,a.purpcode
      ,CAST(a.featureid AS NUMERIC) AS featureid
      ,a.sourcefc
      ,a.rpuid
      ,a.statusflag
      ,CAST(a.catchment AS SMALLINT) AS catchment
      ,CAST(a.burn      AS SMALLINT) AS burn
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdplussink a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdplussink_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdplussink_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdplussink';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdpoint.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdpoint;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdpoint') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdpoint
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,a.ftype
      ,a.fcode
      ,a.nhdplusid
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdpoint a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdpoint OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdpoint TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdpoint';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdpoint_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdpoint_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdpoint') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdpoint_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.reachcode
      ,CAST(a.ftype AS INTEGER)  AS ftype
      ,CAST(a.fcode AS INTEGER)  AS fcode
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdpoint a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdpoint_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdpoint_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdpoint';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdwaterbody.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdwaterbody;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdwaterbody') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdwaterbody
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
      ,a.reachcode
      ,a.ftype
      ,a.fcode
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.onoffnet
      ,a.purpcode
      ,a.burn
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.nhdwaterbody a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdwaterbody OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdwaterbody TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdwaterbody_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nhdwaterbody_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nhdwaterbody_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nhdwaterbody') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nhdwaterbody_esri
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
      ,a.reachcode
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
      cipsrv_nhdplus_m.nhdwaterbody a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nhdwaterbody_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nhdwaterbody_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nhdwaterbody_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nonnetworknhdflowline.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nonnetworknhdflowline;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nonnetworknhdflowline') 
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nonnetworknhdflowline
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,a.ftype
      ,a.fcode
      ,a.mainpath
      ,a.innetwork
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.vpuid
      ,a.globalid
      ,ST_Force2D(a.shape) AS shape
      FROM
      cipsrv_nhdplus_m.nonnetworknhdflowline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nonnetworknhdflowline OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nonnetworknhdflowline TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nonnetworknhdflowline_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_nonnetworknhdflowline_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_nonnetworknhdflowline_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','nonnetworknhdflowline') 
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_nonnetworknhdflowline_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.fdate
      ,a.resolution
      ,a.gnis_id
      ,a.gnis_name
      ,a.lengthkm
      ,a.reachcode
      ,a.flowdir
      ,a.wbarea_permanent_identifier
      ,CAST(a.ftype      AS INTEGER)  AS ftype
      ,CAST(a.fcode      AS INTEGER)  AS fcode
      ,a.mainpath
      ,CAST(a.innetwork  AS SMALLINT) AS innetwork
      ,a.visibilityfilter
      ,CAST(a.nhdplusid  AS NUMERIC) AS nhdplusid
      ,a.vpuid
      ,a.globalid
      ,ST_Force2D(a.shape) AS shape
      FROM
      cipsrv_nhdplus_m.nonnetworknhdflowline a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_nonnetworknhdflowline_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_nonnetworknhdflowline_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_nonnetworknhdflowline_esri';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_wbdhu12.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_wbdhu12;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','wbdhu12') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_wbdhu12
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
      ,a.nhdplusid
      ,a.vpuid
      ,a.shape
      FROM
      cipsrv_nhdplus_m.wbdhu12 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_wbdhu12 OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_wbdhu12 TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_wbdhu12';
   
   END IF;

END$$;
--******************************--
----- views/nhdplus_m_wbdhu12_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.nhdplus_m_wbdhu12_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_m','wbdhu12') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_m_wbdhu12_esri
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
      cipsrv_nhdplus_m.wbdhu12 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_m_wbdhu12_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_m_wbdhu12_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_m_wbdhu12_esri';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_control.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_control;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_control') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_control
      AS
      SELECT
       (ROW_NUMBER() OVER())::INTEGER AS objectid
      ,a.keyword
      ,a.value_str
      ,a.value_num
      ,a.value_date
      FROM
      cipsrv_owld.wqp_control a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_control OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_control TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_control';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_control_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_control_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_control') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_control_esri
      AS
      SELECT
       (ROW_NUMBER() OVER())::INTEGER AS objectid
      ,a.keyword
      ,a.value_str
      ,a.value_num
      ,a.value_date
      FROM
      cipsrv_owld.wqp_control a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_control_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_control_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_control_esri';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_attr.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_attr;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_attr') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_attr
      AS
      SELECT
       a.objectid
      ,a.source_joinkey
      ,a.organizationidentifier
      ,a.organizationformalname
      ,a.monitoringlocationidentifier
      ,a.monitoringlocationname
      ,a.monitoringlocationtypename
      ,a.monitoringlocationdescription
      ,a.huceightdigitcode
      ,a.drainageareameasure_measureval
      ,a.drainageareameasure_measureunt
      ,a.contributingdrainageareameasva
      ,a.contributingdrainageareameasun
      ,a.latitudemeasure
      ,a.longitudemeasure
      ,a.sourcemapscalenumeric
      ,a.horizontalaccuracymeasureval
      ,a.horizontalaccuracymeasureunit
      ,a.horizontalcollectionmethodname
      ,a.horizontalcoordinatereferences
      ,a.verticalmeasure_measurevalue
      ,a.verticalmeasure_measureunit
      ,a.verticalaccuracymeasurevalue
      ,a.verticalaccuracymeasureunit
      ,a.verticalcollectionmethodname
      ,a.verticalcoordinatereferencesys
      ,a.countrycode
      ,a.statecode
      ,a.countycode
      ,a.aquifername
      ,a.formationtypetext
      ,a.aquifertypename
      ,a.localaqfrname
      ,a.constructiondatetext
      ,a.welldepthmeasure_measurevalue
      ,a.welldepthmeasure_measureunit
      ,a.wellholedepthmeasure_measureva
      ,a.wellholedepthmeasure_measureun
      ,a.providername
      ,a.globalid
      FROM
      cipsrv_owld.wqp_attr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_attr OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_attr TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_attr';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_attr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_attr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_attr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_attr_esri
      AS
      SELECT
       a.objectid
      ,a.source_joinkey
      ,a.organizationidentifier
      ,a.organizationformalname
      ,a.monitoringlocationidentifier
      ,a.monitoringlocationname
      ,a.monitoringlocationtypename
      ,a.monitoringlocationdescription
      ,a.huceightdigitcode
      ,a.drainageareameasure_measureval
      ,a.drainageareameasure_measureunt
      ,a.contributingdrainageareameasva
      ,a.contributingdrainageareameasun
      ,a.latitudemeasure
      ,a.longitudemeasure
      ,a.sourcemapscalenumeric
      ,a.horizontalaccuracymeasureval
      ,a.horizontalaccuracymeasureunit
      ,a.horizontalcollectionmethodname
      ,a.horizontalcoordinatereferences
      ,a.verticalmeasure_measurevalue
      ,a.verticalmeasure_measureunit
      ,a.verticalaccuracymeasurevalue
      ,a.verticalaccuracymeasureunit
      ,a.verticalcollectionmethodname
      ,a.verticalcoordinatereferencesys
      ,a.countrycode
      ,a.statecode
      ,a.countycode
      ,a.aquifername
      ,a.formationtypetext
      ,a.aquifertypename
      ,a.localaqfrname
      ,a.constructiondatetext
      ,a.welldepthmeasure_measurevalue
      ,a.welldepthmeasure_measureunit
      ,a.wellholedepthmeasure_measureva
      ,a.wellholedepthmeasure_measureun
      ,a.providername
      ,a.globalid
      FROM
      cipsrv_owld.wqp_attr a
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_attr_esri OWNER TO cipsrv_gis
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_attr_esri TO public
      $q$;
      
   ELSE
      RAISE WARNING 'skipping owld_wqp_attr_esri';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_cip_h.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_cip_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_cip') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_cip_h
      AS
      SELECT
       a.objectid
      ,a.cip_joinkey
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,a.nhdplusid
      ,a.istribal
      ,a.istribal_areasqkm
      ,a.catchmentresolution
      ,a.catchmentareasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.globalid
      FROM
      cipsrv_owld.wqp_cip a
      WHERE
      a.catchmentresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_cip_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_cip_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_cip_h';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_cip_m.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_cip_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_cip') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_cip_m
      AS
      SELECT
       a.objectid
      ,a.cip_joinkey
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,a.nhdplusid
      ,a.istribal
      ,a.istribal_areasqkm
      ,a.catchmentresolution
      ,a.catchmentareasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.globalid
      FROM
      cipsrv_owld.wqp_cip a
      WHERE
      a.catchmentresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_cip_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_cip_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_cip_m';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_cip_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_cip_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_cip') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_cip_esri
      AS
      SELECT
       a.objectid
      ,a.cip_joinkey
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.istribal
      ,a.istribal_areasqkm
      ,a.catchmentresolution
      ,a.catchmentareasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.globalid
      FROM
      cipsrv_owld.wqp_cip a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_cip_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_cip_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_cip';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_cip_geo_h.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_cip_geo_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_cip_geo') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_cip_geo_h
      AS
      SELECT
       a.objectid
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,a.nhdplusid
      ,a.catchmentresolution
      ,a.catchmentareasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_cip_geo a
      WHERE
      a.catchmentresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_cip_geo_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_cip_geo_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_cip_geo_h';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_cip_geo_m.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_cip_geo_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_cip_geo') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_cip_geo_m
      AS
      SELECT
       a.objectid
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,a.nhdplusid
      ,a.catchmentresolution
      ,a.catchmentareasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_cip_geo a
      WHERE
      a.catchmentresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_cip_geo_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_cip_geo_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_cip_geo_m';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_cip_geo_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_cip_geo_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_cip_geo') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_cip_geo_esri
      AS
      SELECT
       a.objectid
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.catchmentresolution
      ,a.catchmentareasqkm
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_cip_geo a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_cip_geo_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_cip_geo_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_cip_geo_esri';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_huc12_h.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_huc12_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_huc12') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_huc12_h
      AS
      SELECT
       a.objectid
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.xwalk_huc12
      ,a.xwalk_catresolution
      ,a.xwalk_huc12_version
      ,a.xwalk_huc12_areasqkm
      ,a.globalid
      FROM
      cipsrv_owld.wqp_huc12 a
      WHERE
      a.xwalk_catresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_huc12_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_huc12_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_huc12_h';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_huc12_m.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_huc12_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_huc12') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_huc12_m
      AS
      SELECT
       a.objectid
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.xwalk_huc12
      ,a.xwalk_catresolution
      ,a.xwalk_huc12_version
      ,a.xwalk_huc12_areasqkm
      ,a.globalid
      FROM
      cipsrv_owld.wqp_huc12 a
      WHERE
      a.xwalk_catresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_huc12_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_huc12_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_huc12_m';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_huc12_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_huc12_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_huc12') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_huc12_esri
      AS
      SELECT
       a.objectid
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.xwalk_huc12
      ,a.xwalk_catresolution
      ,a.xwalk_huc12_version
      ,a.xwalk_huc12_areasqkm
      ,a.globalid
      FROM
      cipsrv_owld.wqp_huc12 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_huc12_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_huc12_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.wqp_huc12_esri';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_huc12_geo_h.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_huc12_geo_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_huc12_geo') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_huc12_geo_h
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_catresolution
      ,a.xwalk_huc12_version
      ,a.xwalk_huc12_areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_huc12_geo a
      WHERE
      a.xwalk_catresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_huc12_geo_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_huc12_geo_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.wqp_huc12_geo_h';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_huc12_geo_m.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_huc12_geo_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_huc12_geo') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_huc12_geo_m
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_catresolution
      ,a.xwalk_huc12_version
      ,a.xwalk_huc12_areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_huc12_geo a
      WHERE
      a.xwalk_catresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_huc12_geo_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_huc12_geo_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.wqp_huc12_geo_m';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_huc12_geo_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_huc12_geo_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_huc12_geo') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_huc12_geo_esri
      AS
      SELECT
       a.objectid
      ,a.xwalk_huc12
      ,a.xwalk_catresolution
      ,a.xwalk_huc12_version
      ,a.xwalk_huc12_areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_huc12_geo a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_huc12_geo_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_huc12_geo_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.wqp_huc12_geo_esri';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_rad_a_h.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_a_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_a') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_a_h
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.eventdate
      ,a.reachcode
      ,a.reachsmdate
      ,a.reachresolution
      ,a.feature_permanent_identifier
      ,a.featureclassref
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_datadesc
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.eventtype
      ,a.event_areasqkm
      ,a.geogstate
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_rad_a a
      WHERE
      a.reachresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_rad_a_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_a_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_a_h';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_a_m.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_a_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_a') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_a_m
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.eventdate
      ,a.reachcode
      ,a.reachsmdate
      ,a.reachresolution
      ,a.feature_permanent_identifier
      ,a.featureclassref
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_datadesc
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.eventtype
      ,a.event_areasqkm
      ,a.geogstate
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_rad_a a
      WHERE
      a.reachresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_a_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_a_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_a_m';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_a_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_a_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_a') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_a_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.eventdate
      ,a.reachcode
      ,a.reachsmdate
      ,a.reachresolution
      ,a.feature_permanent_identifier
      ,a.featureclassref
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_datadesc
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.eventtype
      ,a.event_areasqkm
      ,a.geogstate
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_rad_a a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_rad_a_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_a_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_a_esri';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_evt2meta_h.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_evt2meta_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_evt2meta') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_evt2meta_h
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.meta_processid
      ,a.globalid
      FROM
      cipsrv_owld.wqp_rad_evt2meta a
      WHERE
      a.reachresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_evt2meta_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_evt2meta_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_evt2meta_h';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_evt2meta_m.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_evt2meta_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_evt2meta') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_evt2meta_m
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.meta_processid
      ,a.globalid
      FROM
      cipsrv_owld.wqp_rad_evt2meta a
      WHERE
      a.reachresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_evt2meta_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_evt2meta_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_evt2meta_m';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_evt2meta_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_evt2meta_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_evt2meta') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_evt2meta_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.meta_processid
      ,a.globalid
      FROM
      cipsrv_owld.wqp_rad_evt2meta a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_evt2meta_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_evt2meta_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_evt2meta';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_l_h.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_l_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_l') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_l_h
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.eventdate
      ,a.reachcode
      ,a.reachsmdate
      ,a.reachresolution
      ,a.feature_permanent_identifier
      ,a.featureclassref
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_datadesc
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.fmeasure
      ,a.tmeasure
      ,a.eventtype
      ,a.eventoffset
      ,a.event_lengthkm
      ,a.geogstate
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_rad_l a
      WHERE
      a.reachresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_l_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_l_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_l_h';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_l_m.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_l_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_l') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_l_m
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.eventdate
      ,a.reachcode
      ,a.reachsmdate
      ,a.reachresolution
      ,a.feature_permanent_identifier
      ,a.featureclassref
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_datadesc
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.fmeasure
      ,a.tmeasure
      ,a.eventtype
      ,a.eventoffset
      ,a.event_lengthkm
      ,a.geogstate
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_rad_l a
      WHERE
      a.reachresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_l_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_l_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_l_m';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_l_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_l_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_l') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_l_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.eventdate
      ,a.reachcode
      ,a.reachsmdate
      ,a.reachresolution
      ,a.feature_permanent_identifier
      ,a.featureclassref
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_datadesc
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.fmeasure
      ,a.tmeasure
      ,a.eventtype
      ,a.eventoffset
      ,a.event_lengthkm
      ,a.geogstate
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_rad_l a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_l_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_l_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_l_esri';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_metadata.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_metadata;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_metadata') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_metadata
      AS
      SELECT
       a.objectid
      ,a.meta_processid
      ,a.processdescription
      ,a.processdate
      ,a.attributeaccuracyreport
      ,a.logicalconsistencyreport
      ,a.completenessreport
      ,a.horizpositionalaccuracyreport
      ,a.vertpositionalaccuracyreport
      ,a.metadatastandardname
      ,a.metadatastandardversion
      ,a.metadatadate
      ,a.datasetcredit
      ,a.contactorganization
      ,a.addresstype
      ,a.address
      ,a.city
      ,a.stateorprovince
      ,a.postalcode
      ,a.contactvoicetelephone
      ,a.contactinstructions
      ,a.contactemailaddress
      ,a.globalid
      FROM
      cipsrv_owld.wqp_rad_metadata a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_metadata OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_metadata TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_metadata';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_metadata_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_metadata_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_metadata') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_metadata_esri
      AS
      SELECT
       a.objectid
      ,a.meta_processid
      ,a.processdescription
      ,a.processdate
      ,a.attributeaccuracyreport
      ,a.logicalconsistencyreport
      ,a.completenessreport
      ,a.horizpositionalaccuracyreport
      ,a.vertpositionalaccuracyreport
      ,a.metadatastandardname
      ,a.metadatastandardversion
      ,a.metadatadate
      ,a.datasetcredit
      ,a.contactorganization
      ,a.addresstype
      ,a.address
      ,a.city
      ,a.stateorprovince
      ,a.postalcode
      ,a.contactvoicetelephone
      ,a.contactinstructions
      ,a.contactemailaddress
      ,a.globalid
      FROM
      cipsrv_owld.wqp_rad_metadata a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_metadata_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_metadata_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_metadata_esri';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_p_h.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_p_h;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_p') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_p_h
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.eventdate
      ,a.reachcode
      ,a.reachsmdate
      ,a.reachresolution
      ,a.feature_permanent_identifier
      ,a.featureclassref
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_datadesc
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.measure
      ,a.eventtype
      ,a.eventoffset
      ,a.geogstate
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_rad_p a
      WHERE
      a.reachresolution = 'HR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_p_h OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_p_h TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_p_h';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_rad_p_m.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_p_m;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_p') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_p_m
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.eventdate
      ,a.reachcode
      ,a.reachsmdate
      ,a.reachresolution
      ,a.feature_permanent_identifier
      ,a.featureclassref
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_datadesc
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.measure
      ,a.eventtype
      ,a.eventoffset
      ,a.geogstate
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_rad_p a
      WHERE
      a.reachresolution = 'MR';
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_p_m OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_p_m TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_p_m';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_p_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_p_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_p') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_p_esri
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,a.eventdate
      ,a.reachcode
      ,a.reachsmdate
      ,a.reachresolution
      ,a.feature_permanent_identifier
      ,a.featureclassref
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_datadesc
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.measure
      ,a.eventtype
      ,a.eventoffset
      ,a.geogstate
      ,a.xwalk_huc12
      ,a.xwalk_method
      ,a.xwalk_huc12_version
      ,a.isnavigable
      ,a.hasvaa
      ,a.issink
      ,a.isheadwater
      ,a.iscoastal
      ,a.isocean
      ,a.isalaskan
      ,a.h3hexagonaddr
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_rad_p a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_p_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_p_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_p_esri';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_srccit.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_srccit;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_srccit') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_srccit
      AS
      SELECT
       a.objectid
      ,a.title
      ,a.source_datasetid
      ,a.sourcecitationabbreviation
      ,a.originator
      ,a.publicationdate
      ,a.beginningdate
      ,a.endingdate
      ,a.sourcecontribution
      ,a.sourcescaledenominator
      ,a.typeofsourcemedia
      ,a.calendardate
      ,a.sourcecurrentnessreference
      ,a.meta_processid
      ,a.globalid
      FROM
      cipsrv_owld.wqp_rad_srccit a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_srccit OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_srccit TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_srccit';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_rad_srccit_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_srccit_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_rad_srccit') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_srccit_esri
      AS
      SELECT
       a.objectid
      ,a.title
      ,a.source_datasetid
      ,a.sourcecitationabbreviation
      ,a.originator
      ,a.publicationdate
      ,a.beginningdate
      ,a.endingdate
      ,a.sourcecontribution
      ,a.sourcescaledenominator
      ,a.typeofsourcemedia
      ,a.calendardate
      ,a.sourcecurrentnessreference
      ,a.meta_processid
      ,a.globalid
      FROM
      cipsrv_owld.wqp_rad_srccit a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_rad_srccit_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_rad_srccit_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_rad_srccit_esri';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_sfid.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_sfid;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_sfid') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_sfid
      AS
      SELECT
       a.objectid
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.sfiddetailurl
      ,a.load_id
      ,a.load_date
      ,a.src_event_count
      ,a.src_point_count
      ,a.src_line_count
      ,a.src_area_count
      ,a.cat_mr_count
      ,a.cat_hr_count
      ,a.xwalk_huc12_np21_count
      ,a.rad_mr_event_count
      ,a.rad_hr_event_count
      ,a.rad_mr_point_count
      ,a.rad_hr_point_count
      ,a.rad_mr_line_count
      ,a.rad_hr_line_count
      ,a.rad_mr_area_count
      ,a.rad_hr_area_count
      ,a.globalid
      FROM
      cipsrv_owld.wqp_sfid a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_sfid OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_sfid TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_sfid';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_sfid_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_sfid_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_sfid') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_sfid_esri
      AS
      SELECT
       a.objectid
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.sfiddetailurl
      ,a.load_id
      ,a.load_date
      ,a.src_event_count
      ,a.src_point_count
      ,a.src_line_count
      ,a.src_area_count
      ,a.cat_mr_count
      ,a.cat_hr_count
      ,a.xwalk_huc12_np21_count
      ,a.rad_mr_event_count
      ,a.rad_hr_event_count
      ,a.rad_mr_point_count
      ,a.rad_hr_point_count
      ,a.rad_mr_line_count
      ,a.rad_hr_line_count
      ,a.rad_mr_area_count
      ,a.rad_hr_area_count
      ,a.globalid
      FROM
      cipsrv_owld.wqp_sfid a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_sfid_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_sfid_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_sfid_esri';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_src2cip.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src2cip;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_src2cip') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src2cip
      AS
      SELECT
       a.objectid
      ,a.src2cip_joinkey
      ,a.cip_joinkey
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,a.nhdplusid
      ,a.catchmentresolution
      ,a.cip_action
      ,a.overlap_measure
      ,a.cip_method
      ,a.cip_parms
      ,a.cip_date
      ,a.cip_version
      ,a.globalid
      FROM
      cipsrv_owld.wqp_src2cip a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_src2cip OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_src2cip TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_src2cip';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_src2cip_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src2cip_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_src2cip') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src2cip_esri
      AS
      SELECT
       a.objectid
      ,a.src2cip_joinkey
      ,a.cip_joinkey
      ,a.source_joinkey
      ,a.permid_joinkey
      ,a.cat_joinkey
      ,a.catchmentstatecode
      ,CAST(a.nhdplusid AS NUMERIC) AS nhdplusid
      ,a.catchmentresolution
      ,a.cip_action
      ,a.overlap_measure
      ,a.cip_method
      ,a.cip_parms
      ,a.cip_date
      ,a.cip_version
      ,a.globalid
      FROM
      cipsrv_owld.wqp_src2cip a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_src2cip_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_src2cip_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_src2cip';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_src_a.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src_a;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_src_a') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src_a
      AS
      SELECT
       a.objectid
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_src_a a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.owld_wqp_src_a OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_src_a TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_src_a';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_src_a_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src_a_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_src_a') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src_a_esri
      AS
      SELECT
       a.objectid
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.areasqkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_src_a a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_src_a_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_src_a_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_src_a';
   
   END IF;

END$$;

--******************************--
----- views/owld_wqp_src_l.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src_l;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_src_l') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src_l
      AS
      SELECT
       a.objectid
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.lengthkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_src_l a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_src_l OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_src_l TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_src_l';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_src_l_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src_l_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_src_l') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src_l_esri
      AS
      SELECT
       a.objectid
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.lengthkm
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_src_l a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_src_l_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_src_l_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_src_l';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_src_p.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src_p;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_src_p') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src_p
      AS
      SELECT
       a.objectid
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_src_p a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_src_p OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_src_p TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_src_p';
   
   END IF;

END$$;
--******************************--
----- views/owld_wqp_src_p_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_src_p_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_owld','wqp_src_p') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_src_p_esri
      AS
      SELECT
       a.objectid
      ,a.permid_joinkey
      ,a.source_originator
      ,a.source_featureid
      ,a.source_featureid2
      ,a.source_series
      ,a.source_subdivision
      ,a.source_joinkey
      ,a.start_date
      ,a.end_date
      ,a.featuredetailurl
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_owld.wqp_src_p a;
      $q$;                                                                                                                          

      EXECUTE $q$

      ALTER TABLE cipsrv_gis.owld_wqp_src_p_esri OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.owld_wqp_src_p_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.owld_wqp_src_p';
   
   END IF;

END$$;
--******************************--
----- views/tiger_aiannha.sql 

DROP VIEW IF EXISTS cipsrv_gis.tiger_aiannha;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_support','tiger_aiannha') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.tiger_aiannha
      AS
      SELECT
       a.objectid
      ,a.aiannhns
      ,a.geoid
      ,a.namelsad
      ,a.classfp
      ,a.comptyp
      ,a.aiannhr
      ,a.mtfcc
      ,a.funcstat
      ,a.aland
      ,a.awater
      ,a.intptlat
      ,a.intptlon
      ,a.shape
      FROM
      cipsrv_support.tiger_aiannha a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.tiger_aiannha OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.tiger_aiannha TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping tiger_aiannha';
   
   END IF;

END$$;
--******************************--
----- views/tiger_aiannha_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.tiger_aiannha_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_support','tiger_aiannha')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.tiger_aiannha_esri
      AS
      SELECT
       a.objectid
      ,a.aiannhns
      ,a.geoid
      ,a.namelsad
      ,a.classfp
      ,a.comptyp
      ,a.aiannhr
      ,a.mtfcc
      ,a.funcstat
      ,a.aland
      ,a.awater
      ,a.intptlat
      ,a.intptlon
      ,a.shape
      FROM
      cipsrv_support.tiger_aiannha a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.tiger_aiannha_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.tiger_aiannha_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping tiger_aiannha_esri';
   
   END IF;

END$$;
--******************************--
----- views/tiger_fedstatewaters.sql 

DROP VIEW IF EXISTS cipsrv_gis.tiger_fedstatewaters;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_support','tiger_fedstatewaters') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.tiger_fedstatewaters
      AS
      SELECT
       a.objectid
      ,a.statens
      ,a.geoid
      ,a.stusps
      ,a.name
      ,a.aland
      ,a.awater
      ,a.intptlat
      ,a.intptlon
      ,a.shape
      FROM
      cipsrv_support.tiger_fedstatewaters a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.tiger_fedstatewaters OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.tiger_fedstatewaters TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping tiger_fedstatewaters';
   
   END IF;

END$$;
--******************************--
----- views/tiger_fedstatewaters_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.tiger_fedstatewaters_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_support','tiger_fedstatewaters')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.tiger_fedstatewaters_esri
      AS
      SELECT
       a.objectid
      ,a.statens
      ,a.geoid
      ,a.stusps
      ,a.name
      ,a.aland
      ,a.awater
      ,a.intptlat
      ,a.intptlon
      ,a.shape
      FROM
      cipsrv_support.tiger_fedstatewaters a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.tiger_fedstatewaters_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.tiger_fedstatewaters_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping tiger_fedstatewaters_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu12_f3.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12_f3;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12_f3') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12_f3
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12_f3 OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12_f3 TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12_f3';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu12sp_f3.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12sp_f3;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12sp_f3') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12sp_f3
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12sp_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12sp_f3 OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12sp_f3 TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12sp_f3';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu12_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu12sp_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12sp_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12sp_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12sp_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12sp_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12sp_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12sp_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12sp_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu10_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu10_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu10_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu10_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc10
      ,a.hutype
      ,a.humod
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu10_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu10_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu10_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu10_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu10sp_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu10sp_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu10sp_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu10sp_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc10
      ,a.hutype
      ,a.humod
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu10sp_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu10sp_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu10sp_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu10sp_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu8_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu8_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu8_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu8_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc8
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu8_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu8_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu8_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu8_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu8sp_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu8sp_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu8sp_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu8sp_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc8
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu8sp_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu8sp_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu8sp_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu8sp_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu6_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu6_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu6_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu6_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc6
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu6_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu6_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu6_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu6_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu6sp_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu6sp_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu6sp_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu6sp_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc6
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu6sp_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu6sp_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu6sp_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu6sp_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu4_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu4_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu4_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu4_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc4
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu4_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu4_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu4_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu4_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu4sp_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu4sp_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu4sp_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu4sp_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc4
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu4sp_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu4sp_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu4sp_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu4sp_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu2_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu2_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu2_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu2_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc2
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu2_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu2_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu2_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu2_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu2sp_f3_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu2sp_f3_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu2sp_f3')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu2sp_f3_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc2
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu2sp_f3 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu2sp_f3_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu2sp_f3_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu2sp_f3_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu12_np21.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12_np21;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12_np21') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12_np21
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12_np21 OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12_np21 TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12_np21';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu12_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu12sp_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12sp_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12sp_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12sp_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12sp_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12sp_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12sp_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12sp_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu10_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu10_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu10_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu10_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc10
      ,a.hutype
      ,a.humod
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu10_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu10_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu10_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu10_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu10sp_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu10sp_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu10sp_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu10sp_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc10
      ,a.hutype
      ,a.humod
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu10sp_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu10sp_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu10sp_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu10sp_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu8_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu8_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu8_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu8_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc8
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu8_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu8_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu8_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu8_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu8sp_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu8sp_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu8sp_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu8sp_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc8
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu8sp_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu8sp_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu8sp_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu8sp_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu6_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu6_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu6_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu6_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc6
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu6_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu6_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu6_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu6_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu6sp_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu6sp_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu6sp_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu6sp_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc6
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu6sp_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu6sp_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu6sp_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu6sp_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu4_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu4_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu4_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu4_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc4
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu4_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu4_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu4_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu4_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu4sp_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu4sp_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu4sp_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu4sp_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc4
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu4sp_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu4sp_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu4sp_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu4sp_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu2_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu2_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu2_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu2_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc2
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu2_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu2_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu2_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu2_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu2sp_np21_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu2sp_np21_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu2sp_np21')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu2sp_np21_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc2
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu2sp_np21 a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu2sp_np21_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu2sp_np21_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu2sp_np21_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu12_nphr.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12_nphr;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12_nphr') 
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12_nphr
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12_nphr OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12_nphr TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12_nphr';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu12_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu12sp_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu12sp_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu12sp_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu12sp_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc12
      ,a.hutype
      ,a.humod
      ,a.tohuc
      ,a.noncontributingareaacres
      ,a.noncontributingareasqkm
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu12sp_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu12sp_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu12sp_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu12sp_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu10_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu10_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu10_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu10_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc10
      ,a.hutype
      ,a.humod
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu10_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu10_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu10_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu10_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu10sp_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu10sp_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu10sp_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu10sp_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc10
      ,a.hutype
      ,a.humod
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu10sp_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu10sp_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu10sp_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu10sp_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu8_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu8_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu8_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu8_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc8
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu8_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu8_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu8_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu8_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu8sp_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu8sp_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu8sp_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu8sp_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc8
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu8sp_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu8sp_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu8sp_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu8sp_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu6_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu6_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu6_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu6_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc6
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu6_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu6_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu6_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu6_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu6sp_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu6sp_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu6sp_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu6sp_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc6
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu6sp_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu6sp_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu6sp_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu6sp_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu4_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu4_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu4_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu4_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc4
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu4_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu4_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu4_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu4_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu4sp_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu4sp_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu4sp_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu4sp_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc4
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu4sp_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu4sp_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu4sp_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu4sp_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu2_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu2_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu2_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu2_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc2
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu2_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu2_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu2_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu2_nphr_esri';
   
   END IF;

END$$;
--******************************--
----- views/wbd_hu2sp_nphr_esri.sql 

DROP VIEW IF EXISTS cipsrv_gis.wbd_hu2sp_nphr_esri;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_wbd','wbd_hu2sp_nphr')
   THEN 

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.wbd_hu2sp_nphr_esri
      AS
      SELECT
       a.objectid
      ,a.tnmid
      ,a.metasourceid
      ,a.loaddate
      ,a.areasqkm
      ,a.areaacres
      ,a.name
      ,a.states
      ,a.huc2
      ,a.centermass_x
      ,a.centermass_y
      ,a.globalid
      ,a.shape
      FROM
      cipsrv_wbd.wbd_hu2sp_nphr a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.wbd_hu2sp_nphr_esri OWNER TO cipsrv_gis;
      $q$;

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.wbd_hu2sp_nphr_esri TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping wbd_hu2sp_nphr_esri';
   
   END IF;

END$$;
