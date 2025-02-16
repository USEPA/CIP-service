DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_networknhdflowline;

DO $$DECLARE 
BEGIN

   IF EXISTS(
      SELECT 1 FROM information_schema.tables a
      WHERE a.table_schema = 'cipsrv_nhdplus_h'
      AND   a.table_name   = 'networknhdflowline'
   ) 
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
