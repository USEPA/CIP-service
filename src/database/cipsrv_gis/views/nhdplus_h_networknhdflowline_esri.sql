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
,ST_Transform(ST_Force2D(a.shape),3857) AS shape
FROM
cipsrv_nhdplus_h.networknhdflowline a;

ALTER TABLE cipsrv_gis.nhdplus_h_networknhdflowline_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.nhdplus_h_networknhdflowline_esri TO public;
