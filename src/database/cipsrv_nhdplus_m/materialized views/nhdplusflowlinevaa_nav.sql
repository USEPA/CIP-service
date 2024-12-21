DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.nhdplusflowlinevaa_nav;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(
    objectid
   ,nhdplusid
   ,hydroseq
   ,fmeasure
   ,tmeasure
   ,levelpathi
   ,terminalpa
   ,uphydroseq
   ,dnhydroseq
   ,dnminorhyd
   ,divergence
   ,fromnode
   ,tonode
   /* ++++++++++ */
   ,lengthkm
   ,totma
   ,pathlength
   ,pathtimema
   /* ++++++++++ */
   ,force_main_line
   ,ary_upstream_hydroseq
   ,ary_downstream_hydroseq
   /* ++++++++++ */
   ,headwater
   ,coastal_connection
   ,network_end
   /* ++++++++++ */
   ,vpuid
)
AS
SELECT
 CAST(a.objectid AS INTEGER) AS objectid
,CAST(a.nhdplusid  AS BIGINT) AS nhdplusid
,CAST(a.hydroseq   AS BIGINT) AS hydroseq 
,a.frommeas
,a.tomeas
,CAST(a.levelpathi AS BIGINT) AS levelpathi
,CAST(a.terminalpa AS BIGINT) AS terminalpa
,CASE
 WHEN a.uphydroseq = 0
 THEN
   CAST(NULL AS BIGINT)
 ELSE
   CAST(a.uphydroseq AS BIGINT)
 END AS uphydroseq
,CASE
 WHEN a.dnhydroseq = 0
 THEN
   CAST(NULL AS BIGINT)
 ELSE
   CAST(a.dnhydroseq AS BIGINT)
 END AS dnhydroseq
,CASE
 WHEN a.dnminorhyd = 0
 THEN
   CAST(NULL AS BIGINT)
 ELSE
   CAST(a.dnminorhyd AS BIGINT)
 END AS dnminorhyd
,a.divergence
,CAST(a.fromnode AS BIGINT)   AS fromnode
,CAST(a.tonode AS BIGINT)     AS tonode
/* ++++++++++ */
,CAST(a.lengthkm AS NUMERIC) AS lengthkm
,CASE
 WHEN a.totma IN (-9999,-9998)
 THEN
   CAST(NULL AS NUMERIC)
 ELSE
   CAST(a.totma AS NUMERIC)
 END AS totma
,CAST(a.pathlength AS NUMERIC) AS pathlength
,CASE
 WHEN a.pathtimema IN (-9999,-9998)
 THEN
   CAST(NULL AS NUMERIC)
 ELSE
   CAST(a.pathtimema AS NUMERIC)
 END AS pathtimema
/* ++++++++++ */
,CASE
 WHEN a.hydroseq IN (
   --- Big Tributaries --
    350009839  -- Arkansas
   ,550002171  -- Big Blue
   ,590012528  -- Big Sioux
   ,590004188  -- Bighorn
   ,350003361  -- Black
   ,390006004  -- Black (2)
   ,390000311  -- Canadian
   ,510002921  -- Cedar
   ,590007834  -- Cheyenne
   ,590010733  -- Cheyenne (2)
   ,390001215  -- Cimarron
   ,50004179   -- Clearwater
   ,430000065  -- Cumberland
   ,510002338  -- Des Moines
   ,10002380   -- Feather
   ,720000771  -- Gila
   ,760000231  -- Green
   ,430001637  -- Green
   ,510000257  -- Illinois
   ,510002770  -- Iowa
   ,590008899  -- James
   ,430000838  -- Kanawha
   ,550001526  -- Kansas
   ,430002658  -- Kentucky
   ,720001913  -- Little Colorado
   ,590006912  -- Little Missouri
   ,550008373  -- Loup
   ,590012003  -- Milk
   ,510003597  -- Minnesota
   ,350003335  -- Mississippi from Atchafalaya
   ,550000017  -- Missouri
   ,720001632  -- Muddy
   ,430002416  -- Muskingum
   ,390004971  -- Neosho
   ,590001226  -- Niobrara
   ,550003947  -- North Platte
   ,350003411  -- Ouachita
   ,430000004  -- Ohio
   ,550009800  -- Osage
   ,50003837   -- Owyhee 
   ,680001003  -- Pecos
   ,550000622  -- Platte
   ,590006969  -- Powder
   ,550005927  -- Republican
   ,510001488  -- Rock
   ,50002910   -- Salmon
   ,720001660  -- Salt
   ,760000974  -- San Juan
   ,430002448  -- Scioto
   ,840000351  -- Sheyenne
   ,50001581   -- Snake
   ,550010594  -- Solomon
   ,510003688  -- St. Croix
   ,350005173  -- St. Francis
   ,470000012  -- Tennessee
   ,430001211  -- Wabash
   ,350003903  -- White
   ,590011506  -- White (2)
   ,50004305   -- Willamette
   ,510002581  -- Wisconsin
   ,350005918  -- Yazoo
   ,590001280  -- Yellowstone
   --- Born on the Port Allen Bayou --
   ,350002673
   ,350002676
   ,350002718
   ,350002733
   ,350002775
   ,350002785
   ,350002783
   ,350002835
   ,350002844
   ,350002873
   ,350002878
   ,350002894
   ,350002915
   ,350002946
   ,350002973
   ,350003025
   ,350003055
   ,350003153
   ,350003177
   ,350003182
   ,350003196
   ,350003274
   ,350037594
   ,350045866
   ,350083155
   --- Kaskaskia Old Course --
   ,510000109
   ,510000101
   ,510000102
   ,510000111
   --- Other minor networks receiving big water
   ,510000080
   ,510000089
   ,510000143
   ,550002456
   ,550003310
 )
 THEN
   TRUE
 ELSE
   FALSE
 END AS force_main_line
,ARRAY(SELECT CAST(bb.fromhydroseq AS BIGINT) FROM cipsrv_nhdplus_m.nhdplusflow bb WHERE bb.tohydroseq = a.hydroseq) AS ary_upstream_hydroseq
,CASE
 WHEN a.dndraincou = 1
 THEN
   ARRAY[CAST(a.dnhydroseq AS BIGINT)]
 WHEN a.dndraincou = 2
 THEN
   ARRAY[CAST(a.dnhydroseq AS BIGINT),CAST(a.dnminorhyd AS BIGINT)]
 WHEN a.dndraincou > 2
 THEN
   ARRAY(SELECT CAST(cc.tohydroseq AS BIGINT) FROM cipsrv_nhdplus_m.nhdplusflow cc WHERE cc.fromhydroseq = a.hydroseq)
 ELSE
   NULL
 END AS ary_downstream_hydroseq
,CASE
 WHEN a.startflag = 1
 THEN
   TRUE
 ELSE
   FALSE
 END AS headwater
,CASE
 WHEN EXISTS (SELECT 1 FROM cipsrv_nhdplus_m.nhdplusflow dd WHERE dd.fromhydroseq = a.hydroseq AND dd.direction = 714)
 THEN
   TRUE
 ELSE
   FALSE
 END AS coastal_connection
,CASE
 WHEN EXISTS (SELECT 1 FROM cipsrv_nhdplus_m.nhdplusflow ee WHERE ee.fromhydroseq = a.hydroseq AND ee.direction = 713)
 THEN
   TRUE
 ELSE
   FALSE
 END AS network_end
,a.vpuid
FROM
cipsrv_nhdplus_m.networknhdflowline a
WHERE
    a.pathlength NOT IN (-9999,-9998)
AND a.fcode <> 56600;

ALTER TABLE cipsrv_nhdplus_m.nhdplusflowlinevaa_nav OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav TO public;

CREATE UNIQUE INDEX nhdplusflowlinevaa_nav_01u
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(nhdplusid);

CREATE UNIQUE INDEX nhdplusflowlinevaa_nav_02u
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(hydroseq);

CREATE UNIQUE INDEX nhdplusflowlinevaa_nav_03u
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(objectid);

CREATE INDEX nhdplusflowlinevaa_nav_01i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(levelpathi);

CREATE INDEX nhdplusflowlinevaa_nav_02i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(divergence);

CREATE INDEX nhdplusflowlinevaa_nav_03i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(uphydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_04i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(dnhydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_05i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(dnminorhyd);

CREATE INDEX nhdplusflowlinevaa_nav_06i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(pathlength);

CREATE INDEX nhdplusflowlinevaa_nav_07i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(pathtimema);

CREATE INDEX nhdplusflowlinevaa_nav_08i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(terminalpa);

CREATE INDEX nhdplusflowlinevaa_nav_09i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(force_main_line);

CREATE INDEX nhdplusflowlinevaa_nav_gn1
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav USING GIN(ary_upstream_hydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_gn2
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav USING GIN(ary_downstream_hydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_10i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(headwater);

CREATE INDEX nhdplusflowlinevaa_nav_11i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(coastal_connection);

CREATE INDEX nhdplusflowlinevaa_nav_12i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(network_end);

CREATE INDEX nhdplusflowlinevaa_nav_13i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_nav(vpuid);

ANALYZE cipsrv_nhdplus_m.nhdplusflowlinevaa_nav;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.nhdplusflowlinevaa_nav;

