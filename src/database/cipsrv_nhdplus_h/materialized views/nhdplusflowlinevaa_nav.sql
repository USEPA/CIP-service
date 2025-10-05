DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdplusflowlinevaa_nav;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(
    objectid
   ,nhdplusid
   ,hydroseq
   ,fmeasure
   ,tmeasure
   ,levelpathi
   ,terminalpa
   ,arbolatesu
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
   ,ary_upstream_hydromains
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
 CAST(a.objectid   AS INTEGER) AS objectid
,CAST(a.nhdplusid  AS BIGINT)  AS nhdplusid
,CAST(a.hydroseq   AS BIGINT)  AS hydroseq 
,CAST(a.frommeas   AS NUMERIC) AS frommeas
,CAST(a.tomeas     AS NUMERIC) AS tomeas
,CAST(a.levelpathi AS BIGINT)  AS levelpathi
,CAST(a.terminalpa AS BIGINT)  AS terminalpa
,a.arbolatesu
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
    10000200005013  -- Maurice River 10963 
   ,10000500000548  -- Shenandoah River 21700
   ,10000500000765  -- South Fork Shenandoah River 12047 
   ,10000600000411  -- West Branch Susquehanna River 29280
   ,10000600000673  -- Juniata River 15304
   ,10000700000321  -- Mohawk River 14159
   ,10000700000636  -- Rondout Creek 10242
   ,15001300000607  -- Tombigbee River
   ,15001300001726  -- Sucarnoochee River 18382
   ,15001700000666  -- Tallapoosa River 32481
   ,15001700001463  -- Etowah River 11340
   ,20000400000250  -- Big Black River 46379
   ,20000500000071  -- Yazoo River 170075
   ,20000500000729  -- Coldwater River 58709
   ,20000500001089  -- Steele Bayou 20123
   ,20000500001351  -- Yalobusha River 30261
   ,20000500001664  -- Little Tallahatchie River 35056
   ,20000500001871  -- Yocona River 17473
   ,20000500076357  -- Arkansas River 840641
   ,20000600001111  -- Saint Francis River 43889
   ,20000600001753  -- White River 147417
   ,20000600002264  -- Cache River 12841
   ,20000700000082  -- Hatchie River 19785
   ,20000700001083  -- Obion River 14563
   ,20000700001264  -- Middle Fork Forked Deer River 12059
   ,21000200000576  -- Illinois River 18186
   ,21000200001034  -- Poteau River 10770
   ,21000200047079  -- Canadian River 190959
   ,21000200058920  -- Neosho River 94044
   ,21000400000542  -- Salt Fork Arkansas River 31837
   ,21000400021946  -- Walnut River 15514
   ,21000700000173  -- Cimarron River 88552
   ,21000900005140  -- Cheyenne Creek 14253
   ,21001300000507  -- Verdigris River 58531
   ,21001400000447  -- Black River 47701
   ,21001400000830  -- Little Red River 11239
   ,21001400001118  -- Current River 12209
   ,21001400001192  -- Spring River 16726
   ,22000100000445  -- Meramec River
   ,22000100000687  -- Kaskaskia River 25281
   ,22000100001473  -- Big Muddy River 11737
   ,22000200000332  -- Salt River 13050
   ,22000200026081  -- Missouri River
   ,22000200027139  -- Illinois River 352504
   ,22000300000988  -- Iowa River 36390
   ,22000300001684  -- Skunk River 25813
   ,22000300045683  -- Rock River 33515
   ,22000400021144  -- Wisconsin River 41636
   ,22000600028297  -- Minnesota River 32136
   ,22001200000203  -- Saint Croix River 14743
   ,23000100000450  -- Lamine River 15089
   ,23000100031638  -- Grand River 80062
   ,23000100034279  -- Gasconade River 20581
   ,23000100036670  -- Kansas River 238078
   ,23000100037336  -- Osage River 101705
   ,23000100038307  -- Chariton River 12971
   ,23000200000143  -- Platte River 27476
   ,23000200000582  -- Nodaway River 12750
   ,23000200000832  -- Nishnabotna River 13583
   ,23000200001259  -- Big Nemaha River 11109
   ,23001000000222  -- Big Blue River 49602
   ,23001500001309  -- Sac River 13400
   ,23001600000400  -- Republican River 76105
   ,23001700000783  -- Solomon River 31581
   ,23001700000903  -- Saline River 16113
   ,24000100000182  -- Ohio River 2997623
   ,24000100000878  -- Blue River 32741
   ,24000100001166  -- Indian Creek 16430
   ,24000100001421  -- Salt River 26524
   ,24000100001664  -- Anderson River 16281
   ,24000100001720  -- Little Pigeon Creek 28157
   ,24000100001735  -- Pigeon Creek 29232
   ,24000100002176  -- Silver Creek 14662
   ,24001200000150  -- Green River 48522
   ,24001300000093  -- Wabash River 1530400
   ,24001300004940  -- Sugar Creek 47093
   ,24001300016450  -- Vermilion River 11962
   ,25000100000172  -- Tennessee River 426936
   ,35000200000828  -- Rio San Rodrigo 10295
   ,35000300002413  -- Dry Mexican network southwest of Big Bend National Park 26115
   ,35000300003120  -- Devils River 15710
   ,35000300003402  -- Dry Mexican network north and west of Lake Amistad 12130
   ,35000300003526  -- Dry Mexican network south of Big Bend National Park 13322
   ,35000300003658  -- Dry Mexican network southwest of Big Bend Ranch State Park 15182
   ,35000300003673  -- Dry Mexican network over Manuel Benevidas 14976
   ,35000300004180  -- Mexican Branch off Lake Amistad 28284
   ,35000300004330  -- Subnetwork off 35000300004180 14976
   ,35000300150490  -- Pecos River 80139
   ,35000600003441  -- Rio Puerco 15343
   ,35000600003604  -- Rio Chama 29287
   ,35000900000871  -- Irrigation system south of Alamos 10819
   ,35000900000877  -- Irrigation system south of Alamos 10742
   ,40000200003048  -- Bill Williams River 19363
   ,40000200024935  -- Gila River 220361
   ,40000300001646  -- Gila Bend Dry network 19992
   ,40000300001649  -- Gila River Branch 
   ,40000300002908  -- Gila Bend Canal 19052
   ,40000400001442  -- San Pedro River 26717
   ,40000400001798  -- Santa Cruz Wash 19414
   ,40000400001867  -- Santa Rosa Wash 15414
   ,40000400001927  -- Greene Wash 11814
   ,40000400002152  -- Greene Wash 11532
   ,40000500002081  -- Virgin River 55578
   ,40000500002991  -- Muddy River 12374
   ,40000500003138  -- Fort Pearce Wash 12525
   ,40000600000358  -- Salt River 50267
   ,40000600000495  -- Verde River 22365
   ,40000700001137  -- San Francisco River 15379
   ,40000800001710  -- Little Colorado River 67238
   ,40000800002035  -- Zuñi River 13070
   ,40000800002095  -- Puerco River 12755
   ,41000100000336  -- Dirty Devil River 15045
   ,41000100026157  -- Green River 252327
   ,41000200000431  -- Dolores River 40825
   ,41000200001099  -- San Miguel River 13123
   ,41000300003232  -- Eagle River 14918
   ,41000300003236  -- Roaring Fork River
   ,41000300023342  -- The Redlands Power Canal 67059
   ,41000400001044  -- Duchesne River 12826
   ,41000400001537  -- Unnamed Wash south of Uteland Butte 12357
   ,41000400036945  -- White River 35730
   ,41000500001847  -- Blacks Fork 21423
   ,41000600001146  -- San Juan River 165052
   ,41000600002633  -- Chaco River 17908
   ,41000600003089  -- Farmington Glade 31731
   ,41000600003098  -- Animas River 31502
   ,41000600003398  -- Largo Wash 11259
   ,41000600004720  -- Chinle Creek 12956
   ,41000600004731  -- Piedra River 11192
   ,41000700001915  -- North Fork Gunnison River 12092
   ,41000800000558  -- Yampa River 71407
   ,41000800000869  -- Little Snake River 31838
   ,50000900001311  -- Cache Slough 70455
   ,50000900002383  -- Feather River 169900
   ,50000900004145  -- Cottonwood Creek 15237
   ,50000900004481  -- Pit River 51343
   ,50000900006273  -- American River 54548
   ,50000900010491  -- McCloud River 12447
   ,55000100002387  -- Smith River
   ,55000300000273  -- Youngs River 18129
   ,55000300000494  -- Cowlitz River 82336
   ,55000300000624  -- Sandy River 15318
   ,55000300000833  -- Toutle River 21718
   ,55000300001166  -- Lewis River 28482
   ,55000300001330  -- Kalama River 11020
   ,55000300002121  -- Deep River 13231
   ,55000300002134  -- Grays River 11142
   ,55000300096893  -- Multnomah Channel 272776
   ,55000400001056  -- John Day River 119247
   ,55000400001939  -- Deschutes River 66876
   ,55000400003730  -- North Fork John Day River 32606
   ,55000400003814  -- Umatilla River
   ,55000400005356  -- Walla Walla River 13599
   ,55000500003511  -- Columbia River 139143
   ,55000500005090  -- Okanogan River 65204
   ,55000500005645  -- Kettle River 36329
   ,55000500008143  -- Methow River 17281
   ,55000500008443  -- Sanpoil River 20592
   ,55000500010112  -- Wenatchee River
   ,55000500010429  -- Colville River 11412
   ,55000500182580  -- Snake River 494546
   ,55000500192124  -- Yakima River 60988
   ,55000500209416  -- Pend-d'Oreille River 114979
   ,55000600002174  -- Grande Ronde River 55285
   ,55000600002351  -- Imnaha River 23683
   ,55000600003400  -- Palouse River 16954
   ,55000700001591  -- Malheur River 66256
   ,55000700003318  -- Powder River 13259
   ,55000700003731  -- Weiser River 16670
   ,55000700010460  -- Bruneau River 12778
   ,55000900000980  -- McKenzie River 29308
   ,55000900000983  -- Santiam River 72524
   ,55000900001458  -- North Santiam River 26069
   ,55000900001473  -- Clackamas River 21207
   ,55000900002448  -- Middle Santiam River 19218
   ,55000900002762  -- Molalla River 20616
   ,55000900002940  -- Yamhill River 21132
   ,55000900003646  -- Tualatin River 25026
   ,55001100001629  -- Duncan River 28891
   ,55001100003553  -- Spokane River 52013
   ,55001100003696  -- Elk River 17755
   ,55001100006916  -- Slocan River 14826
   ,55001100011213  -- Little Spokane River 10735
   ,55001200001961  -- Clover Creek 11959
   ,55001200006271  -- Blackfoot River 10191
   ,55001200007442  -- Henrys Fork Snake River 10040
 )
 THEN
   TRUE
 ELSE
   FALSE
 END AS force_main_line
,ARRAY(
   SELECT 
   CAST(bb.fromhydroseq AS BIGINT) 
   FROM 
   cipsrv_nhdplus_h.nhdplusflow bb 
   WHERE 
   bb.tohydroseq = a.hydroseq
 ) AS ary_upstream_hydroseq
,ARRAY(
   SELECT 
   CAST(ff.fromhydroseq AS BIGINT) 
   FROM 
   cipsrv_nhdplus_h.nhdplusflow ff 
   JOIN
   cipsrv_nhdplus_h.networknhdflowline gg
   ON
       gg.hydroseq    = ff.fromhydroseq
   AND gg.dnhydroseq  = ff.tohydroseq 
   WHERE 
   ff.tohydroseq = a.hydroseq
 ) AS ary_upstream_hydromains
,CASE
 WHEN a.dndraincou = 1
 THEN
   ARRAY[CAST(a.dnhydroseq AS BIGINT)]
 WHEN a.dndraincou = 2
 THEN
   ARRAY[CAST(a.dnhydroseq AS BIGINT),CAST(a.dnminorhyd AS BIGINT)]
 WHEN a.dndraincou > 2
 THEN
   ARRAY(
      SELECT 
      CAST(cc.tohydroseq AS BIGINT) 
      FROM 
      cipsrv_nhdplus_h.nhdplusflow cc 
      WHERE 
      cc.fromhydroseq = a.hydroseq
   )
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
 WHEN EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.nhdplusflow dd WHERE dd.fromhydroseq = a.hydroseq AND dd.direction = 714)
 THEN
   TRUE
 ELSE
   FALSE
 END AS coastal_connection
,CASE
 WHEN EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.nhdplusflow ee WHERE ee.fromhydroseq = a.hydroseq AND ee.direction = 713)
 THEN
   TRUE
 ELSE
   FALSE
 END AS network_end
/* ++++++++++ */
,a.vpuid
FROM
cipsrv_nhdplus_h.networknhdflowline a
WHERE
    a.pathlength NOT IN (-9999,-9998)
AND a.fcode <> 56600;

ALTER TABLE cipsrv_nhdplus_h.nhdplusflowlinevaa_nav OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav TO public;

CREATE UNIQUE INDEX nhdplusflowlinevaa_nav_01u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(nhdplusid);

CREATE UNIQUE INDEX nhdplusflowlinevaa_nav_02u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(hydroseq);

CREATE UNIQUE INDEX nhdplusflowlinevaa_nav_03u
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(objectid);

CREATE INDEX nhdplusflowlinevaa_nav_01i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(levelpathi);

CREATE INDEX nhdplusflowlinevaa_nav_02i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(divergence);

CREATE INDEX nhdplusflowlinevaa_nav_03i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(uphydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_04i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(dnhydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_05i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(dnminorhyd);

CREATE INDEX nhdplusflowlinevaa_nav_06i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(pathlength);

CREATE INDEX nhdplusflowlinevaa_nav_07i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(pathtimema);

CREATE INDEX nhdplusflowlinevaa_nav_08i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(terminalpa);

CREATE INDEX nhdplusflowlinevaa_nav_09i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(force_main_line);

CREATE INDEX nhdplusflowlinevaa_nav_gn1
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav USING GIN(ary_upstream_hydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_gn2
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav USING GIN(ary_upstream_hydromains);

CREATE INDEX nhdplusflowlinevaa_nav_gn3
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav USING GIN(ary_downstream_hydroseq);

CREATE INDEX nhdplusflowlinevaa_nav_10i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(headwater);

CREATE INDEX nhdplusflowlinevaa_nav_11i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(coastal_connection);

CREATE INDEX nhdplusflowlinevaa_nav_12i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(network_end);

CREATE INDEX nhdplusflowlinevaa_nav_13i
ON cipsrv_nhdplus_h.nhdplusflowlinevaa_nav(vpuid);

ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_nav;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdplusflowlinevaa_nav;

