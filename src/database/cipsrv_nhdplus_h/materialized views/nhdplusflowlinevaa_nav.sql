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
   ,streamleve
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
,CAST(a.fromnode AS BIGINT)    AS fromnode
,CAST(a.tonode AS BIGINT)      AS tonode
,CAST(a.streamleve AS INTEGER) AS streamleve
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
   ,15001300000929  -- Black Warrior River 38370
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
   ,20000600001111  -- Saint Francis River 43889
   ,20000600001753  -- White River 147417
   ,20000600001906  -- Arkansas River 819515
   ,20000600002264  -- Cache River 12841
   ,20000700000082  -- Hatchie River 19785
   ,20000700001083  -- Obion River 14563
   ,20000700001264  -- Middle Fork Forked Deer River 12059
   ,20000700001523  -- Forked Deer River 11766
   ,20000800000197  -- Red River 603498
   ,20000800000379  -- Black River 144408
   ,20000800000794  -- Saline River 16593
   ,20000800001287  -- Little Missouri River 10906
   ,20000800001571  -- Little River 19838
   ,20000800002932  -- Bayou D'Arbonne 15663
   ,20000800074456  -- Boeuf River 20885
   ,20000900000350  -- Tensas River 17600
   ,20001000003249  -- Yockanookany River 10568
   ,20001000003370  -- Tuscolameta Creek 14496
   ,21000100000651  -- Loggy Bayou 29537
   ,21000100001545  -- Sulphur River 24728
   ,21000100001646  -- Bayou Pierre 25321
   ,21000100002186  -- Little River 19933
   ,21000100003791  -- Cross Bayou 35810
   ,21000100007179  -- Saline Bayou 10783
   ,21000100007746  -- Muddy Boggy Creek 13850
   ,21000200000576  -- Illinois River, OK 18186
   ,21000200001034  -- Poteau River 10770
   ,21000200047079  -- Canadian River 190959
   ,21000200058920  -- Neosho River 94044
   ,21000300001248  -- Dugan Creek 19399
   ,21000300001930  -- Washita River 53619
   ,21000300002960  -- Wichita River 23717
   ,21000300004945  -- Little Wichita River 16331
   ,21000300005580  -- Cache Creek 19874
   ,21000300124781  -- Salt Fork Red River 11192
   ,21000400000542  -- Salt Fork Arkansas River 31837
   ,21000400021946  -- Walnut River 15514
   ,21000500055210  -- North Canadian River 65540
   ,21000700000173  -- Cimarron River 88552
   ,21000800001783  -- North Fork Red River 24407
   ,21000900005140  -- Cheyenne Creek 14253
   ,21000900007015  -- Purgatoire River 25162
   ,21000900009518  -- Huerfano River 19269
   ,21000900011413  -- Fountain Creek 19097
   ,21001000004058  -- North Carrizo Creek 11109
   ,21001200008129  -- Deep Fork 18708
   ,21001300000507  -- Verdigris River 58531
   ,21001300000707  -- Spring River 15311
   ,21001300001120  -- Elk River 14075
   ,21001300001228  -- Caney River 14731
   ,21001300001507  -- Cottonwood River 12231
   ,21001400000447  -- Black River 47701
   ,21001400000830  -- Little Red River 11239
   ,21001400001118  -- Current River 12209
   ,21001400001192  -- Spring River 16726
   ,22000100000445  -- Meramec River
   ,22000100000687  -- Kaskaskia River 25281
   ,22000100001473  -- Big Muddy River 11737
   ,22000200000332  -- Salt River 13050
   ,22000200000612  -- Fabius River 8494
   ,22000200026081  -- Missouri River
   ,22000200026537  -- Des Moines River 45229
   ,22000200027139  -- Illinois River, IL 352504
   ,22000300000988  -- Iowa River 36390
   ,22000300001179  -- Cedar River 20616
   ,22000300001684  -- Skunk River 25813
   ,22000300002964  -- North Skunk River 10578
   ,22000300045683  -- Rock River 33515
   ,22000400021144  -- Wisconsin River 41636
   ,22000500026205  -- Chippewa River 25158
   ,22000600028297  -- Minnesota River 32136
   ,22000700000978  -- Sangamon River 18794
   ,22000700001286  -- Spoon River 8369
   ,22000700001425  -- Apple Creek 13194
   ,22000700001485  -- Big Sandy Creek 11982
   ,22000800000280  -- Kankakee River 222073
   ,22000800000425  -- Iroquois River 55012
   ,22000800000653  -- Yellow River 30778
   ,22000800000997  -- Fox, IL 9961
   ,22000800001706  -- Singleton Ditch 17136
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
   ,23000300000469  -- Little Sioux River 13876
   ,23000300022693  -- Big Sioux River 27915
   ,23000400037304  -- James River 30690
   ,23000500000848  -- White River 58631
   ,23000500001390  -- Bad River 26105
   ,23000600001362  -- Cannonball River 21305
   ,23000600001712  -- Grand River 33131
   ,23000600002041  -- North Fork Grand River 19398
   ,23000600002597  -- Moreau River 36066
   ,23000600002674  -- Heart River 18002
   ,23000600003778  -- South Fork Grand River 11112
   ,23000600004994  -- Knife River 14667
   ,23000600009067  -- Cheyenne River 322263
   ,23000700000585  -- Little Missouri River 58886
   ,23000800001538  -- Poplar River 16082
   ,23000800002349  -- Redwater River 20932
   ,23000800003588  -- Big Muddy Creek 11906
   ,23000800010791  -- Milk River 136792
   ,23000800050723  -- Yellowstone River 437893
   ,23000900000736  -- Fort Peck Lake 65828
   ,23000900000807  -- Big Dry Creek 25737
   ,23000900000885  -- Flatwillow Creek 12625
   ,23000900002576  -- Armells Creek 10040
   ,23000900002746  -- Judith River 15589
   ,23001000000222  -- Big Blue River 49602
   ,23001000000382  -- Little Blue River 17273
   ,23001100000091  -- Platte River 465183
   ,23001100001964  -- South Platte River 206458
   ,23001100019240  -- Loup River 41861
   ,23001200030798  -- Bighorn River 209258
   ,23001200045188  -- Tongue River 37448
   ,23001200050941  -- Powder River 105338
   ,23001400000328  -- Thompson River 23130
   ,23001500001304  -- Marais des Cygnes River 23648
   ,23001500001309  -- Sac River 13400
   ,23001500002520  -- South Grand River 12434
   ,23001600000400  -- Republican River 76105
   ,23001600001392  -- Sappa Creek 10302
   ,23001700000783  -- Solomon River 31581
   ,23001700000903  -- Saline River 16113
   ,23001800004179  -- Laramie River 37835
   ,23001800004803  -- Medicine Bow River 15668
   ,23001800007284  -- North Laramie River 12335
   ,23001900001700  -- Lodgepole Creek 13112
   ,23001900006705  -- Crow Creek 12311
   ,23001900008410  -- Cache la Poudre River 25414
   ,23001900010115  -- North Fork Cache La Poudre River 13894
   ,23001900010292  -- Horse Creek 16505
   ,23001900010467  -- North Fork South Platte River 12328
   ,23001900010917  -- Big Thompson River 11762
   ,23002000000514  -- South Loup River 13471
   ,23002100000035  -- Elkhorn River 16768
   ,23002200001882  -- Cheyenne River weird transfer to Sage Creek
   ,23002200002255  -- Belle Fourche River 51387
   ,23002200003528  -- Lance Creek 33516
   ,23002200003892  -- Lightning Creek 21649
   ,23002200004014  -- Lodgepole Creek 16704
   ,23002200004193  -- Black Thunder Creek 38744
   ,23002200004316  -- Cherry Creek 14458
   ,23002200004486  -- Dry Fork Cheyenne River 13933
   ,23002200004694  -- Beaver Creek 28382
   ,23002200004775  -- Little Thunder Creek 13661
   ,23002200004834  -- Hat Creek 13819
   ,23002300000149  -- Niobrara River 30496
   ,23002500001890  -- Powder River weird turn to Spotted Horse Creek 41877
   ,23002500002551  -- Little Powder River 35216
   ,23002600008115  -- Greybull River 19070
   ,23002600009763  -- Shoshone River 25873
   ,23002600014035  -- Little Wind River 10386
   ,23002600014477  -- Nowood River
   ,23002700003360  -- Clarks Fork Yellowstone River 14530
   ,23002800001378  -- Beaver Creek 19424
   ,23002800001648  -- Frenchman Creek 17609
   ,23002800002388  -- Rock Creek 13130
   ,23002800002716  -- Battle Creek 10580
   ,23002900001488  -- Big Hole River 14829
   ,24000100000182  -- Ohio River 2997623
   ,24000100000288  -- Cumberland River 80553
   ,24000100000878  -- Blue River 32741
   ,24000100001166  -- Indian Creek 16430
   ,24000100001421  -- Salt River 26524
   ,24000100001664  -- Anderson River 16281
   ,24000100001720  -- Little Pigeon Creek 28157
   ,24000100001735  -- Pigeon Creek 29232
   ,24000100002176  -- Silver Creek 14662
   ,24000100164878  -- Kentucky River 45188
   ,24000200000265  -- Laughery Creek 22863
   ,24000200043089  -- Great Miami River 100515
   ,24000200051424  -- Licking River 29091
   ,24000200061270  -- Scioto River 24505
   ,24000300000482  -- Beaver River 17672
   ,24000300000556  -- Little Kanawha River 10507
   ,24000400000438  -- Kiskiminetas River 10511
   ,24000500000031  -- Monongahela River 34421
   ,24000600000037  -- Muskingum River 41357
   ,24000600000335  -- Walhonding River 10678
   ,24000700000149  -- Kanawha River 65296
   ,24000900000031  -- Big Sandy River 25314
   ,24001000000102  -- Whitewater River 83150
   ,24001000000413  -- East Fork Whitewater River 19247
   ,24001200000150  -- Green River 48522
   ,24001300000093  -- Wabash River 1530400
   ,24001300000500  -- White River 702784
   ,24001300001080  -- East Fork White River 341155
   ,24001300002122  -- Tippecanoe River 136121
   ,24001300003742  -- Mississinewa River 48917
   ,24001300003876  -- Fall Creek 21421
   ,24001300004053  -- Patoka River 61676
   ,24001300004391  -- Wildcat Creek 48717
   ,24001300004621  -- Eel River 78841
   ,24001300004892  -- Eel River 47781
   ,24001300004936  -- Muscatatuck River 70337
   ,24001300004940  -- Sugar Creek 47093
   ,24001300005686  -- Driftwood River 71014
   ,24001300006247  -- Salamonie River 33213
   ,24001300006368  -- Big Raccoon Creek 31827
   ,24001300007009  -- Sugar Creek 30042
   ,24001300007049  -- Salt Creek 36435
   ,24001300007490  -- Lost River 21025
   ,24001300007540  -- Vernon Fork Muscatatuck River 25417
   ,24001300007933  -- Big Walnut Creek 20459
   ,24001300008224  -- Deer Creek 17784
   ,24001300008266  -- South Fork Wildcat Creek 22320
   ,24001300008834  -- Sand Creek 15631
   ,24001300009034  -- Pipe Creek 11760
   ,24001300009953  -- Coal Creek 15695
   ,24001300010204  -- Clifty Creek 12022
   ,24001300010862  -- Big Pine Creek 19397
   ,24001300011674  -- Little Wabash River 15138
   ,24001300012342  -- White Lick Creek 20992
   ,24001300012666  -- North Fork Salt Creek 10086
   ,24001300012845  -- Beanblossom Creek 12014
   ,24001300012997  -- Eagle Creek 15051
   ,24001300013571  -- Little River 19033
   ,24001300013847  -- Cicero Creek 12714
   ,24001300014289  -- Plummer Creek 10194
   ,24001300015223  -- Big Creek 15898
   ,24001300016450  -- Vermilion River 11962
   ,24001300017322  -- Wea Creek 10280
   ,24001300017946  -- Busseron Creek 17309
   ,24001300020228  -- Big Monon Ditch 14581
   ,24001400001478  -- Caney Fork River 10182
   ,25000100000172  -- Tennessee River 426936
   ,25000100000474  -- Duck River 27330
   ,25000200000389  -- Elk River 14859
   ,25000300000173  -- Hiwassee River 18713
   ,25000400001850  -- Holston River 35557
   ,25000400001882  -- Clinch River 31829
   ,25000400002206  -- Little Tennessee River 24199
   ,25000400002665  -- Nolichucky River 23581
   ,25000400002881  -- Pigeon River 12261
   ,25000400004841  -- Ivy Creek 10337
   ,30000300001641  -- Llano River 24923
   ,30000300001954  -- Concho River 16591
   ,30000300002066  -- San Saba River 15008
   ,30000300002289  -- Pecan Bayou 21088
   ,30000300002462  -- Pedernales River 10544
   ,30000500001667  -- Frio River 38884
   ,30000600002052  -- Richland Creek 15214
   ,30000600002151  -- Elm Fork Trinity River 22767
   ,30000600002187  -- East Fork Trinity River 10606
   ,30000600002283  -- Cedar Creek 13160
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
   ,41000700000235  -- Gunnison River 66985
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
   ,55000300090644  -- Willamette River 265607
   ,55000300096893  -- Multnomah Channel 272776
   ,55000400001056  -- John Day River 119247
   ,55000400001939  -- Deschutes River 66876
   ,55000400003137  -- Crooked River 40028
   ,55000400003730  -- North Fork John Day River 32606
   ,55000400003814  -- Umatilla River
   ,55000400005356  -- Walla Walla River 13599
   ,55000500000556  -- Lower Crab Creek 14434
   ,55000500003511  -- Columbia River 139143
   ,55000500004552  -- Wood River 13230
   ,55000500005090  -- Okanogan River 65204
   ,55000500005645  -- Kettle River 36329
   ,55000500006119  -- Similkameen River 30718
   ,55000500008143  -- Methow River 17281
   ,55000500008443  -- Sanpoil River 20592
   ,55000500010112  -- Wenatchee River
   ,55000500010429  -- Colville River 11412
   ,55000500182580  -- Snake River 494546
   ,55000500192124  -- Yakima River 60988
   ,55000500209416  -- Pend-d'Oreille River 114979
   ,55000600001480  -- Salmon River 56300
   ,55000600002174  -- Grande Ronde River 55285
   ,55000600002227  -- Clearwater River 46683
   ,55000600002351  -- Imnaha River 23683
   ,55000600003400  -- Palouse River 16954
   ,55000600003502  -- Wallowa River 22277
   ,55000700001591  -- Malheur River 66256
   ,55000700001595  -- Owyhee River 40560
   ,55000700002365  -- Boise River 17599
   ,55000700003318  -- Powder River 13259
   ,55000700003369  -- Payette River 16073
   ,55000700003731  -- Weiser River 16670
   ,55000700010460  -- Bruneau River 12778
   ,55000900000980  -- McKenzie River 29308
   ,55000900000983  -- Santiam River 72524
   ,55000900001458  -- North Santiam River 26069
   ,55000900001473  -- Clackamas River 21207
   ,55000900002448  -- Middle Santiam River 19218
   ,55000900002762  -- Molalla River 20616
   ,55000900002940  -- Yamhill River 21132
   ,55000900003044  -- Coast Fork Willamette River 17230
   ,55000900003646  -- Tualatin River 25026
   ,55000900003751  -- Row River 12181
   ,55000900005235  -- Dairy Creek 13468
   ,55001100001629  -- Duncan River 28891
   ,55001100002782  -- Clark Fork 84550
   ,55001100003553  -- Spokane River 52013
   ,55001100003696  -- Elk River 17755
   ,55001100006916  -- Slocan River 14826
   ,55001100011213  -- Little Spokane River 10735
   ,55001200001961  -- Clover Creek 11959
   ,55001200002020  -- Malad River 11237
   ,55001200003127  -- Rock Creek 62227
   ,55001200006271  -- Blackfoot River 10191
   ,55001200007442  -- Henrys Fork Snake River 10040
   ,60000200000546  -- Missisquoi River 27040
   ,60000200000599  -- Otter Creek 14686
   ,60000200000960  -- Winooski River 10956
   ,60000300000594  -- Oswegatchie River 11929
   
   -- Breakers
   ,21000200000358  -- Arkansas at Illinois Bayou
   ,22000100000212  -- Mississippi at Ohio
   ,22000200000040  -- Mississippi at Missouri
   ,22000400000399  -- Mississippi at Wisconsin
   ,22000700001518  -- Illinois at Mackinaw
   ,22001100001517  -- Wisconsin at Big Eau Pleine
   ,23000300000321  -- Missouri at Platte
   ,23002700001358  -- Yellowstone at Pryor Creek
   ,24000200000364  -- Ohio at Cincinnati
   ,35000300002711  -- Rio Grande at Rio Conchos
   ,40000200003815  -- Colorado at Havasu
   ,55000400000895  -- Columbia at Eagle Creek
   ,55000400001036  -- Columbia at Deschutes
   ,60002700000471  -- Fox at Fond du Lac
   
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

