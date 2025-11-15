

function dzStr(val) {
   if (val == 0 ) {
      return "0";
   } else if (!val) {
      return "";
   } else {
      return val.toString().replace(/,/g,"");
   }
} 


function init_layer_items() {
   
   if (
      typeof gis_host === "undefined" ||
      gis_host === null ||
      gis_host == "NONE"
   ) {
      null;
   } else {
      zgis_host = gis_host;
      
      if (
         typeof gis_type === "undefined" ||
         gis_type === null ||
         gis_type == "NONE"
      ) {
         zgis_type = "geoserver";
      } else {
         zgis_type = gis_type;
      }
   
      if (
         typeof gis_prot === "undefined" ||
         gis_prot === null ||
         gis_prot == "NONE"
      ) {
         zgis_prot = "http";
      } else {
         zgis_prot = gis_prot;
      }
      
      if (
         typeof gis_port === "undefined" ||
         gis_port === null ||
         gis_port == "NONE"
      ) {
         zgis_port = "";
      } else {
         zgis_port = ":" + gis_port;
      }
      
      if ( zgis_type ) {
         let gis_server = zgis_prot + "://" + zgis_host + zgis_port;
         
         if (typeof gis_pref === "undefined" || gis_pref === null || gis_pref == "NONE") {
            // pass
         } else {
            gis_server = zgis_prot + "://" + zgis_host + zgis_port + '/' + gis_pref
         }
         
         if ( zgis_type == "geoserver" ) {
            
            lyr_nhdplus_m_flowlines = L.tileLayer.wms(gis_server + "/wms",{
                layers: 'cipsrv:nhdplus_m_flowlines'
               ,format: 'image/png'
               ,transparent: true
               ,version: '1.3.0'
               ,attribution: "US EPA"
            });
            
            lyr_nhdplus_h_flowlines = L.tileLayer.wms(gis_server + "/wms",{
                layers: 'cipsrv:nhdplus_h_flowlines'
               ,format: 'image/png'
               ,transparent: true
               ,version: '1.3.0'
               ,attribution: "US EPA"
            });
                        
         } else if ( zgis_type == "arcgis" ) {
            
            const usgs_fcode = [
                {
                   "ID": 0
                  ,"source": {
                      "type"      : "mapLayer"
                     ,"mapLayerId": 0
                   }
                }
               ,{
                   "ID": 4
                  ,"source": {
                      "type"      : "mapLayer"
                     ,"mapLayerId": 4
                   }
                  ,"drawingInfo":{
                     "renderer": {
                        "type": "uniqueValue",
                        "field1": "ftype",
                        "field2": "fcode",
                        "uniqueValueGroups": [
                         {
                          "heading": "FType, FCode",
                          "classes": [
                           {
                            "label": "Perennial",
                            "description": "Perennial",
                            "symbol": {
                             "type": "esriSLS",
                             "style": "esriSLSSolid",
                             "color": [
                              74,
                              182,
                              198,
                              255
                             ],
                             "width": 1.5
                            },
                            "values": [
                             [
                              "460",
                              "46000"
                             ],
                             [
                              "460",
                              "46006"
                             ]
                            ]
                           },
                           {
                            "label": "Intermittent",
                            "description": "Intermittent",
                            "symbol": {
                             "type": "esriSLS",
                             "style": "esriSLSSolid",
                             "color": [
                              74,
                              182,
                              198,
                              255
                             ],
                             "width": 1.5
                            },
                            "values": [
                             [
                              "460",
                              "46003"
                             ]
                            ]
                           },
                           {
                            "label": "Ephemeral",
                            "description": "Ephemeral",
                            "symbol": {
                             "type": "esriSLS",
                             "style": "esriSLSSolid",
                             "color": [
                              136,
                              92,
                              47,
                              255
                             ],
                             "width": 1.5
                            },
                            "values": [
                             [
                              "460",
                              "46007"
                             ]
                            ]
                           },
                           {
                            "label": "Artificial Path",
                            "description": "Artificial Path",
                            "symbol": {
                             "type": "esriSLS",
                             "style": "esriSLSSolid",
                             "color": [
                              173,
                              0,
                              132,
                              255
                             ],
                             "width": 1.5
                            },
                            "values": [
                             [
                              "558",
                              "0"
                             ],
                             [
                              "558",
                              "55800"
                             ]
                            ]
                           },
                           {
                            "label": "Drainageway",
                            "description": "Drainageway",
                            "symbol": {
                             "type": "esriSLS",
                             "style": "esriSLSSolid",
                             "color": [
                              130,
                              130,
                              130,
                              255
                             ],
                             "width": 1.5
                            },
                            "values": [
                             [
                              "468",
                              "46800"
                             ]
                            ]
                           },
                           {
                            "label": "Canal Ditch",
                            "description": "Canal Ditch",
                            "symbol": {
                             "type": "esriSLS",
                             "style": "esriSLSSolid",
                             "color": [
                              255,
                              170,
                              0,
                              255
                             ],
                             "width": 1.5
                            },
                            "values": [
                             [
                              "336",
                              "0"
                             ],
                             [
                              "336",
                              "33400"
                             ],
                             [
                              "336",
                              "33600"
                             ],
                             [
                              "336",
                              "33601"
                             ],
                             [
                              "336",
                              "33603"
                             ]
                            ]
                           },
                           {
                            "label": "Coastline",
                            "description": "Coastline",
                            "symbol": {
                             "type": "esriSLS",
                             "style": "esriSLSSolid",
                             "color": [
                              115,
                              0,
                              0,
                              255
                             ],
                             "width": 1.5
                            },
                            "values": [
                             [
                              "566",
                              "56600"
                             ]
                            ]
                           },
                           {
                            "label": "Connector",
                            "description": "Connector",
                            "symbol": {
                             "type": "esriSLS",
                             "style": "esriSLSSolid",
                             "color": [
                              0,
                              0,
                              0,
                              255
                             ],
                             "width": 1.5
                            },
                            "values": [
                             [
                              "334",
                              "33400"
                             ]
                            ]
                           },
                           {
                            "label": "Pipeline",
                            "description": "Pipeline",
                            "symbol": {
                             "type": "esriSLS",
                             "style": "esriSLSSolid",
                             "color": [
                              115,
                              36,
                              0,
                              255
                             ],
                             "width": 1.5
                            },
                            "values": [
                             [
                              "428",
                              "42800"
                             ],
                             [
                              "428",
                              "42816"
                             ],
                             [
                              "428",
                              "42801"
                             ],
                             [
                              "428",
                              "42802"
                             ],
                             [
                              "428",
                              "42803"
                             ],
                             [
                              "428",
                              "42804"
                             ],
                             [
                              "428",
                              "42814"
                             ],
                             [
                              "428",
                              "42805"
                             ],
                             [
                              "428",
                              "42806"
                             ],
                             [
                              "428",
                              "42807"
                             ],
                             [
                              "428",
                              "42808"
                             ],
                             [
                              "428",
                              "42815"
                             ],
                             [
                              "428",
                              "42809"
                             ],
                             [
                              "428",
                              "42810"
                             ],
                             [
                              "428",
                              "42811"
                             ],
                             [
                              "428",
                              "42812"
                             ],
                             [
                              "428",
                              "42813"
                             ],
                             [
                              "428",
                              "42820"
                             ],
                             [
                              "428",
                              "42822"
                             ],
                             [
                              "428",
                              "42823"
                             ],
                             [
                              "428",
                              "42824"
                             ]
                            ]
                           },
                           {
                            "label": "Underground Conduit",
                            "description": "Underground Conduit",
                            "symbol": {
                             "type": "esriSLS",
                             "style": "esriSLSSolid",
                             "color": [
                              74,
                              113,
                              0,
                              255
                             ],
                             "width": 1.5
                            },
                            "values": [
                             [
                              "420",
                              "42000"
                             ],
                             [
                              "420",
                              "42003"
                             ],
                             [
                              "420",
                              "42001"
                             ],
                             [
                              "420",
                              "42002"
                             ]
                            ]
                           }
                          ]
                         }
                        ],
                        "uniqueValueInfos": [
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            74,
                            182,
                            198,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "460,46000",
                          "label": "Perennial"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            74,
                            182,
                            198,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "460,46006",
                          "label": "Perennial"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            74,
                            182,
                            198,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "460,46003",
                          "label": "Intermittent"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            136,
                            92,
                            47,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "460,46007",
                          "label": "Ephemeral"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            173,
                            0,
                            132,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "558,0",
                          "label": "Artificial Path"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            173,
                            0,
                            132,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "558,55800",
                          "label": "Artificial Path"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            130,
                            130,
                            130,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "468,46800",
                          "label": "Drainageway"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            255,
                            170,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "336,0",
                          "label": "Canal Ditch"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            255,
                            170,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "336,33400",
                          "label": "Canal Ditch"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            255,
                            170,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "336,33600",
                          "label": "Canal Ditch"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            255,
                            170,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "336,33601",
                          "label": "Canal Ditch"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            255,
                            170,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "336,33603",
                          "label": "Canal Ditch"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            0,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "566,56600",
                          "label": "Coastline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            0,
                            0,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "334,33400",
                          "label": "Connector"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42800",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42816",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42801",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42802",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42803",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42804",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42814",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42805",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42806",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42807",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42808",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42815",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42809",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42810",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42811",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42812",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42813",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42820",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42822",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42823",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            115,
                            36,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "428,42824",
                          "label": "Pipeline"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            74,
                            113,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "420,42000",
                          "label": "Underground Conduit"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            74,
                            113,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "420,42003",
                          "label": "Underground Conduit"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            74,
                            113,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "420,42001",
                          "label": "Underground Conduit"
                         },
                         {
                          "symbol": {
                           "type": "esriSLS",
                           "style": "esriSLSSolid",
                           "color": [
                            74,
                            113,
                            0,
                            255
                           ],
                           "width": 1.5
                          },
                          "value": "420,42002",
                          "label": "Underground Conduit"
                         }
                        ],
                        "fieldDelimiter": ",",
                        "authoringInfo": {
                         "colorRamp": {
                          "type": "multipart",
                          "colorRamps": [
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             64,
                             78,
                             207,
                             255
                            ],
                            "toColor": [
                             64,
                             78,
                             207,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             193,
                             205,
                             247,
                             255
                            ],
                            "toColor": [
                             193,
                             205,
                             247,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             137,
                             150,
                             245,
                             255
                            ],
                            "toColor": [
                             137,
                             150,
                             245,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             47,
                             50,
                             214,
                             255
                            ],
                            "toColor": [
                             47,
                             50,
                             214,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             137,
                             140,
                             196,
                             255
                            ],
                            "toColor": [
                             137,
                             140,
                             196,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             73,
                             95,
                             196,
                             255
                            ],
                            "toColor": [
                             73,
                             95,
                             196,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             116,
                             116,
                             247,
                             255
                            ],
                            "toColor": [
                             116,
                             116,
                             247,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             162,
                             180,
                             250,
                             255
                            ],
                            "toColor": [
                             162,
                             180,
                             250,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             88,
                             86,
                             245,
                             255
                            ],
                            "toColor": [
                             88,
                             86,
                             245,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             99,
                             118,
                             194,
                             255
                            ],
                            "toColor": [
                             99,
                             118,
                             194,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             165,
                             170,
                             207,
                             255
                            ],
                            "toColor": [
                             165,
                             170,
                             207,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             128,
                             124,
                             235,
                             255
                            ],
                            "toColor": [
                             128,
                             124,
                             235,
                             255
                            ]
                           },
                           {
                            "type": "algorithmic",
                            "algorithm": "esriCIELabAlgorithm",
                            "fromColor": [
                             46,
                             56,
                             199,
                             255
                            ],
                            "toColor": [
                             46,
                             56,
                             199,
                             255
                            ]
                           }
                          ]
                         }
                        }
                       }
                     ,"showLabels": false
                   }                  
                }
            ];
            
            const dynlyr = [
                {
                   "ID": 0
                  ,"source": {
                      "type"      : "mapLayer"
                     ,"mapLayerId": 0
                   }
                }
               ,{
                   "ID": 4
                  ,"source": {
                      "type"      : "mapLayer"
                     ,"mapLayerId": 4
                   }
                  ,"drawingInfo":{
                      "renderer": {
                         "type"  : "simple"
                        ,"symbol": {
                            "type" : "esriSLS"
                           ,"style": "esriSLSSolid"
                           ,"color": [65,107,223,198]
                           ,"width": 1.5
                         }
                      }
                     ,"showLabels": false
                   }                  
                }
            ];
            
            pfunc = function(error,featureCollection) {
               if (error || featureCollection.features.length === 0) {
                  // pass
               } else {
                  for (let i = 0; i < featureCollection.features.length; i++) {
                     if (featureCollection.features[i].layerId == 4) {
                        const z = "<B>NHDPlus " + featureCollection.features[i].properties.Resolution + " Resolution</B><BR/>" +
                        "NHDPlusID: " +
                        dzStr(featureCollection.features[i].properties.NHDPlusID) +
                        "<BR/>" +
                        "Perm ID: " +
                        featureCollection.features[i].properties.Permanent_Identifier +
                        "<BR/>" +
                        "FCode: " +
                        dzStr(featureCollection.features[i].properties.FCode) +
                        "<BR/>" +
                        "Reach Code: " +
                        featureCollection.features[i].properties.ReachCode +
                        "<BR/>" +
                        "Hydro Seq: " +
                        dzStr(featureCollection.features[i].properties.HydrologicSequence) +
                        "<BR/>" +
                        "FMeasure: " +
                        dzStr(featureCollection.features[i].properties.FromMeasure) +
                        "<BR/>" +
                        "TMeasure: " +
                        dzStr(featureCollection.features[i].properties.ToMeasure) +
                        "<BR/>" +
                        "Down Hydro Seq: " +
                        dzStr(featureCollection.features[i].properties.DownstreamMainPathHydroSeq) +
                        "<BR/>" +
                        "Terminal Path ID: " +
                        dzStr(featureCollection.features[i].properties.TerminalPathIdentifier) +
                        "<BR/>" +
                        "GNIS Name: " +
                        featureCollection.features[i].properties.GNIS_Name +
                        "<BR/>" +
                        "Length (Km): " +
                        dzStr(featureCollection.features[i].properties.LengthKm) +
                        "<BR/>" +
                        "FlowTime (Day): " +
                        dzStr(featureCollection.features[i].properties.TimeOfTravelMeanAverage) +
                        "<BR/>";
                        return z;
                     }
                  }
               }
            }
            
            lyr_nhdplus_m_flowlines = L.esri.dynamicMapLayer({
                url: gis_server + "/rest/services/nhdplus_m/MapServer/"
               ,layers: [0,4]
               ,dynamicLayers: usgs_fcode
            });            
            lyr_nhdplus_m_flowlines.bindPopup(pfunc);
            
            lyr_nhdplus_h_flowlines = L.esri.dynamicMapLayer({
                url: gis_server + "/rest/services/nhdplus_h/MapServer/"
               ,layers: [0,4]
               ,dynamicLayers: usgs_fcode
            });            
            lyr_nhdplus_h_flowlines.bindPopup(pfunc);
         
         } else {
            console.log('err ' + zgis_type);
            
         }
         
         const nv = get_select_values(document.getElementById("p_nhdplus_version"));
         
         if (nv == "nhdplus_m" ) {
            lyr_nhdplus_m_flowlines.addTo(map);
         }         
         layer_items["MR Flowlines"] = lyr_nhdplus_m_flowlines;
         
         if (nv == "nhdplus_h" ) {
            lyr_nhdplus_h_flowlines.addTo(map);
         }
         layer_items["HR Flowlines"] = lyr_nhdplus_h_flowlines;
         
      }
      
   }
   
}

function onEachFeature_snap_path(feature,layer) {
   if (feature.properties && feature.properties.snap_distancekm) {
      layer.bindPopup(
         "Length (Km): " +
         dzStr(feature.properties.snap_distancekm)
      );
   }
}

function onEachFeature_flowlines(feature,layer) {
   if (feature.properties && feature.properties.nhdplusid) {
      layer.bindPopup(
         "<B>Navigation Results</B><BR/>" +
         "NHDPlusID: " +
         dzStr(feature.properties.nhdplusid) +
         "<BR/>" +
         "Perm ID: " +
         feature.properties.permanent_identifier +
         "<BR/>" +
         "FCode: " +
         dzStr(feature.properties.fcode) +
         "<BR/>" +
         "Reach Code: " +
         feature.properties.reachcode +
         "<BR/>" +
         "Hydro Seq: " +
         dzStr(feature.properties.hydroseq) +
         "<BR/>" +
         "FMeasure: " +
         dzStr(feature.properties.fmeasure) +
         "<BR/>" +
         "TMeasure: " +
         dzStr(feature.properties.tmeasure) +
         "<BR/>" +
         "Down Hydro Seq: " +
         dzStr(feature.properties.dnhydroseq) +
         "<BR/>" +
         "Terminal Path ID: " +
         dzStr(feature.properties.terminalpa) +
         "<BR/>" +
         "GNIS Name: " +
         feature.properties.gnis_name +
         "<BR/>" +
         "Length (Km): " +
         dzStr(feature.properties.lengthkm) +
         "<BR/>" +
         "Network Distance (Km): " +
         dzStr(feature.properties.network_distancekm) +
         "<BR/>" 
      );
   }
}

function httpGet(url,callback) {
   var xmlHttp = new XMLHttpRequest();
   xmlHttp.open("GET",url,true);
   xmlHttp.setRequestHeader('Content-Type','application/json');
   xmlHttp.responseType = 'json';
   xmlHttp.onreadystatechange = function() {
      if(xmlHttp.readyState === 4) {
         if(xmlHttp.status === 200) {
            response = xmlHttp.response;
            callback(null,response);
         } else {
            callback(xmlHttp.statusText,null);
         }
      }
   };
   xmlHttp.send(null);
}

function httpPost(url,data,callback) {
   var xmlHttp = new XMLHttpRequest();
   xmlHttp.open("POST",url,true);
   xmlHttp.setRequestHeader('Content-Type','application/json');
   xmlHttp.responseType = 'json';
   xmlHttp.onreadystatechange = function() {
      if(xmlHttp.readyState === 4) {
         if(xmlHttp.status === 200) {
            response = xmlHttp.response;
            callback(null,response);
         } else {
            callback(xmlHttp.statusText,null);
         }
      }
   };
   xmlHttp.send(JSON.stringify(data));
}

function get_select_values(sval){
   let result = null;
   
   if ( sval == null ) {
      return result;
   }
   
   if ( sval.options == null ) {
      return result;
   }
   
   if ( sval.selectedIndex == null ) {
      return result;
   }
   //console.log(sval);
   return sval.options[sval.selectedIndex].value;

}

function get_mselect_values(sval){
   let result = [];
   let options = sval && sval.options;
   let opt;
   
   if ( options == null ) {
      return result;
   }

   for (var i=0, iLen=options.length; i<iLen; i++) {
      opt = options[i];

      if (opt.selected) {
         result.push(opt.value || opt.text);
      }
   }
   return result;
}

function get_url_parameter(param_key) {
   const pu = window.location.search.substring(1);
   
   var url_variables = pu.split('&');
   for (var i = 0; i < url_variables.length; i++) {
      var parameter_name = url_variables[i].split('=');
      if (parameter_name[0] == param_key) {
         return parameter_name[1];
      }
   }
   return null;
}

function build_api() {
   
   if (
      typeof postgrest_host === "undefined" ||
      postgrest_host === null ||
      postgrest_host == "NONE"
   ) {
      zpostgrest_host = get_url_parameter("postgrest_host");
   } else {
      zpostgrest_host = postgrest_host;
   }

   if (
      typeof postgrest_port === "undefined" ||
      postgrest_port === null ||
      postgrest_port == "NONE"
   ) {
      zpostgrest_port = get_url_parameter("postgrest_port");
   } else {
      zpostgrest_port = postgrest_port;
   }

   if (
      typeof postgrest_pref === "undefined" ||
      postgrest_pref === null ||
      postgrest_pref == "NONE"
   ) {
      zpostgrest_pref = get_url_parameter("postgrest_pref");
   } else {
      zpostgrest_pref = postgrest_pref;
   }
   
   if (zpostgrest_pref === null ) {
      zpostgrest_pref = "";
   } else {
      zpostgrest_pref = zpostgrest_pref + "/";
   }

   if (zpostgrest_host === null) {
      console.log("no PostgREST API server location defined.");
      return;
   }

   var http = "http";
   if (zpostgrest_port == "443") {
      http = "https";
   }
   
   return http +
      "://" +
      zpostgrest_host +
      ":" +
      zpostgrest_port + "/" + 
      zpostgrest_pref + "rpc/";
      
}

function roundNumber(number, digits) {
   if (number === null || number === undefined ) {
      return null;
   }
   var multiple = Math.pow(10, digits);
   var rndedNum = Math.round(number * multiple) / multiple;
   return rndedNum;
}

function change_resolution() {
   const nv = get_select_values(document.getElementById("p_nhdplus_version"));
   blank_nhdplusids();

   if (lyr_nhdplus_m_flowlines !== null && nv == "nhdplus_m" ) {
      if (map.hasLayer(lyr_nhdplus_h_flowlines) ) {
         map.removeLayer(lyr_nhdplus_h_flowlines);
      }
      if (! map.hasLayer(lyr_nhdplus_m_flowlines) ) {
         map.addLayer(lyr_nhdplus_m_flowlines);
      }

   } else if (lyr_nhdplus_h_flowlines !== null && nv == "nhdplus_h" ) {
      if (map.hasLayer(lyr_nhdplus_m_flowlines) ) {
         map.removeLayer(lyr_nhdplus_m_flowlines);
      }
      if (! map.hasLayer(lyr_nhdplus_h_flowlines) ) {
         map.addLayer(lyr_nhdplus_h_flowlines);
      }

   }
}
