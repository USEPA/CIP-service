# -*- coding: utf-8 -*-

import arcpy;
import requests;
import os,sys,json;

g_proj      = "CURRENT";
g_mapname   = "CIP-service";
g_dmap_host = "https://cip-api.dmap-stage.aws.epa.gov";
g_dmap_port = 443;

g_domains_cache = None;

class Toolbox(object):
   def __init__(self):
      self.label = "Toolbox"
      self.alias = "toolbox"

      self.tools = [CIPIndex]

class CIPIndex(object):

   def __init__(self):
      self.label = "CIPIndex"
      self.description = ""
      self.canRunInBackground = False

   def getParameterInfo(self):
   
      boo_bad = False;
      clip_list = [];
      filter_list = [];
      status_message = None;
      
      ##---------------------------------------------------------------------##
      if not checkDMAP():
         boo_bad = True;
         status_message = "Unable to contact DMAP API";
      
      else:
         (clip_list,filter_list,error_code,status_message) = domains2menu();
         if error_code != 0:
            boo_bad = True;       
      
      ##---------------------------------------------------------------------##
      param0 = arcpy.Parameter(
          displayName   = "Status Message"
         ,name          = "StatusMessage"
         ,datatype      = "GPString"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = boo_bad
      );
      param0.value = status_message;
      
      ##---------------------------------------------------------------------##
      param1 = arcpy.Parameter(
          displayName   = "Sketch Geometry"
         ,name          = "Sketch Geometry"
         ,datatype      = "GPFeatureRecordSetLayer"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      
      ##---------------------------------------------------------------------##
      param2 = arcpy.Parameter(
          displayName   = "NHDPlus Resolution"
         ,name          = "NHDPlusResolution"
         ,datatype      = "GPString"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      param2.filter.type = "ValueList";
      param2.filter.list = ['Medium','High'];
      param2.value       = 'Medium';
      
      ##---------------------------------------------------------------------##
      param3 = arcpy.Parameter(
          displayName   = "Clip By"
         ,name          = "ClipBy"
         ,datatype      = "GPString"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = not boo_bad
         ,multiValue    = True
      );
      param3.filter.type = "ValueList";
      param3.filter.list = clip_list;
      #param3.value       = None;
      
      ##---------------------------------------------------------------------##
      param4 = arcpy.Parameter(
          displayName   = "When to Clip"
         ,name          = "WhentoClip"
         ,datatype      = "GPString"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      param4.filter.type = "ValueList";
      param4.filter.list = ['Before','After'];
      param4.value       = 'Before';
      
      ##---------------------------------------------------------------------##
      param5 = arcpy.Parameter(
          displayName   = "Filter By"
         ,name          = "FilterBy"
         ,datatype      = "GPString"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = not boo_bad
         ,multiValue    = True
      );
      param5.filter.type = "ValueList";
      param5.filter.list = filter_list;
      #param5.value       = None;
      
      ##---------------------------------------------------------------------##
      param6 = arcpy.Parameter(
          displayName   = "Default Point Indexing Method"
         ,name          = "DefaultPointIndexingMethod"
         ,datatype      = "GPString"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      param6.filter.type = "ValueList";
      param6.filter.list = ['Point - Simple'];
      param6.value       = 'Point - Simple';
      
      ##---------------------------------------------------------------------##
      param7 = arcpy.Parameter(
          displayName   = "Default Line Indexing Method"
         ,name          = "DefaultLineIndexingMethod"
         ,datatype      = "GPString"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      param7.filter.type = "ValueList";
      param7.filter.list = ['Line - Simple','Line - LevelPath'];
      param7.value       = 'Line - LevelPath';
      
      ##---------------------------------------------------------------------##
      param8 = arcpy.Parameter(
          displayName   = "Default Ring Indexing Method"
         ,name          = "DefaultRingIndexingMethod"
         ,datatype      = "GPString"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      param8.filter.type = "ValueList";
      param8.filter.list = ['Area - Simple','Area - Centroid','Area - Artificial Paths','Treat Rings as Lines'];
      param8.value       = 'Area - Simple';
      
      ##---------------------------------------------------------------------##
      param9 = arcpy.Parameter(
          displayName   = "Default Area Indexing Method"
         ,name          = "DefaultAreaIndexingMethod"
         ,datatype      = "GPString"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      param9.filter.type = "ValueList";
      param9.filter.list = ['Area - Simple','Area - Centroid','Area - Artificial Paths'];
      param9.value       = 'Area - Simple';
      
      ##---------------------------------------------------------------------##
      param10 = arcpy.Parameter(
          displayName   = "Linear Threshold"
         ,name          = "LinearThreshold"
         ,datatype      = "GPDouble"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      param10.value = 10;
      
      ##---------------------------------------------------------------------##
      param11 = arcpy.Parameter(
          displayName   = "Area Catchment Threshold"
         ,name          = "AreaCatchmentThreshold"
         ,datatype      = "GPDouble"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      param11.value = 50;
      
      ##---------------------------------------------------------------------##
      param12 = arcpy.Parameter(
          displayName   = "Area Event Threshold"
         ,name          = "AreaEventThreshold"
         ,datatype      = "GPDouble"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      param12.value = 1;
      
      ##---------------------------------------------------------------------##
      param13 = arcpy.Parameter(
          displayName   = "Show Flowlines"
         ,name          = "ShowFlowlines"
         ,datatype      = "GPBoolean"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not boo_bad
      );
      param13.value = True;
      
      params = [
          param0
         ,param1
         ,param2
         ,param3
         ,param4
         ,param5
         ,param6
         ,param7
         ,param8
         ,param9
         ,param10
         ,param11
         ,param12
         ,param13
      ];
      return params

   def isLicensed(self):
      return True;

   def updateParameters(self, parameters):
      return;

   def updateMessages(self, parameters):
      return;

   def execute(self, parameters, messages):
   
      aprx = arcpy.mp.ArcGISProject(g_proj);
      try:
         map  = aprx.listMaps(g_mapname)[0];
      except:
         map  = aprx.listMaps("*")[0];
      
      ##---------------------------------------------------------------------##
      resolution        = resolution2code(parameters[2].valueAsText);
      clipby            = multi2array(parameters[3].valueAsText,clip2code);
      clipstage         = clipstage2code(parameters[4].valueAsText);
      filterby          = multi2array(parameters[5].valueAsText,filter2code);
      point_method      = method2code(parameters[6].valueAsText);
      line_method       = method2code(parameters[7].valueAsText);
      ring_method       = method2code(parameters[8].valueAsText);
      area_method       = method2code(parameters[9].valueAsText);
      linear_threshold  = parameters[10].value;
      areacat_threshold = parameters[11].value;
      areaevt_threshold = parameters[12].value;
      if parameters[13].value or parameters[13].valueAsText == 'True':
         show_flowlines = True;
      else:
         show_flowlines = False;
      
      ##---------------------------------------------------------------------##
      sketch = parameters[1].valueAsText;
      
      if sketch is None or not arcpy.Exists(sketch):
         raise Exception('sketch geometry required for CIP indexing');
      
      scratchfile = arcpy.env.scratchFolder + os.sep + 'scratch.geojson';
      if arcpy.Exists(scratchfile):
         arcpy.Delete_management(scratchfile);

      arcpy.conversion.FeaturesToJSON(
          in_features      = sketch
         ,out_json_file    = scratchfile
         ,format_json      = 'NOT_FORMATTED'
         ,include_z_values = 'NO_Z_VALUES'
         ,include_m_values = 'NO_M_VALUES'
         ,geoJSON          = 'GEOJSON'
         ,outputToWGS84    = 'WGS84'
      );
      
      with open(scratchfile,"r") as file:
         geojson = json.loads(file.read());
         
      payload = {
          'geometry':                      geojson
         ,'geometry_clip':                 clipby
         ,'geometry_clip_stage':           clipstage
         ,'catchment_filter':              filterby
         ,'nhdplus_version':               resolution
         ,'wbd_version':                   None
            
         ,'default_point_indexing_method': point_method
         ,'default_line_indexing_method':  line_method
         ,'default_ring_indexing_method':  ring_method
         ,'default_area_indexing_method':  area_method
         ,'default_line_threshold':        linear_threshold
         ,'default_areacat_threshold':     areacat_threshold
         ,'default_areaevt_threshold':     areaevt_threshold
          
         ,'return_indexed_features':       True
         ,'return_indexed_collection':     False
         ,'return_catchment_geometry':     True
         ,'return_flowlines':              show_flowlines
         ,'return_flowline_geometry':      True
         ,'return_huc12s':                 False
      }
      
      #arcpy.AddMessage(str(json.dumps(payload)))
         
      ##---------------------------------------------------------------------##
      response = requests.post(
          g_dmap_host + ':' + str(g_dmap_port) + '/rpc/cipsrv_index'
         ,json = payload
      );
      
      #arcpy.AddMessage(str(response))
      if response.status_code != 200:
         raise Exception("Indexing Query failed with return code " + str(response.status_code) + ".");
      
      jresponse = response.json();
      
      if not 'return_code' in jresponse:
         raise Exception("Indexing Query failed: " + str(jresponse));
         
      if jresponse['return_code'] != 0:
         raise Exception("Indexing Query failed with return code: " + str(jresponse['return_code']) + ", " + jresponse['status_message']);
         
      ##---------------------------------------------------------------------##
      if show_flowlines and 'flowlines' in jresponse:
         
         flowlinefc = arcpy.CreateScratchName(
             prefix = 'flowlines'
            ,suffix = ''
            ,data_type = 'FeatureClass'
            ,workspace = aprx.defaultGeodatabase
         );

         if arcpy.Exists(scratchfile):
            arcpy.Delete_management(scratchfile);
            
         with open(scratchfile,"w") as outfile:
            outfile.write(json.dumps(jresponse['flowlines']));
         
         arcpy.conversion.JSONToFeatures(
             in_json_file  = scratchfile
            ,out_features  = flowlinefc
            ,geometry_type = 'POLYLINE'
         );
         
         lyrx_flowlines = tempLyrx(
             in_layerfile = aprx.homeFolder + os.sep + 'flowlines.lyrx'
            ,dataset      = os.path.basename(flowlinefc)
            ,name         = os.path.basename(flowlinefc)
            ,aprx         = aprx
         );
         lyr_flowlines = arcpy.mp.LayerFile(lyrx_flowlines);
         map.addLayer(lyr_flowlines,"AUTO_ARRANGE");
      
      ##---------------------------------------------------------------------##
      if 'catchments' in jresponse:
      
         catchmentfc = arcpy.CreateScratchName(
             prefix = 'catchments'
            ,suffix = ''
            ,data_type = 'FeatureClass'
            ,workspace = aprx.defaultGeodatabase
         );
         
         if arcpy.Exists(scratchfile):
            arcpy.Delete_management(scratchfile);
            
         with open(scratchfile,"w") as outfile:
            outfile.write(json.dumps(jresponse['catchments']));
            
         arcpy.conversion.JSONToFeatures(
             in_json_file  = scratchfile
            ,out_features  = catchmentfc
            ,geometry_type = 'POLYGON'
         );
         
         lyrx_catchments = tempLyrx(
             in_layerfile = aprx.homeFolder + os.sep + 'catchments.lyrx'
            ,dataset      = os.path.basename(catchmentfc)
            ,name         = os.path.basename(catchmentfc)
            ,aprx         = aprx
         );
         lyr_catchments = arcpy.mp.LayerFile(lyrx_catchments);
         map.addLayer(lyr_catchments,"AUTO_ARRANGE");
      
      return;

   def postExecute(self, parameters):
      return;

def checkDMAP():
   
   try:
      response = requests.get(g_dmap_host + ':' + str(g_dmap_port) + '/rpc/healthcheck');
   except:
      return False;
   
   if response.status_code == 200:
      return True;
      
   else:
      return False;
      
def getDomains():
   global g_domains_cache;
   
   if g_domains_cache is None:
      response = requests.get(g_dmap_host + ':' + str(g_dmap_port) + '/rpc/cipsrv_domains');
      
      if response.status_code == 200:
         g_domains_cache = response.json();
         
      else:
         return {'return_code': -1,'status_message': 'unable to query domains service.'}
          
   return g_domains_cache;
   
def domains2menu():
   
   clip_list = [];
   filter_list = [];
   json = getDomains();
   
   if 'status' not in json:
   
      for item in json["states"]:
         clip_list.append(item["name"]);
         
      for item in json["tribes"]:
      
         if item["has_trust_lands"] and item["has_reservation_lands"]:
            clip_list.append(item["aiannha_namelsad"]);
            clip_list.append(item["aiannha_namelsad"] + " (Reservation Lands Only)");
            clip_list.append(item["aiannha_namelsad"] + " (Trust Lands Only)");
         elif not item["has_trust_lands"] and item["has_reservation_lands"]:
            clip_list.append(item["aiannha_namelsad"] + " (Reservation Lands Only)");
         elif item["has_trust_lands"] and not item["has_reservation_lands"]:  
            clip_list.append(item["aiannha_namelsad"] + " (Trust Lands Only)");
            
      filter_list.append("Tribes");
      for item in json["states"]:
         filter_list.append(item["name"]);
      
   else:
      return ([],[],json["return_code"],json["status_message"]);
      
   return (clip_list,filter_list,0,None);
      
def clip2code(p_value):
   json = getDomains();
   
   for item in json["states"]:
      if p_value == item["name"]:
         return 'USPS:' + item["stusps"];
         
   for item in json["tribes"]:
      if p_value == item["aiannha_namelsad"]:
         return 'GEOID_STEM:' + item["aiannha_geoid_stem"];
      elif p_value == item["aiannha_namelsad"] + ' (Trust Lands Only)':
         return 'GEOID_STEM:' + item["aiannha_geoid_stem"] + ':T';
      elif p_value == item["aiannha_namelsad"] + ' (Reservation Lands Only)':
         return 'GEOID_STEM:' + item["aiannha_geoid_stem"] + ':R';
         
   return None;

def clipstage2code(p_value):

   if p_value == 'Before':
      return 'BEFORE';
      
   elif p_value == 'After':
      return 'AFTER';
      
   return p_value;
   
def filter2code(p_value):

   if p_value == 'Tribes':
      return 'TRIBES';
      
   json = getDomains();
   
   for item in json["states"]:
      if p_value == item["name"]:
         return item["stusps"];
         
def resolution2code(p_value):
 
   if p_value in ['Medium','Medium Resolution']:
      return 'nhdplus_m';
      
   elif p_value in ['High','High Resolution']:
      return 'nhdplus_h';
      
   return p_value;
      
def method2code(p_value):

   if p_value == 'Point - Simple':
      return 'point_simple';
      
   elif p_value == 'Line - Simple':
      return 'line_simple';
      
   elif p_value == 'Line - LevelPath':
      return 'line_levelpath';
      
   elif p_value == 'Area - Simple':
      return 'area_simple';
      
   elif p_value == 'Area - Centroid':
      return 'area_centroid';
      
   elif p_value == 'Area - Artificial Paths':
      return 'area_artpath';
      
   elif p_value == 'Treat Rings as Lines':
      return 'treat_as_lines';
      
   return p_value;

def multi2array(p_value,fnc=None):

   aryout = [];
   if p_value is None:
      return aryout;   
   
   for item in p_value.split(';'):
      if item.startswith("'") and item.endswith("'"):
         if fnc is None:
            aryout.append(item[1:-1]);
         else:
            aryout.append(fnc(item[1:-1]));
      else:
         if fnc is None:
            aryout.append(item);
         else:
            aryout.append(fnc(item));
         
   return aryout;

###############################################################################
def tempLyrx(
    in_layerfile
   ,dataset   = None
   ,sourceURI = None
   ,name      = None
   ,aprx      = None
):

   if aprx is None:
      aprx = arcpy.mp.ArcGISProject(g_prj);

   with open(in_layerfile,"r") as jsonFile_target:
      data_in = json.load(jsonFile_target);

   for item in data_in["layerDefinitions"]:
      
      if dataset is not None:   
         item["featureTable"]["dataConnection"]["workspaceConnectionString"] = "DATABASE=" + aprx.defaultGeodatabase;
         item["featureTable"]["dataConnection"]["dataset"] = dataset;

      elif sourceURI is not None:
         item["sourceURI"] = sourceURI;
      
      if name is not None:
         item["name"] = name;
   
   lyrx_target = arcpy.CreateScratchName(
       prefix    = "tmp"
      ,suffix    = ".lyrx"
      ,data_type = "Folder"
      ,workspace = arcpy.env.scratchFolder
   );
   
   with open(lyrx_target,"w") as jsonFile:
      json.dump(data_in,jsonFile);

   return lyrx_target;

