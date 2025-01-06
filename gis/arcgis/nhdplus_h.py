import arcpy;
from arcpy import env;
import os,sys;

spref = arcpy.SpatialReference(3857);

###############################################################################
def auto_fm(source):

   sdeflds = ['objectid','shape','se_anno_cad_data','shape_length','shape_area','len','area','globalid'];
   source_desc = arcpy.da.Describe(source);

   flds = []
   for item in source_desc["fields"]:
      if item.name.lower() not in sdeflds:
         flds.append(item);

   fms = arcpy.FieldMappings();

   for i in range(len(flds)):
      fm = arcpy.FieldMap();
      fm.addInputField(source,flds[i].name);
      fd             = fm.outputField;
      fd.name        = flds[i].name.lower()
      #print(fd.name + ' ' + fd.type)
      fd.type        = flds[i].type
      fd.length      = flds[i].length;
      fd.precision   = flds[i].precision;
      fd.scale       = flds[i].scale;
      fm.outputField = fd;
      fms.addFieldMap(fm);

   return fms;

###############################################################################
work_path = r"D:\Public\Data\pdziemie\gis";
container_name = "nhdplus_h.gdb";
source_conn = r"D:\Public\Data\WatersGeoPro\watersdev_cipsrv_gis.sde";
print("using " + source_conn + "...");
###############################################################################
if not arcpy.Exists(work_path + os.sep + container_name):
   arcpy.management.CreateFileGDB(
       out_folder_path = work_path
      ,out_name        = container_name
   );

print(" ");
print("purging existing data...",end="",flush=True);

arcpy.env.preserveGlobalIds = True;

item = work_path + os.sep + container_name + os.sep + "flow_direction"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "nhdarea"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "nhdline"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "nhdpoint"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "nhdwaterbody"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "networknhdflowline"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "nonnetworknhdflowline"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "nhdplusflow"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "nhdpluscatchment"
if arcpy.Exists(item):
   arcpy.Delete_management(item);
   
item = work_path + os.sep + container_name + os.sep + "nhdplussink"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "nhdplusgage"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wbdhu12"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

print(" DONE.");
print(" ");

###############################################################################   
def tbexporter(source,outname,work_path,container_name):

   print("counting " + source + "....",end="",flush=True);
   bef = arcpy.GetCount_management(source)[0];
   print(" " + str(bef) + ".");
   print("exporting " + outname + "...",end="",flush=True);
   target_nm = work_path + os.sep + container_name + os.sep + outname;
   arcpy.conversion.ExportTable(
       in_table      = source
      ,out_table     = work_path + os.sep + container_name + os.sep + outname
      ,field_mapping = auto_fm(source)
   );
   print(" DONE.");
   print("counting " + outname + "....",end="",flush=True);
   aft = arcpy.GetCount_management(work_path + os.sep + container_name + os.sep + outname)[0];
   print(" " + str(aft) + ".");
   if bef != aft:
      raise Exception("export counts for " + outname + " do not match!");
   
   arcpy.management.AddGlobalIDs(work_path + os.sep + container_name + os.sep + outname);   
   print(" DONE.");
    
###############################################################################   
def fcexporter(source,outname,work_path,container_name):

   print("counting " + source + "....",end="",flush=True);
   bef = int(arcpy.GetCount_management(source)[0]);
   print(" " + str(bef) + ".");
   
   arcpy.env.outputCoordinateSystem = spref;
   
   if bef == 0:
      print("creating empty fc " + outname + "...",end="",flush=True);
      arcpy.management.CreateFeatureclass(
          out_path      = work_path + os.sep + container_name
         ,out_name      = outname
         ,geometry_type = geometry_type
         ,template      = source
         ,has_m         = 'DISABLED'
         ,has_z         = 'DISABLED'
         ,spatial_reference = spref
      );
   
   else:
      print("exporting " + outname + "...",end="",flush=True);
      arcpy.conversion.ExportFeatures(
           in_features   = source
          ,out_features  = work_path + os.sep + container_name + os.sep + outname
          ,field_mapping = auto_fm(source)
      );
      
   print(" DONE.");
   print("counting " + outname  + "....",end="",flush=True);
   aft = int(arcpy.GetCount_management(work_path + os.sep + container_name + os.sep + outname)[0]);
   print(" " + str(aft) + ".");
   if bef != aft:
      raise Exception("export counts for " + outname + " do not match!");
   
   arcpy.management.AddGlobalIDs(work_path + os.sep + container_name + os.sep + outname);
   print(" DONE.");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_flow_direction_esri";
outname   = "flow_direction";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx02");
arcpy.management.AddIndex(target_nm,"ftype"                         ,"idx03");
arcpy.management.AddIndex(target_nm,"fcode"                         ,"idx04");
arcpy.management.AddIndex(target_nm,"visibilityfilter"              ,"idx05");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdarea_esri";
outname   = "nhdarea";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx02");
arcpy.management.AddIndex(target_nm,"gnis_id"                       ,"idx03");
arcpy.management.AddIndex(target_nm,"ftype"                         ,"idx04");
arcpy.management.AddIndex(target_nm,"fcode"                         ,"idx05");
arcpy.management.AddIndex(target_nm,"visibilityfilter"              ,"idx06");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx07");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdline_esri";
outname   = "nhdline";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx02");
arcpy.management.AddIndex(target_nm,"gnis_id"                       ,"idx03");
arcpy.management.AddIndex(target_nm,"ftype"                         ,"idx04");
arcpy.management.AddIndex(target_nm,"fcode"                         ,"idx05");
arcpy.management.AddIndex(target_nm,"visibilityfilter"              ,"idx06");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx07");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdpoint_esri";
outname   = "nhdpoint";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx02");
arcpy.management.AddIndex(target_nm,"gnis_id"                       ,"idx03");
arcpy.management.AddIndex(target_nm,"ftype"                         ,"idx04");
arcpy.management.AddIndex(target_nm,"fcode"                         ,"idx05");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx06");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdwaterbody_esri";
outname   = "nhdwaterbody";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx02");
arcpy.management.AddIndex(target_nm,"gnis_id"                       ,"idx03");
arcpy.management.AddIndex(target_nm,"ftype"                         ,"idx04");
arcpy.management.AddIndex(target_nm,"fcode"                         ,"idx05");
arcpy.management.AddIndex(target_nm,"visibilityfilter"              ,"idx06");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx07");
arcpy.management.AddIndex(target_nm,"reachcode"                     ,"idx08");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_networknhdflowline_esri";
outname   = "networknhdflowline";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx02");
arcpy.management.AddIndex(target_nm,"gnis_id"                       ,"idx03");
arcpy.management.AddIndex(target_nm,"ftype"                         ,"idx04");
arcpy.management.AddIndex(target_nm,"fcode"                         ,"idx05");
arcpy.management.AddIndex(target_nm,"visibilityfilter"              ,"idx06");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx07");
arcpy.management.AddIndex(target_nm,"reachcode"                     ,"idx08");
arcpy.management.AddIndex(target_nm,"wbarea_permanent_identifier"   ,"idx09");
arcpy.management.AddIndex(target_nm,"levelpathi"                    ,"idx10");
arcpy.management.AddIndex(target_nm,"hydroseq"                      ,"idx11");
arcpy.management.AddIndex(target_nm,"uphydroseq"                    ,"idx12");
arcpy.management.AddIndex(target_nm,"dnhydroseq"                    ,"idx13");
arcpy.management.AddIndex(target_nm,"terminalpa"                    ,"idx14");
arcpy.management.AddIndex(target_nm,"uplevelpat"                    ,"idx15");
arcpy.management.AddIndex(target_nm,"dnlevelpat"                    ,"idx16");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nonnetworknhdflowline_esri";
outname   = "nonnetworknhdflowline";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx02");
arcpy.management.AddIndex(target_nm,"gnis_id"                       ,"idx03");
arcpy.management.AddIndex(target_nm,"ftype"                         ,"idx04");
arcpy.management.AddIndex(target_nm,"fcode"                         ,"idx05");
arcpy.management.AddIndex(target_nm,"visibilityfilter"              ,"idx06");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx07");
arcpy.management.AddIndex(target_nm,"reachcode"                     ,"idx08");
arcpy.management.AddIndex(target_nm,"wbarea_permanent_identifier"   ,"idx09");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdplusflow_esri";
outname   = "nhdplusflow";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = tbexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"fromnhdpid"                    ,"idx01");
arcpy.management.AddIndex(target_nm,"tonhdpid"                      ,"idx02");
arcpy.management.AddIndex(target_nm,"nodenumber"                    ,"idx03");
arcpy.management.AddIndex(target_nm,"direction"                     ,"idx04");
arcpy.management.AddIndex(target_nm,"frompermid"                    ,"idx05");
arcpy.management.AddIndex(target_nm,"topermid"                      ,"idx06");
arcpy.management.AddIndex(target_nm,"fromhydroseq"                  ,"idx07");
arcpy.management.AddIndex(target_nm,"tohydroseq"                    ,"idx08");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdpluscatchment_esri";
outname   = "nhdpluscatchment";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"sourcefc"                      ,"idx02");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx03");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdplussink_esri";
outname   = "nhdplussink";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,'POINT');

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"featureid"                     ,"idx02");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx03");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdplusgage_esri";
outname   = "nhdplusgage";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,'POINT');

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"hydroaddressid"                ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_wbdhu12_esri";
outname   = "wbdhu12";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc12"                         ,"idx01");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx02");
print(" DONE.");
print(outname + " export complete.");
print(" ");

