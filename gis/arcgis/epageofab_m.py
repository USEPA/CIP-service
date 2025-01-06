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
container_name = "epageofab_m.gdb";
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

item = work_path + os.sep + container_name + os.sep + "catchment_fabric"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "catchment_fabric_huc12"
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
source    = source_conn + r"/cipsrv_gis.epageofab_m_catchment_fabric_esri";
outname   = "catchment_fabric";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"catchmentstatecode"            ,"idx01");
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx02");
arcpy.management.AddIndex(target_nm,"istribal"                      ,"idx03");
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx04");
arcpy.management.AddIndex(target_nm,"sourcefc"                      ,"idx05");
arcpy.management.AddIndex(target_nm,"isnavigable"                   ,"idx06");
arcpy.management.AddIndex(target_nm,"hasvaa"                        ,"idx07");
arcpy.management.AddIndex(target_nm,"issink"                        ,"idx08");
arcpy.management.AddIndex(target_nm,"isheadwater"                   ,"idx09");
arcpy.management.AddIndex(target_nm,"iscoastal"                     ,"idx10");
arcpy.management.AddIndex(target_nm,"isocean"                       ,"idx11");
arcpy.management.AddIndex(target_nm,"isalaskan"                     ,"idx12");
arcpy.management.AddIndex(target_nm,"h3hexagonaddr"                 ,"idx13");
arcpy.management.AddIndex(target_nm,"state_count"                   ,"idx14");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx15");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.epageofab_m_catchment_fabric_huc12_esri";
outname   = "catchment_fabric_huc12";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

