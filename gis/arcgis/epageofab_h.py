import os,sys,arcpy;
from exporter import auto_fm,tbexporter,fcexporter,get_env_data;
import domains;
from aliases import update_aliases;

###############################################################################
env = get_env_data(os.path.dirname(os.path.realpath(__file__)) + os.sep + '.env');
work_path = os.path.dirname(os.path.realpath(__file__)) + os.sep + "epageofab_h";
container_name = "epageofab_h.gdb";
source_conn = env['sde_conn'];
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
# Export feature classes and tables
###############################################################################

###############################################################################
source    = source_conn + r"/cipsrv_gis.epageofab_h_catchment_fabric_esri";
outname   = "catchment_fabric";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

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
source    = source_conn + r"/cipsrv_gis.epageofab_h_catchment_fabric_huc12_esri";
outname   = "catchment_fabric_huc12";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

