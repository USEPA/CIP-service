import os,sys,arcpy;
from exporter import auto_fm,tbexporter,fcexporter,get_env_data;
import domains;
from aliases import update_aliases;

###############################################################################
env = get_env_data(os.path.dirname(os.path.realpath(__file__)) + os.sep + '.env');
work_path = os.path.dirname(os.path.realpath(__file__)) + os.sep + "xwalk_huc12";
container_name = "xwalk_huc12.gdb";
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

item = work_path + os.sep + container_name + os.sep + "epageofab_m_catchment_fabric_huc12_np21"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "epageofab_m_catchment_fabric_huc12_nphr"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "epageofab_m_catchment_fabric_huc12_f3"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "epageofab_h_catchment_fabric_huc12_np21"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "epageofab_h_catchment_fabric_huc12_nphr"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "epageofab_h_catchment_fabric_huc12_f3"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

print(" DONE.");
print(" ");

############################################################################### 
# Export feature classes and tables
###############################################################################
force_date = True;

###############################################################################
source    = source_conn + r"/cipsrv_gis.epageofab_m_catchment_fabric_huc12_np21_esri";
outname   = "epageofab_m_catchment_fabric_huc12_np21";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.epageofab_m_catchment_fabric_huc12_nphr_esri";
outname   = "epageofab_m_catchment_fabric_huc12_nphr";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.epageofab_m_catchment_fabric_huc12_f3_esri";
outname   = "epageofab_m_catchment_fabric_huc12_f3";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"xwalk_huc10"                   ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.epageofab_h_catchment_fabric_huc12_np21_esri";
outname   = "epageofab_h_catchment_fabric_huc12_np21";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.epageofab_h_catchment_fabric_huc12_nphr_esri";
outname   = "epageofab_h_catchment_fabric_huc12_nphr";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.epageofab_h_catchment_fabric_huc12_f3_esri";
outname   = "epageofab_h_catchment_fabric_huc12_f3";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");