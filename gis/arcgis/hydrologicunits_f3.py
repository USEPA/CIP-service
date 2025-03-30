import os,sys,arcpy;
from exporter import auto_fm,tbexporter,fcexporter,get_env_data;
import domains;
from aliases import update_aliases;

###############################################################################
env = get_env_data(os.path.dirname(os.path.realpath(__file__)) + os.sep + '.env');
work_path = os.path.dirname(os.path.realpath(__file__)) + os.sep + "hydrologicunits_f3";
container_name = "hydrologicunits_f3.gdb";
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

item = work_path + os.sep + container_name + os.sep + "hu12"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "hu12sp"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "hu10"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "hu10sp"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "hu8"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "hu8sp"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "hu6"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "hu6sp"
if arcpy.Exists(item):
   arcpy.Delete_management(item);
   
item = work_path + os.sep + container_name + os.sep + "hu4"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "hu4sp"
if arcpy.Exists(item):
   arcpy.Delete_management(item);
   
item = work_path + os.sep + container_name + os.sep + "hu2"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "hu2sp"
if arcpy.Exists(item):
   arcpy.Delete_management(item);
   
print(" DONE.");
print(" ");

############################################################################### 
# Export feature classes and tables
###############################################################################
force_date = True;

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu12_f3_esri";
outname   = "hu12";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc12"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu12sp_f3_esri";
outname   = "hu12sp";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc12"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu10_f3_esri";
outname   = "hu10";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc10"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu10sp_f3_esri";
outname   = "hu10sp";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc10"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu8_f3_esri";
outname   = "hu8";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc8"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu8sp_f3_esri";
outname   = "hu8sp";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc8"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu6_f3_esri";
outname   = "hu6";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc6"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu6sp_f3_esri";
outname   = "hu6sp";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc6"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu4_f3_esri";
outname   = "hu4";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc4"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu4sp_f3_esri";
outname   = "hu4sp";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc4"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu2_f3_esri";
outname   = "hu2";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc2"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.wbd_hu2sp_f3_esri";
outname   = "hu2sp";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,force_date=force_date);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc2"                         ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");
