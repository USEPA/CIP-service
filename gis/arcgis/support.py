import os,sys,arcpy;
from exporter import auto_fm,tbexporter,fcexporter,get_env_data;
import domains;
from aliases import update_aliases;

###############################################################################
env = get_env_data(os.path.dirname(os.path.realpath(__file__)) + os.sep + '.env');
work_path = os.path.dirname(os.path.realpath(__file__)) + os.sep + "support";
container_name = "support.gdb";
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

item = work_path + os.sep + container_name + os.sep + "tiger_fedstatewaters"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "tiger_aiannha"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

print(" DONE.");
print(" ");

############################################################################### 
# Export feature classes and tables
###############################################################################

###############################################################################
source    = source_conn + r"/cipsrv_gis.tiger_fedstatewaters_esri";
outname   = "tiger_fedstatewaters";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"statens"                       ,"idx01");
arcpy.management.AddIndex(target_nm,"geoid"                         ,"idx02");
arcpy.management.AddIndex(target_nm,"stusps"                        ,"idx03");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.tiger_aiannha_esri";
outname   = "tiger_aiannha";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"aiannhns"                      ,"idx01");
arcpy.management.AddIndex(target_nm,"geoid"                         ,"idx02");
arcpy.management.AddIndex(target_nm,"classfp"                       ,"idx03");
arcpy.management.AddIndex(target_nm,"mtfcc"                         ,"idx04");
arcpy.management.AddIndex(target_nm,"funcstat"                      ,"idx05");
print(" DONE.");
print(outname + " export complete.");
print(" ");
