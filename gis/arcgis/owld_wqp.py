import os,sys,arcpy;
from exporter import auto_fm,tbexporter,fcexporter,get_env_data;
import domains;
from aliases import update_aliases;

###############################################################################
env = get_env_data(os.path.dirname(os.path.realpath(__file__)) + os.sep + '.env');
work_path = os.path.dirname(os.path.realpath(__file__)) + os.sep + "owld_wqp";
container_name = "owld_wqp.gdb";
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

item = work_path + os.sep + container_name + os.sep + "wqp_attr"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_cip"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_cip_geo"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_huc12"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_huc12_geo"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_rad_a"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_rad_evt2meta"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_rad_l"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_rad_metadata"
if arcpy.Exists(item):
   arcpy.Delete_management(item);
   
item = work_path + os.sep + container_name + os.sep + "wqp_rad_p"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_rad_srccit"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_sfid"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_src2cip"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_src_a"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_src_l"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

item = work_path + os.sep + container_name + os.sep + "wqp_src_p"
if arcpy.Exists(item):
   arcpy.Delete_management(item);

print(" DONE.");
print(" ");

############################################################################### 
# Load domains into file geodatabase
###############################################################################

############################################################################### 
# Export feature classes and tables
###############################################################################

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_attr_esri";
outname   = "wqp_attr";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = tbexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx01");
arcpy.management.AddIndex(target_nm,"organizationidentifier"        ,"idx02");
arcpy.management.AddIndex(target_nm,"monitoringlocationidentifier"  ,"idx03");
arcpy.management.AddIndex(target_nm,"countrycode"                   ,"idx04");
arcpy.management.AddIndex(target_nm,"statecode"                     ,"idx05");
arcpy.management.AddIndex(target_nm,"countycode"                    ,"idx06");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_cip_esri";
outname   = "wqp_cip";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = tbexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"cip_joinkey"                   ,"idx01");
arcpy.management.AddIndex(target_nm,"permid_joinkey"                ,"idx02");
arcpy.management.AddIndex(target_nm,"source_originator"             ,"idx03");
arcpy.management.AddIndex(target_nm,"source_featureid"              ,"idx04");
arcpy.management.AddIndex(target_nm,"source_featureid2"             ,"idx05");
arcpy.management.AddIndex(target_nm,"source_series"                 ,"idx06");
arcpy.management.AddIndex(target_nm,"source_subdivision"            ,"idx07");
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx08");
arcpy.management.AddIndex(target_nm,"start_date"                    ,"idx09");
arcpy.management.AddIndex(target_nm,"end_date"                      ,"idx10");
arcpy.management.AddIndex(target_nm,"cat_joinkey"                   ,"idx11");
arcpy.management.AddIndex(target_nm,"catchmentstatecode"            ,"idx12");
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx13");
arcpy.management.AddIndex(target_nm,"istribal"                      ,"idx14");
arcpy.management.AddIndex(target_nm,"catchmentresolution"           ,"idx15");
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx16");
arcpy.management.AddIndex(target_nm,"isnavigable"                   ,"idx17");
arcpy.management.AddIndex(target_nm,"hasvaa"                        ,"idx18");
arcpy.management.AddIndex(target_nm,"issink"                        ,"idx19");
arcpy.management.AddIndex(target_nm,"isheadwater"                   ,"idx20");
arcpy.management.AddIndex(target_nm,"iscoastal"                     ,"idx21");
arcpy.management.AddIndex(target_nm,"isocean"                       ,"idx22");
arcpy.management.AddIndex(target_nm,"isalaskan"                     ,"idx23");
arcpy.management.AddIndex(target_nm,"h3hexagonaddr"                 ,"idx24");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_cip_geo_esri";
outname   = "wqp_cip_geo";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,'POLYGON');

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"cat_joinkey"                   ,"idx01");
arcpy.management.AddIndex(target_nm,"catchmentstatecode"            ,"idx02");
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx03");
arcpy.management.AddIndex(target_nm,"catchmentresolution"           ,"idx04");
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx05");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_huc12_esri";
outname   = "wqp_huc12";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = tbexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"source_originator"             ,"idx01");
arcpy.management.AddIndex(target_nm,"source_featureid"              ,"idx02");
arcpy.management.AddIndex(target_nm,"source_featureid2"             ,"idx03");
arcpy.management.AddIndex(target_nm,"source_series"                 ,"idx04");
arcpy.management.AddIndex(target_nm,"source_subdivision"            ,"idx05");
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx06");
arcpy.management.AddIndex(target_nm,"permid_joinkey"                ,"idx07");
arcpy.management.AddIndex(target_nm,"start_date"                    ,"idx08");
arcpy.management.AddIndex(target_nm,"end_date"                      ,"idx09");
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx10");
arcpy.management.AddIndex(target_nm,"xwalk_catresolution"           ,"idx11");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_huc12_geo_esri";
outname   = "wqp_huc12_geo";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,'POLYGON');

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx01");
arcpy.management.AddIndex(target_nm,"xwalk_catresolution"           ,"idx02");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_rad_a_esri";
outname   = "wqp_rad_a";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,'POLYGON');

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx01");
arcpy.management.AddIndex(target_nm,"reachcode"                     ,"idx02");
arcpy.management.AddIndex(target_nm,"reachresolution"               ,"idx03");
arcpy.management.AddIndex(target_nm,"source_originator"             ,"idx04");
arcpy.management.AddIndex(target_nm,"source_featureid"              ,"idx05");
arcpy.management.AddIndex(target_nm,"source_featureid2"             ,"idx06");
arcpy.management.AddIndex(target_nm,"source_series"                 ,"idx07");
arcpy.management.AddIndex(target_nm,"source_subdivision"            ,"idx08");
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx09");
arcpy.management.AddIndex(target_nm,"permid_joinkey"                ,"idx10");
arcpy.management.AddIndex(target_nm,"start_date"                    ,"idx11");
arcpy.management.AddIndex(target_nm,"end_date"                      ,"idx12");
arcpy.management.AddIndex(target_nm,"geogstate"                     ,"idx12");
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx12");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_rad_evt2meta_esri";
outname   = "wqp_rad_evt2meta";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = tbexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx01");
arcpy.management.AddIndex(target_nm,"meta_processid"                ,"idx02");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_rad_l_esri";
outname   = "wqp_rad_l";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,'POLYLINE');

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx01");
arcpy.management.AddIndex(target_nm,"reachcode"                     ,"idx02");
arcpy.management.AddIndex(target_nm,"reachresolution"               ,"idx03");
arcpy.management.AddIndex(target_nm,"source_originator"             ,"idx04");
arcpy.management.AddIndex(target_nm,"source_featureid"              ,"idx05");
arcpy.management.AddIndex(target_nm,"source_featureid2"             ,"idx06");
arcpy.management.AddIndex(target_nm,"source_series"                 ,"idx07");
arcpy.management.AddIndex(target_nm,"source_subdivision"            ,"idx08");
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx09");
arcpy.management.AddIndex(target_nm,"permid_joinkey"                ,"idx10");
arcpy.management.AddIndex(target_nm,"start_date"                    ,"idx11");
arcpy.management.AddIndex(target_nm,"end_date"                      ,"idx12");
arcpy.management.AddIndex(target_nm,"geogstate"                     ,"idx12");
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx12");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_rad_metadata_esri";
outname   = "wqp_rad_metadata";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = tbexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"meta_processid"                ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_rad_p_esri";
outname   = "wqp_rad_p";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,'POINT');

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"permanent_identifier"          ,"idx01");
arcpy.management.AddIndex(target_nm,"reachcode"                     ,"idx02");
arcpy.management.AddIndex(target_nm,"reachresolution"               ,"idx03");
arcpy.management.AddIndex(target_nm,"source_originator"             ,"idx04");
arcpy.management.AddIndex(target_nm,"source_featureid"              ,"idx05");
arcpy.management.AddIndex(target_nm,"source_featureid2"             ,"idx06");
arcpy.management.AddIndex(target_nm,"source_series"                 ,"idx07");
arcpy.management.AddIndex(target_nm,"source_subdivision"            ,"idx08");
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx09");
arcpy.management.AddIndex(target_nm,"permid_joinkey"                ,"idx10");
arcpy.management.AddIndex(target_nm,"start_date"                    ,"idx11");
arcpy.management.AddIndex(target_nm,"end_date"                      ,"idx12");
arcpy.management.AddIndex(target_nm,"geogstate"                     ,"idx12");
arcpy.management.AddIndex(target_nm,"xwalk_huc12"                   ,"idx12");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_rad_srccit_esri";
outname   = "wqp_rad_srccit";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = tbexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"source_datasetid"              ,"idx01");
arcpy.management.AddIndex(target_nm,"meta_processid"                ,"idx02");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_sfid_esri";
outname   = "wqp_sfid";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = tbexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"source_originator"             ,"idx01");
arcpy.management.AddIndex(target_nm,"source_featureid"              ,"idx02");
arcpy.management.AddIndex(target_nm,"source_featureid2"             ,"idx03");
arcpy.management.AddIndex(target_nm,"source_series"                 ,"idx04");
arcpy.management.AddIndex(target_nm,"source_subdivision"            ,"idx05");
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx06");
arcpy.management.AddIndex(target_nm,"start_date"                    ,"idx07");
arcpy.management.AddIndex(target_nm,"end_date"                      ,"idx08");
arcpy.management.AddIndex(target_nm,"src_event_count"               ,"idx09");
arcpy.management.AddIndex(target_nm,"rad_mr_event_count"            ,"idx10");
arcpy.management.AddIndex(target_nm,"rad_hr_event_count"            ,"idx11");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_src2cip_esri";
outname   = "wqp_src2cip";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = tbexporter(source,outname,work_path,container_name);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"src2cip_joinkey"               ,"idx01");
arcpy.management.AddIndex(target_nm,"cip_joinkey"                   ,"idx02");
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx03");
arcpy.management.AddIndex(target_nm,"permid_joinkey"                ,"idx04");
arcpy.management.AddIndex(target_nm,"cat_joinkey"                   ,"idx05");
arcpy.management.AddIndex(target_nm,"catchmentstatecode"            ,"idx06");
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx07");
arcpy.management.AddIndex(target_nm,"catchmentresolution"           ,"idx08");
arcpy.management.AddIndex(target_nm,"cip_action"                    ,"idx09");
arcpy.management.AddIndex(target_nm,"cip_method"                    ,"idx10");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_src_a_esri";
outname   = "wqp_src_a";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,'POLYGON');

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"permid_joinkey"                ,"idx01");
arcpy.management.AddIndex(target_nm,"source_originator"             ,"idx02");
arcpy.management.AddIndex(target_nm,"source_featureid"              ,"idx03");
arcpy.management.AddIndex(target_nm,"source_featureid2"             ,"idx04");
arcpy.management.AddIndex(target_nm,"source_series"                 ,"idx05");
arcpy.management.AddIndex(target_nm,"source_subdivision"            ,"idx06");
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx07");
arcpy.management.AddIndex(target_nm,"start_date"                    ,"idx08");
arcpy.management.AddIndex(target_nm,"end_date"                      ,"idx09");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_src_l_esri";
outname   = "wqp_src_l";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,'POLYLINE');

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"permid_joinkey"                ,"idx01");
arcpy.management.AddIndex(target_nm,"source_originator"             ,"idx02");
arcpy.management.AddIndex(target_nm,"source_featureid"              ,"idx03");
arcpy.management.AddIndex(target_nm,"source_featureid2"             ,"idx04");
arcpy.management.AddIndex(target_nm,"source_series"                 ,"idx05");
arcpy.management.AddIndex(target_nm,"source_subdivision"            ,"idx06");
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx07");
arcpy.management.AddIndex(target_nm,"start_date"                    ,"idx08");
arcpy.management.AddIndex(target_nm,"end_date"                      ,"idx09");
print(" DONE.");
print(outname + " export complete.");
print(" ");

###############################################################################
source    = source_conn + r"/cipsrv_gis.owld_wqp_src_p_esri";
outname   = "wqp_src_p";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(source,outname,work_path,container_name,'POINT');

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"permid_joinkey"                ,"idx01");
arcpy.management.AddIndex(target_nm,"source_originator"             ,"idx02");
arcpy.management.AddIndex(target_nm,"source_featureid"              ,"idx03");
arcpy.management.AddIndex(target_nm,"source_featureid2"             ,"idx04");
arcpy.management.AddIndex(target_nm,"source_series"                 ,"idx05");
arcpy.management.AddIndex(target_nm,"source_subdivision"            ,"idx06");
arcpy.management.AddIndex(target_nm,"source_joinkey"                ,"idx07");
arcpy.management.AddIndex(target_nm,"start_date"                    ,"idx08");
arcpy.management.AddIndex(target_nm,"end_date"                      ,"idx09");
print(" DONE.");
print(outname + " export complete.");
print(" ");
