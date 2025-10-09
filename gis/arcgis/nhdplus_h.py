import os,sys,arcpy;
from exporter import auto_fm,tbexporter,fcexporter,get_env_data;
import domains;
from aliases import update_aliases;

networkflowlines_only = False;

###############################################################################
env = get_env_data(os.path.dirname(os.path.realpath(__file__)) + os.sep + '.env');
work_path = os.path.dirname(os.path.realpath(__file__)) + os.sep + "nhdplus_h";
container_name = "nhdplus_h.gdb";
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
# Load domains into file geodatabase
###############################################################################

############################################################################### 
# Export feature classes and tables
###############################################################################

###############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_flow_direction_esri";
outname   = "flow_direction";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("adding domains to " + outname + "...",end="",flush=True);
domains.FType(work_path + os.sep + container_name           ,outname,"ftype");
domains.FCode(work_path + os.sep + container_name           ,outname,"fcode");
domains.VisibilityFilter(work_path + os.sep + container_name,outname,"visibilityfilter");
print(" DONE.");

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
source    = source_conn + r"/cipsrv_gis.nhdplus_h_networknhdflowline_esri";
outname   = "networknhdflowline";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("adding domains to " + outname + "...",end="",flush=True);
domains.Resolution(work_path + os.sep + container_name      ,outname,"resolution");
domains.FlowDir(work_path + os.sep + container_name         ,outname,"flowdir");
domains.FType(work_path + os.sep + container_name           ,outname,"ftype");
domains.FCode(work_path + os.sep + container_name           ,outname,"fcode");
domains.MainPath(work_path + os.sep + container_name        ,outname,"mainpath");
domains.NoYes(work_path + os.sep + container_name           ,outname,"innetwork");
domains.VisibilityFilter(work_path + os.sep + container_name,outname,"visibilityfilter");
domains.Divergence(work_path + os.sep + container_name      ,outname,"divergence");
domains.NoYes(work_path + os.sep + container_name           ,outname,"startflag");
domains.NoYes(work_path + os.sep + container_name           ,outname,"terminalfl");
domains.NoYes(work_path + os.sep + container_name           ,outname,"rtndiv");
domains.NoYes(work_path + os.sep + container_name           ,outname,"vpuin");
domains.NoYes(work_path + os.sep + container_name           ,outname,"vpuout");
domains.NoYes(work_path + os.sep + container_name           ,outname,"elevfixed");
domains.HWType(work_path + os.sep + container_name          ,outname,"hwtype");
domains.StatusFlag(work_path + os.sep + container_name      ,outname,"statusflag");
domains.NoYes(work_path + os.sep + container_name           ,outname,"gageadjma");
print(" DONE.");

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

############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdarea_esri";
outname   = "nhdarea";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
   ,empty_fc       = networkflowlines_only
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("adding domains to " + outname + "...",end="",flush=True);
domains.Resolution(work_path + os.sep + container_name      ,outname,"resolution");
domains.ElevationRange(work_path + os.sep + container_name  ,outname,"elevation");
domains.FType(work_path + os.sep + container_name           ,outname,"ftype");
domains.FCode(work_path + os.sep + container_name           ,outname,"fcode");
domains.VisibilityFilter(work_path + os.sep + container_name,outname,"visibilityfilter");
domains.NoYes(work_path + os.sep + container_name           ,outname,"onoffnet");
domains.PurpCode(work_path + os.sep + container_name        ,outname,"purpcode");
domains.NoYes(work_path + os.sep + container_name           ,outname,"burn");
print(" DONE.");

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

############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdline_esri";
outname   = "nhdline";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
   ,empty_fc       = networkflowlines_only
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("adding domains to " + outname + "...",end="",flush=True);
domains.Resolution(work_path + os.sep + container_name      ,outname,"resolution");
domains.FType(work_path + os.sep + container_name           ,outname,"ftype");
domains.FCode(work_path + os.sep + container_name           ,outname,"fcode");
domains.VisibilityFilter(work_path + os.sep + container_name,outname,"visibilityfilter");
print(" DONE.");

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

############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdpoint_esri";
outname   = "nhdpoint";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
   ,empty_fc       = networkflowlines_only
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("adding domains to " + outname + "...",end="",flush=True);
domains.Resolution(work_path + os.sep + container_name      ,outname,"resolution");
domains.FType(work_path + os.sep + container_name           ,outname,"ftype");
domains.FCode(work_path + os.sep + container_name           ,outname,"fcode");
print(" DONE.");

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

############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdwaterbody_esri";
outname   = "nhdwaterbody";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
   ,empty_fc       = networkflowlines_only
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("adding domains to " + outname + "...",end="",flush=True);
domains.Resolution(work_path + os.sep + container_name      ,outname,"resolution");
domains.ElevationRange(work_path + os.sep + container_name  ,outname,"elevation");
domains.FType(work_path + os.sep + container_name           ,outname,"ftype");
domains.FCode(work_path + os.sep + container_name           ,outname,"fcode");
domains.VisibilityFilter(work_path + os.sep + container_name,outname,"visibilityfilter");
domains.NoYes(work_path + os.sep + container_name           ,outname,"onoffnet");
domains.PurpCode(work_path + os.sep + container_name        ,outname,"purpcode");
domains.NoYes(work_path + os.sep + container_name           ,outname,"burn");
print(" DONE.");

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

############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nonnetworknhdflowline_esri";
outname   = "nonnetworknhdflowline";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
   ,empty_fc       = networkflowlines_only
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("adding domains to " + outname + "...",end="",flush=True);
domains.Resolution(work_path + os.sep + container_name      ,outname,"resolution");
domains.FlowDir(work_path + os.sep + container_name         ,outname,"flowdir");
domains.FType(work_path + os.sep + container_name           ,outname,"ftype");
domains.FCode(work_path + os.sep + container_name           ,outname,"fcode");
domains.MainPath(work_path + os.sep + container_name        ,outname,"mainpath");
domains.NoYes(work_path + os.sep + container_name           ,outname,"innetwork");
domains.VisibilityFilter(work_path + os.sep + container_name,outname,"visibilityfilter");
print(" DONE.");

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

############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdplusflow_esri";
outname   = "nhdplusflow";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = tbexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
   ,empty_tb       = networkflowlines_only
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");
print("adding domains to " + outname + "...",end="",flush=True);
domains.NoYes(work_path + os.sep + container_name           ,outname,"hasgeo");
print(" DONE.");

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

############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdpluscatchment_esri";
outname   = "nhdpluscatchment";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
   ,empty_fc       = networkflowlines_only
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"sourcefc"                      ,"idx02");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx03");
print(" DONE.");
print(outname + " export complete.");
print(" ");

############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdplussink_esri";
outname   = "nhdplussink";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
   ,geometry_type  = 'POINT'
   ,empty_fc       = networkflowlines_only
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("adding domains to " + outname + "...",end="",flush=True);
domains.PurpCode(work_path + os.sep + container_name        ,outname,"purpcode");
domains.StatusFlag(work_path + os.sep + container_name      ,outname,"statusflag");
domains.NoYes(work_path + os.sep + container_name           ,outname,"catchment");
domains.NoYes(work_path + os.sep + container_name           ,outname,"burn");
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"nhdplusid"                     ,"idx01");
arcpy.management.AddIndex(target_nm,"featureid"                     ,"idx02");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx03");
print(" DONE.");
print(outname + " export complete.");
print(" ");

############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_nhdplusgage_esri";
outname   = "nhdplusgage";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
   ,geometry_type  = 'POINT'
   ,empty_fc       = networkflowlines_only
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("adding domains to " + outname + "...",end="",flush=True);
domains.NoYes(work_path + os.sep + container_name           ,outname,"onnetwork");
domains.NoYes(work_path + os.sep + container_name           ,outname,"referencegage");
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"hydroaddressid"                ,"idx01");
print(" DONE.");
print(outname + " export complete.");
print(" ");

############################################################################
source    = source_conn + r"/cipsrv_gis.nhdplus_h_wbdhu12_esri";
outname   = "wbdhu12";
target_nm = work_path + os.sep + container_name + os.sep + outname;
ret       = fcexporter(
    source         = source
   ,outname        = outname
   ,work_path      = work_path
   ,container_name = container_name
   ,empty_fc       = networkflowlines_only
);

print("adding aliases to " + outname + "...",end="",flush=True);
update_aliases(work_path + os.sep + container_name,outname);
print(" DONE.");

print("adding domains to " + outname + "...",end="",flush=True);
domains.HuType(work_path + os.sep + container_name          ,outname,"hutype");
print(" DONE.");

print("indexing " + outname + "...",end="",flush=True);
arcpy.management.AddIndex(target_nm,"huc12"                         ,"idx01");
arcpy.management.AddIndex(target_nm,"vpuid"                         ,"idx02");
print(" DONE.");
print(outname + " export complete.");
print(" ");

