import os,sys,arcpy;

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
      
      try:
         fm.addInputField(source,flds[i].name);
      except:
         print(source,flds[i].name);
         raise;
      
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
def fcexporter(source,outname,work_path,container_name,geometry_type=None):

   print("counting " + source + "....",end="",flush=True);
   bef = int(arcpy.GetCount_management(source)[0]);
   print(" " + str(bef) + ".");
   
   arcpy.env.outputCoordinateSystem = spref;
   
   if bef == 0:
      print("creating empty fc " + outname + "...",end="",flush=True);
      
      if arcpy.Exists(arcpy.env.scratchGDB + os.sep + 'temp'):
         arcpy.Delete_management(arcpy.env.scratchGDB + os.sep + 'temp');
      arcpy.management.CreateFeatureclass(
          out_path      = arcpy.env.scratchGDB
         ,out_name      = 'temp'
         ,geometry_type = geometry_type
         ,template      = source
         ,has_m         = 'DISABLED'
         ,has_z         = 'DISABLED'
         ,spatial_reference = spref
      );
      
      fields = arcpy.ListFields(arcpy.env.scratchGDB + os.sep + 'temp');
      for fld in fields:
         if fld.name.lower() in ['objectid','globalid','shape','shape_length','shape_area']:
            try:
               arcpy.management.DeleteField(
                   in_table   = arcpy.env.scratchGDB + os.sep + 'temp'
                  ,drop_field = fld.name
               );
            except:
               print(".  bouncing on existing " + fld.name);
               None;
            
         if fld.aliasName == 'OBJECTID_1':
            arcpy.AlterField_management(
                in_table        = arcpy.env.scratchGDB + os.sep + 'temp'
               ,field           = fld.name
               ,new_field_alias = 'OBJECTID'
            );
            
      arcpy.management.CreateFeatureclass(
          out_path      = work_path + os.sep + container_name
         ,out_name      = outname
         ,geometry_type = geometry_type
         ,template      = arcpy.env.scratchGDB + os.sep + 'temp'
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
def get_env_data(
   path: str
) -> dict:

   rez = {};
   with open(path, 'r') as f:
      for line in f.readlines():
         line = line.replace('\n','');
         if '=' in line and not line.startswith('#'):
            a,b = line.split('=');
            rez[a] = b;
            
   return rez;
   