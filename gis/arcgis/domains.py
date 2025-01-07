import os,sys,arcpy;
from arcpy import env;

##################################################################################################
def domain_exists(target_ws,domain_name):

   desc = arcpy.Describe(target_ws);
   for domain in desc.Domains:
      if domain == domain_name:
         return True;
   return False;

###############################################################################
def Resolution(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "Resolution";
   
   if domain_exists(target_ws,domname) is False:
      try:
         arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 1, "Local");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 2, "High");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 3, "Medium");

      except:
         print(arcpy.GetMessages(2));
         raise;
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;

###############################################################################      
def ElevationRange(target_ws,table_name,field_name,domname=None):

   if domname is None:
      domname = "ElevationRange";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "DOUBLE", "RANGE");
      arcpy.SetValueForRangeDomain_management(target_ws, domname, -400, 9000);

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################
def HWType(target_ws,table_name,field_name,domname=None):

   if domname is None:
      domname = "HWType";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 0, "Real Headwater");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 1, "Artificial Headwater");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;

###############################################################################
def HydroFlowDirections(target_ws,table_name,field_name,domname=None):

   if domname is None:
      domname = "HydroFlowDirections";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 0, "Uninitialized");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 1, "WithDigitized");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;

###############################################################################      
def FlowDirection(target_ws,table_name,field_name,domname=None):

   if domname is None:
      domname = "FlowDirection";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 709, "In");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 712, "NetworkStart");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 713, "NetworkStop");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 714, "Non-Flowing");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;

###############################################################################      
def ParameterName_Domain(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "ParameterName Domain";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "TEXT", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "Version", "Version");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "Resolution", "Resolution");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "Processing_Organization", "Processing_Organization");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "SchemaVersion", "SchemaVersion");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################      
def Process_Domain(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "Process Domain";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "TEXT", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "AVEDIT", "AVEDIT");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "GDBEDT", "GDBEDT");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "100_24", "100_24");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "24_loc", "24_loc");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "AK2FOD", "AK2FOD (historic)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "ARCUPD", "ARCUPD (historic)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "FODGDB", "FODGDB (historic)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "FTIEDT", "FTIEDT (historic)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "FTIFLO", "FTIFLO (historic)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "GDBCVN", "GDBCVN (historic)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "NHDVIS", "NHDVIS (historic)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "PNWNHD", "PNWNHD (historic)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "RF3DBL", "RF3DBL (historic)");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################      
def ChangeCode_Domain(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "ChangeCode Domain";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "TEXT", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "11", "11  Old reach to new reach");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "1P", "1P  Old reach to part of new reach");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "P1", "P1  Part of old reach to new reach");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "A",  "A    Add new reach");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "D",  "D    Delete old reach");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "PP", "PP  Part of old reach to part of new reach");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "1M", "1M (historic) Old reach split to multiple new reaches");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "M1", "M1 (historic) Multiple old reaches merged to single new reach");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################      
def ReachFileVersion_Domain(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "ReachFileVersion Domain";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "TEXT", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "M002.00000", "M002.00000 GEO Medium Resolution");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "H002.00000", "H002.00000 GEO High Resolution");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "L002.00000", "L002.00000 GEO Local Resolution");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "TBD", "TBD");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "M001.00000", "M001.00000 (historic) FOD Medium Resolution");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "H001.00000", "H001.00000 (historic) FOD High Resolution");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "L001.00000", "L001.00000 (historic) FOD Low Resolution");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "M000.00000", "M000.00000 (historic) Pre-FOD");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;    
      
###############################################################################      
def Status(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "Status";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 1,  "Add Feature");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 2,  "Modify Feature Attribute");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 3,  "Modify Feature Geometry");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 5,  "Delete Feature");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 6,  "Delete Relationship");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 7,  "Add Metadata");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 8,  "Modify VAA");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 9,  "Add Event");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10, "Modify Event");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 11, "Delete Event");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 12, "Modify VAA All");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 13, "Add Relationship");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 14, "Add Metadata All (historic)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 15, "Add FeatureToMetadata All (historic)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 16, "Add FeatureToMeta Relationship");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 17, "Create External Crosswalk");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 18, "Modify External Crosswalk");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 19, "Process External Crosswalk All");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;        

###############################################################################      
def Event_Feature_Class_Reference(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "Event Feature Class Reference";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 1,  "NHDArea");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 2,  "NHDFlowline");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 3,  "NHDLine");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 4,  "NHDPoint");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 5,  "NHDWaterbody");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;  
      
###############################################################################      
def Area_EventType(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "Area EventType";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10001,  "303(d) Listed Impaired Waters");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10002,  "305(b) Assessed Waters");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10003,  "Beaches");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10004,  "CWSRF Benefits Reporting");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10005,  "Combined Sewer Overflows");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10006,  "Clean Watersheds Needs Survey");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10007,  "Drinking Water Intakes");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10008,  "Drinking Water Intakes USGS");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10009,  "Fish Consumption Advisories");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10010,  "Fish Tissue Data");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10011,  "Nonpoint Source Projects");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10012,  "Water Monitoring");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10013,  "No-Discharge Zones for Vessel Sewage");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10014,  "National Estuary Program");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10015,  "Facilities that Discharge to Water");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10016,  "Special Appropriations Projects");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10017,  "Source Water Areas");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10018,  "TMDLs on Impaired Waters");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10019,  "Drinking Water Wells");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10020,  "Water Quality Standards");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10021,  "2002 Impaired Waters Baseline");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10022,  "ATTAINS Integrated Reporting");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10023,  "Impaired Waters with TMDLs");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10024,  "305(b) Waters As Assessed");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10025,  "Drinking Water Surface Water");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10026,  "Drinking Water Ground Water");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10027,  "Drinking Water Source Protection Areas");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10028,  "Facility Registry Service Public");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10029,  "Facility Registry Service Restricted");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;  
      
###############################################################################      
def Line_EventType(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "Line EventType";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10001,  "303(d) Listed Impaired Waters");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10002,  "305(b) Assessed Waters");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10003,  "Beaches");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10004,  "CWSRF Benefits Reporting");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10005,  "Combined Sewer Overflows");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10006,  "Clean Watersheds Needs Survey");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10007,  "Drinking Water Intakes");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10008,  "Drinking Water Intakes USGS");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10009,  "Fish Consumption Advisories");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10010,  "Fish Tissue Data");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10011,  "Nonpoint Source Projects");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10012,  "Water Monitoring");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10013,  "No-Discharge Zones for Vessel Sewage");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10014,  "National Estuary Program");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10015,  "Facilities that Discharge to Water");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10016,  "Special Appropriations Projects");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10017,  "Source Water Areas");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10018,  "TMDLs on Impaired Waters");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10019,  "Drinking Water Wells");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10020,  "Water Quality Standards");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10021,  "2002 Impaired Waters Baseline");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10022,  "ATTAINS Integrated Reporting");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10023,  "Impaired Waters with TMDLs");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10024,  "305(b) Waters As Assessed");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10025,  "Drinking Water Surface Water");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10026,  "Drinking Water Ground Water");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10027,  "Drinking Water Source Protection Areas");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10028,  "Facility Registry Service Public");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10029,  "Facility Registry Service Restricted");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;

###############################################################################      
def Point_EventType(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "Point EventType";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10001,  "303(d) Listed Impaired Waters");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10002,  "305(b) Assessed Waters");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10003,  "Beaches");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10004,  "CWSRF Benefits Reporting");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10005,  "Combined Sewer Overflows");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10006,  "Clean Watersheds Needs Survey");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10007,  "Drinking Water Intakes");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10008,  "Drinking Water Intakes USGS");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10009,  "Fish Consumption Advisories");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10010,  "Fish Tissue Data");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10011,  "Nonpoint Source Projects");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10012,  "Water Monitoring");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10013,  "No-Discharge Zones for Vessel Sewage");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10014,  "National Estuary Program");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10015,  "Facilities that Discharge to Water");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10016,  "Special Appropriations Projects");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10017,  "Source Water Areas");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10018,  "TMDLs on Impaired Waters");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10019,  "Drinking Water Wells");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10020,  "Water Quality Standards");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10021,  "2002 Impaired Waters Baseline");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10022,  "ATTAINS Integrated Reporting");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10023,  "Impaired Waters with TMDLs");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10024,  "305(b) Waters As Assessed");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10025,  "Drinking Water Surface Water");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10026,  "Drinking Water Ground Water");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10027,  "Drinking Water Source Protection Areas");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10028,  "Facility Registry Service Public");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 10029,  "Facility Registry Service Restricted");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 57001,  "Streamgage: Streamgage Status = Active; Record = Continuous");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 57002,  "Streamgage: Streamgage Status = Active; Record = Partial");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 57003,  "Streamgage: Streamgage Status = Inactive");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 57004,  "Water Quality Station");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 57100,  "Dam");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 57201,  "Flow Alteration = Addition");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 57202,  "Flow Alteration = Removal");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 57203,  "Flow Alteration = Unknown");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 57300,  "Hydrologic Unit Outlet");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################      
def FType(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "FType";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 307,      "Area to be Submerged");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 312,      "BayInlet");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 318,      "Bridge");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 334,      "Connector");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 336,      "CanalDitch");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 343,      "DamWeir");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 361,      "Playa");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 362,      "Flume");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 364,      "Foreshore");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 367,      "Gaging Station");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 369,      "Gate");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 373,      "Hazard Zone");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 378,      "Ice Mass");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 390,      "LakePond");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 398,      "Lock Chamber");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 403,      "Inundation Area");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 411,      "Nonearthen Shore");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 420,      "Underground Conduit");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 428,      "Pipeline");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 431,      "Rapids");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 434,      "Reef");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 436,      "Reservoir");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 441,      "Rock");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 445,      "SeaOcean");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 450,      "SinkRise");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 454,      "Special Use Zone");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 455,      "Spillway");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 458,      "SpringSeep");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 460,      "StreamRiver");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 461,      "Submerged Stream");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 466,      "SwampMarsh");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 478,      "Tunnel");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 483,      "Wall");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 484,      "Wash");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 485,      "Water IntakeOutflow");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 487,      "Waterfall");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 488,      "Well");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 493,      "Estuary");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 503,      "Sounding Datum Line");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 533,      "Special Use Zone Limit");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 537,      "Area of Complex Channels");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 558,      "Artificial Path");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 566,      "Coastline");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 568,      "Levee");

   except:
      None;  # Domain already exists, I have no idea how to test for this

   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;      
  
  ###############################################################################      
def FCode(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "FCode";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 30700,      "Area to be Submerged");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 31200,      "Bay/Inlet");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 31800,      "Bridge");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 33400,      "Connector");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 33600,      "Canal/Ditch");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 33601,      "Canal/Ditch: Canal/Ditch Type = Aqueduct");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 33603,      "Canal/Ditch: Canal/Ditch Type = Stormwater");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 34300,      "Dam/Weir");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 34305,      "Dam/Weir: Construction Material = Earthen");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 34306,      "Dam/Weir: Construction Material = Nonearthen");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 36100,      "Playa");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 36200,      "Flume");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 36400,      "Foreshore");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 36700,      "Gaging Station");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 36900,      "Gate");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 37300,      "Hazard Zone");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 37800,      "Ice Mass");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 39000,      "Lake/Pond");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 39001,      "Lake/Pond: Hydrographic Category = Intermittent");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 39004,      "Lake/Pond: Hydrographic Category = Perennial");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 39005,      "Lake/Pond: Hydrographic Category = Intermittent; Stage = High Water Elevation");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 39006,      "Lake/Pond: Hydrographic Category = Intermittent; Stage = Date of Photography");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 39009,      "Lake/Pond: Hydrographic Category = Perennial; Stage = Average Water Elevation");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 39010,      "Lake/Pond: Hydrographic Category = Perennial; Stage = Normal Pool");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 39011,      "Lake/Pond: Hydrographic Category = Perennial; Stage = Date of Photography");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 39012,      "Lake/Pond: Hydrographic Category = Perennial; Stage = Spillway Elevation");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 39800,      "Lock Chamber");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 40300,      "Inundation Area");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 40307,      "Inundation Area: Inundation Control Status = Not Controlled");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 40308,      "Inundation Area: Inundation Control Status = Controlled");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 40309,      "Inundation Area: Inundation Control Status = Controlled; Stage = Flood Elevation");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 41100,      "Nonearthen Shore");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42000,      "Underground Conduit");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42001,      "Underground Conduit: Positional Accuracy = Definite");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42002,      "Underground Conduit: Positional Accuracy = Indefinite");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42003,      "Underground Conduit: Positional Accuracy = Approximate");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42800,      "Pipeline");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42801,      "Pipeline: Pipeline Type = Aqueduct; Relationship to Surface = At or Near");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42802,      "Pipeline: Pipeline Type = Aqueduct; Relationship to Surface = Elevated");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42803,      "Pipeline: Pipeline Type = Aqueduct; Relationship to Surface = Underground");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42804,      "Pipeline: Pipeline Type = Aqueduct; Relationship to Surface = Underwater");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42805,      "Pipeline: Pipeline Type = General Case; Relationship to Surface = At or Near");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42806,      "Pipeline: Pipeline Type = General Case; Relationship to Surface = Elevated");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42807,      "Pipeline: Pipeline Type = General Case; Relationship to Surface = Underground");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42808,      "Pipeline: Pipeline Type = General Case; Relationship to Surface = Underwater");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42809,      "Pipeline: Pipeline Type = Penstock; Relationship to Surface = At or Near");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42810,      "Pipeline: Pipeline Type = Penstock; Relationship to Surface = Elevated");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42811,      "Pipeline: Pipeline Type = Penstock; Relationship to Surface = Underground");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42812,      "Pipeline: Pipeline Type = Penstock; Relationship to Surface = Underwater");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42813,      "Pipeline: Pipeline Type = Siphon");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42814,      "Pipeline: Pipeline Type = General Case");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42815,      "Pipeline: Pipeline Type = Penstock");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42816,      "Pipeline: Pipeline Type = Aqueduct");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42820,      "Pipeline: Pipeline Type = Stormwater");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42821,      "Pipeline: Pipeline Type = Stormwater; Relationship to Surface = At or Near");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42822,      "Pipeline: Pipeline Type = Stormwater; Relationship to Surface = Elevated");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42823,      "Pipeline: Pipeline Type = Stormwater; Relationship to Surface = Underground");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 42824,      "Pipeline: Pipeline Type = Stormwater; Relationship to Surface = Underwater");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43100,      "Rapids");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43400,      "Reef");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43600,      "Reservoir");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43601,      "Reservoir: Reservoir Type = Aquaculture");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43603,      "Reservoir: Reservoir Type = Decorative Pool");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43604,      "Reservoir: Reservoir Type = Tailings Pond; Construction Material = Earthen");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43605,      "Reservoir: Reservoir Type = Tailings Pond");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43606,      "Reservoir: Reservoir Type = Disposal");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43607,      "Reservoir: Reservoir Type = Evaporator");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43608,      "Reservoir: Reservoir Type = Swimming Pool");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43609,      "Reservoir: Reservoir Type = Cooling Pond");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43610,      "Reservoir: Reservoir Type = Filtration Pond");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43611,      "Reservoir: Reservoir Type = Settling Pond");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43612,      "Reservoir: Reservoir Type = Sewage Treatment Pond");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43613,      "Reservoir: Reservoir Type = Water Storage; Construction Material = Nonearthen");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43614,      "Reservoir: Reservoir Type = Water Storage; Construction Material = Earthen; Hydrographic Category = Intermittent");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43615,      "Reservoir: Reservoir Type = Water Storage; Construction Material = Earthen; Hydrographic Category = Perennial");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43617,      "Reservoir: Reservoir Type = Water Storage");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43618,      "Reservoir: Construction Material = Earthen");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43619,      "Reservoir: Construction Material = Nonearthen");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43621,      "Reservoir: Reservoir Type = Water Storage; Hydrographic Category = Perennial");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43623,      "Reservoir: Reservoir Type = Evaporator; Construction Material = Earthen");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43624,      "Reservoir; Reservoir Type = Treatment");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43625,      "Reservoir: Reservoir Type = Disposal; Construction Material = Earthen");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 43626,      "Reservoir: Reservoir Type = Disposal; Construction Material = Nonearthen");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 44100,      "Rock");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 44101,      "Rock: Relationship to Surface = Abovewater");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 44102,      "Rock: Relationship to Surface = Underwater");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 44500,      "Sea/Ocean");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 45000,      "Sink/Rise");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 45400,      "Special Use Zone");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 45401,      "Special Use Zone: Special Use Zone Type = Dump Site; Operational Status = Operational");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 45402,      "Special Use Zone: Special Use Zone Type = Dump Site; Operational Status = Abandoned");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 45403,      "Special Use Zone: Special Use Zone Type = Spoil Area; Operational Status = Operational");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 45404,      "Special Use Zone: Special Use Zone Type = Spoil Area; Operational Status = Abandoned");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 45500,      "Spillway");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 45800,      "Spring/Seep");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 46000,      "Stream/River");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 46003,      "Stream/River: Hydrographic Category = Intermittent");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 46006,      "Stream/River: Hydrographic Category = Perennial");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 46007,      "Stream/River: Hydrographic Category = Ephemeral");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 46100,      "Submerged Stream");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 46600,      "Swamp/Marsh");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 46601,      "Swamp/Marsh: Hydrographic Category = Intermittent");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 46602,      "Swamp/Marsh: Hydrographic Category = Perennial");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 47800,      "Tunnel");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 48300,      "Wall");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 48400,      "Wash");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 48500,      "Water Intake/Outflow");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 48700,      "Waterfall");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 48800,      "Well");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 49300,      "Estuary");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 50300,      "Sounding Datum Line");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 50301,      "Sounding Datum Line: Positional Accuracy = Approximate");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 50302,      "Sounding Datum Line: Positional Accuracy = Definite");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 53300,      "Special Use Zone Limit");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 53301,      "Special Use Zone Limit: Positional Accuracy = Definite");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 53302,      "Special Use Zone Limit: Positional Accuracy = Indefinite");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 53700,      "Area of Complex Channels");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 55800,      "Artificial Path");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 56600,      "Coastline");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 56700,      "Shoreline");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, 56800,      "Levee");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;   
      
###############################################################################
def MainPath(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "MainPath";
   
   if domain_exists(target_ws,domname) is False:
      try:
         arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 0, "Unspecified");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 1, "Confluence Main");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 2, "Divergence Main");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 3, "Both Confluence and Divergence");

      except:
         print(arcpy.GetMessages(2));
         raise;
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################
def Divergence(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "Divergence";
   
   if domain_exists(target_ws,domname) is False:
      try:
         arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 0, "Feature is not part of a divergence");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 1, "Feature is the main path of a divergence");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 2, "Feature is a minor path of a divergence");

      except:
         print(arcpy.GetMessages(2));
         raise;
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################
def NoYes(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "YesNo";
   
   if domain_exists(target_ws,domname) is False:
      try:
         arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 0, "No");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 1, "Yes");

      except:
         print(arcpy.GetMessages(2));
         raise;
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################
def VisibilityFilter(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "VisibilityFilter";
   
   if domain_exists(target_ws,domname) is False:
      try:
         arcpy.CreateDomain_management(target_ws, domname, " ", "LONG", "CODED");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 0,       "Unspecified");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 4800,    "Approximately 1:4,800 or Larger Scale");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 12500,   "Approximately 1:12,500 or Larger Scale");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 24000,   "Approximately 1:24,000 or Larger Scale");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 50000,   "Approximately 1:50,000 or Larger Scale");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 100000,  "Approximately 1:100,000 or Larger Scale");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 150000,  "Approximately 1:150,000 or Larger Scale");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 250000,  "Approximately 1:250,000 or Larger Scale");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 500000,  "Approximately 1:500,000 or Larger Scale");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 1000000, "Approximately 1:1,000,000 or Larger Scale");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 2000000, "Approximately 1:2,000,000 or Larger Scale");
         arcpy.AddCodedValueToDomain_management(target_ws, domname, 5000000, "Approximately 1:5,000,000 or Larger Scale");

      except:
         print(arcpy.GetMessages(2));
         raise;
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################      
def PurpCode_Domain(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "PurpCode Domain";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "TEXT", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "SE", "Network End");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "SN", "Nonspatial Connection");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "SP", "Playa");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "SC", "Closed Lake");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "SH", "HUC12 Min-point/Centroid");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "SK", "Karst Sinkhole");      
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "SD", "Topographic Depression");      
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "SO", "Other Sink");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "AC", "Coastline from adjacent VPU");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "AF", "Added Feature from adjacent VPU - nonupstream");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "AU", "Added Feature from adjacent VPU - upstream");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "AI", "Inflow Connecting Feature - upstream adjacent VPU");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "AO", "Outflow flowlines to elevation clip boundary");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "UF", "Unused Feature (FCode or by feature)");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "DO", "Digitized Outflow Path");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "DC", "Digitized Connector Flowline");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "OC", "Waterbody - sea, bat, estuary");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "WB", "Waterbody - other");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################      
def StatusFlag_Domain(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "StatusFlag Domain";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "TEXT", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "A", "Add");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "C", "Change");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "D", "Delete");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "N", "Not Set");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;
      
###############################################################################      
def HuType_Domain(target_ws,table_name,field_name,domname=None):
   
   if domname is None:
      domname = "HuType Domain";
      
   try:
      arcpy.CreateDomain_management(target_ws, domname, " ", "TEXT", "CODED");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "S", "Standard");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "C", "Closed Basin");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "F", "Frontal");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "M", "Multiple Outlet");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "W", "Water");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "I", "Island");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "U", "Urban");
      arcpy.AddCodedValueToDomain_management(target_ws, domname, "D", "Indeterminate");

   except:
      None;  # Domain already exists, I have no idea how to test for this
       
   try:
      arcpy.AssignDomainToField_management(target_ws + "//" + table_name, field_name, domname);
   
   except:
      print(arcpy.GetMessages(2));
      raise;