import os,sys,arcpy;
from exporter import appender,get_env_data;
import domains;
from aliases import update_aliases;
from concurrent import futures;
from concurrent.futures import wait,as_completed,ALL_COMPLETED;

def main():
   ###############################################################################
   env = get_env_data(os.path.dirname(os.path.realpath(__file__)) + os.sep + '.env');
   source_path = os.path.dirname(os.path.realpath(__file__)) + os.sep + "owld_frspub";
   container_name = "owld_frspub.gdb";
   target_conn = env['oracle_conn'];
   print("targeting " + target_conn + "...");
   egdb_conn = arcpy.ArcSDESQLExecute(target_conn);
   
   ###############################################################################
   if not arcpy.Exists(source_path + os.sep + container_name):
      print("source filegeodatabase not found.");
      sys.exit(-99);

   print(" ");
   print("truncating existing data...",end="",flush=True);

   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_attr");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_cip");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_cip_geo");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_control");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_huc12");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_huc12_geo");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_rad_a");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_rad_evt2meta");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_rad_l");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_rad_metadata");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_rad_p");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_rad_srccit");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_sfid");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_src_a");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_src_l");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_src_p");
   egdb_return = egdb_conn.execute("TRUNCATE TABLE owld_public.frspub_src2cip");

   egdb_conn.commitTransaction();
  
   print(" DONE.");
   print(" ");

   ###############################################################################
   src_name  = "frspub_attr";

   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,None
         ,True
      );
      executor.shutdown(wait=True);
      print(future1.result());

   ###############################################################################
   src_name  = "frspub_cip";

   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,None
         ,True
      );
      executor.shutdown(wait=True);
      print(future1.result());

   ###############################################################################
   src_name  = "frspub_cip_geo";

   try:
      egdb_return = egdb_conn.execute("DROP INDEX owld_public." + src_name + "_spx");
   except:
      None;
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid < 100000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future1.result());
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future2 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 100000 and objectid < 200000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future2.result());
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future3 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 200000 and objectid < 300000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future3.result());
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future4 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 300000 and objectid < 400000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future4.result());
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future5 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 400000 and objectid < 500000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future5.result());
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future6 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 500000"
         ,True
      );
      executor.shutdown(wait=True);
      print(future6.result());
   
   wait([future1,future2,future3,future4,future5,future6],return_when=ALL_COMPLETED);
   egdb_return = egdb_conn.execute("CREATE INDEX owld_public." + src_name + "_spx ON owld_public." + src_name + "(shape) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('TABLESPACE=OS_INDEX_ORCWATER SECUREFILE=TRUE COMPRESSION=HIGH')");

   ###############################################################################
   src_name  = "frspub_control";

   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,None
         ,True
      );
      executor.shutdown(wait=True);
      print(future1.result());
      
   wait([future1,],return_when=ALL_COMPLETED);

   ###############################################################################
   src_name  = "frspub_huc12";

   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,None
         ,True
      );
      executor.shutdown(wait=True);
      print(future1.result());
   
   wait([future1,],return_when=ALL_COMPLETED);

   ###############################################################################
   src_name  = "frspub_huc12_geo";

   try:
      egdb_return = egdb_conn.execute("DROP INDEX owld_public." + src_name + "_spx");
   except:
      None;
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid < 30000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future1.result());
      
   wait([future1,],return_when=ALL_COMPLETED);
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future2 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 30000 AND objectid < 60000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future2.result());
      
   wait([future2,],return_when=ALL_COMPLETED);
       
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future3 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 60000 AND objectid < 100000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future3.result());
      
   wait([future3,],return_when=ALL_COMPLETED);
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future4 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 100000 AND objectid < 130000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future4.result());
      
   wait([future4,],return_when=ALL_COMPLETED);
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future5 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 130000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future5.result());
   
   wait([future5,],return_when=ALL_COMPLETED);
   
   egdb_return = egdb_conn.execute("CREATE INDEX owld_public." + src_name + "_spx ON owld_public." + src_name + "(shape) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('TABLESPACE=OS_INDEX_ORCWATER SECUREFILE=TRUE COMPRESSION=HIGH')");

   ###############################################################################
   src_name  = "frspub_rad_evt2meta";

   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,None
         ,False
      );
      executor.shutdown(wait=True);
      print(future1.result());
      
   wait([future1,],return_when=ALL_COMPLETED);

   ###############################################################################
   src_name  = "frspub_rad_metadata";

   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,None
         ,True
      );
      executor.shutdown(wait=True);
      print(future1.result());
      
   wait([future1,],return_when=ALL_COMPLETED);
 
   ###############################################################################
   src_name  = "frspub_rad_p";

   try:
      egdb_return = egdb_conn.execute("DROP INDEX owld_public." + src_name + "_spx");
   except:
      None;
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid < 300000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future1.result());
      
   wait([future1,],return_when=ALL_COMPLETED);
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future2 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 300000"
         ,True
      );
      executor.shutdown(wait=True);
      print(future2.result());

   wait([future2,],return_when=ALL_COMPLETED);
   
   egdb_return = egdb_conn.execute("CREATE INDEX owld_public." + src_name + "_spx ON owld_public." + src_name + "(shape) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('TABLESPACE=OS_INDEX_ORCWATER SECUREFILE=TRUE COMPRESSION=HIGH')");

   ###############################################################################
   src_name  = "frspub_sfid";

   try:
      egdb_return = egdb_conn.execute("DROP INDEX owld_public." + src_name + "_spx");
   except:
      None;
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,None
         ,True
      );
      executor.shutdown(wait=True);
      print(future1.result());
      
   wait([future1,],return_when=ALL_COMPLETED);

   egdb_return = egdb_conn.execute("CREATE INDEX owld_public." + src_name + "_spx ON owld_public." + src_name + "(shape) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('TABLESPACE=OS_INDEX_ORCWATER SECUREFILE=TRUE COMPRESSION=HIGH')");

   ###############################################################################
   src_name  = "frspub_src_p";

   try:
      egdb_return = egdb_conn.execute("DROP INDEX owld_public." + src_name + "_spx");
   except:
      None;
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid < 100000"
         ,False
      );
      executor.shutdown(wait=True);
      print(future1.result());
      
   wait([future1,],return_when=ALL_COMPLETED);
      
   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future2 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,"objectid >= 100000"
         ,True
      );
      executor.shutdown(wait=True);
      print(future2.result());
   
   wait([future2,],return_when=ALL_COMPLETED);
   
   egdb_return = egdb_conn.execute("CREATE INDEX owld_public." + src_name + "_spx ON owld_public." + src_name + "(shape) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('TABLESPACE=OS_INDEX_ORCWATER SECUREFILE=TRUE COMPRESSION=HIGH')");

   ###############################################################################
   src_name  = "frspub_src2cip";

   with futures.ProcessPoolExecutor(max_workers=1) as executor:
      future1 = executor.submit(appender
         ,source_path + os.sep + container_name
         ,src_name
         ,target_conn
         ,"OWLD_PUBLIC." + src_name.upper()
         , arcpy.SpatialReference(4269)
         ,None
         ,True
      );
      executor.shutdown(wait=True);
      print(future1.result());
      
   wait([future1,],return_when=ALL_COMPLETED);

   del egdb_conn;
   
if __name__ == '__main__':
   main();
