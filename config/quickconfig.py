# This setup script uses Python 3 to set up a local CIP-Service with the medium resolution NHDPlus dataset using the default profiles.
# Note it will use default cip123 passwords so be sure not to expose the containers to the outside world.

# Your host Python will require the jinja2 package which can be installed via
# >pip install Jinja2
#
import os,sys,importlib,argparse,shutil,yaml,getpass;
from subprocess import Popen, PIPE;
cc = importlib.import_module("config-compose");

script_root = os.path.realpath(os.path.dirname(__file__));
parser = argparse.ArgumentParser(description='quickconfig script for cipsrv');
parser.add_argument("--recipe"                  ,required=False,default=None);

parser.add_argument("--mr_dumpfile"             ,required=False,default=None);
parser.add_argument("--mr_dumpfile_copyin"      ,required=False,default=None);
parser.add_argument("--hr_dumpfile"             ,required=False,default=None);
parser.add_argument("--hr_dumpfile_copyin"      ,required=False,default=None);

parser.add_argument("--mrgf_dumpfile"           ,required=False,default=None);
parser.add_argument("--mrgf_dumpfile_copyin"    ,required=False,default=None);
parser.add_argument("--hrgf_dumpfile"           ,required=False,default=None);
parser.add_argument("--hrgf_dumpfile_copyin"    ,required=False,default=None);

parser.add_argument("--mr2_dumpfile"            ,required=False,default=None);
parser.add_argument("--mr2_dumpfile_copyin"     ,required=False,default=None);
parser.add_argument("--hr2_dumpfile"            ,required=False,default=None);
parser.add_argument("--hr2_dumpfile_copyin"     ,required=False,default=None);

parser.add_argument("--mrgrid_dumpfile"         ,required=False,default=None);
parser.add_argument("--mrgrid_dumpfile_copyin"  ,required=False,default=None);
parser.add_argument("--hrgrid_dumpfile"         ,required=False,default=None);
parser.add_argument("--hrgrid_dumpfile_copyin"  ,required=False,default=None);

parser.add_argument("--mrtp_dumpfile"           ,required=False,default=None);
parser.add_argument("--mrtp_dumpfile_copyin"    ,required=False,default=None);
parser.add_argument("--hrtp_dumpfile"           ,required=False,default=None);
parser.add_argument("--hrtp_dumpfile_copyin"    ,required=False,default=None);

parser.add_argument("--mrws_dumpfile"           ,required=False,default=None);
parser.add_argument("--mrws_dumpfile_copyin"    ,required=False,default=None);
parser.add_argument("--hrws_dumpfile"           ,required=False,default=None);
parser.add_argument("--hrws_dumpfile_copyin"    ,required=False,default=None);

parser.add_argument("--owld_dumpfiles"          ,required=False,default=None);
parser.add_argument("--owld_dumpfiles_copyin"   ,required=False,default=None);

parser.add_argument("--support_dumpfile"        ,required=False,default=None);
parser.add_argument("--support_dumpfile_copyin" ,required=False,default=None);

parser.add_argument("--override_postgresql_port",required=False,default=None);

parser.add_argument("--override_demo_pgrst_port",required=False,default=None);
parser.add_argument("--override_demo_pgrst_host",required=False,default=None);
parser.add_argument("--override_demo_gis_prot"  ,required=False,default=None);
parser.add_argument("--override_demo_gis_port"  ,required=False,default=None);
parser.add_argument("--override_demo_gis_host"  ,required=False,default=None);

parser.add_argument("--override_engine_profile" ,required=False,default=None);
parser.add_argument("--override_git_branch"     ,required=False,default=None);
parser.add_argument("--override_username"       ,required=False,default=None);
parser.add_argument("--force_nogit_uselocal"    ,required=False,default=False,action=argparse.BooleanOptionalAction);
parser.add_argument("--down_volumes"            ,required=False,default=False,action=argparse.BooleanOptionalAction);
parser.add_argument("--build_nocache"           ,required=False,default=False,action=argparse.BooleanOptionalAction);

def main(
    recipe
    
   ,mr_dumpfile
   ,mr_dumpfile_copyin
   ,hr_dumpfile
   ,hr_dumpfile_copyin
   
   ,mrgf_dumpfile
   ,mrgf_dumpfile_copyin
   ,hrgf_dumpfile
   ,hrgf_dumpfile_copyin
   
   ,mr2_dumpfile
   ,mr2_dumpfile_copyin
   ,hr2_dumpfile
   ,hr2_dumpfile_copyin
   
   ,mrgrid_dumpfile
   ,mrgrid_dumpfile_copyin
   ,hrgrid_dumpfile
   ,hrgrid_dumpfile_copyin
   
   ,mrtp_dumpfile
   ,mrtp_dumpfile_copyin
   ,hrtp_dumpfile
   ,hrtp_dumpfile_copyin
   
   ,mrws_dumpfile
   ,mrws_dumpfile_copyin
   ,hrws_dumpfile
   ,hrws_dumpfile_copyin
   
   ,owld_dumpfiles
   ,owld_dumpfiles_copyin
   
   ,support_dumpfile
   ,support_dumpfile_copyin
   
   ,override_postgresql_port
   
   ,override_demo_pgrst_port
   ,override_demo_pgrst_host
   ,override_demo_gis_prot
   ,override_demo_gis_port
   ,override_demo_gis_host
   
   ,override_engine_profile
   ,override_git_branch
   ,override_username
   ,force_nogit_uselocal
   ,down_volumes
   ,build_nocache
):

   def dzproc(cmd,nofail=False):
      proc = Popen(
          cmd
         ,stdin              = PIPE
         ,stdout             = PIPE
         ,stderr             = PIPE
         ,universal_newlines = True
      );

      while True:
         line = proc.stdout.readline();
         if not line:
            break;
         if line.strip() != "":
            print(".  " + ' '.join(line.split()));
      rc = proc.wait();
      if rc != 0 and not nofail:
         while True:
            line = proc.stderr.readline();
            if not line:
               break;
            if line.strip() != "":
               print(".  " + ' '.join(line.split()));
         sys.exit(rc);
         
   ################################################################################## 
   def cipld(
       ipnyb
      ,dumpfile
      ,dumpfile_copyin
      ,dumpfile_parm
      ,multi_flag           = False
      ,override_username    = None
   ):
      cmd = [
          "docker","compose","exec","cip_jp","jupyter"
         ,"nbconvert","/home/jovyan/notebooks/setup/pg_restore_" + ipnyb + ".ipynb"
         ,"--to","python","--output","/tmp/pg_restore_" + ipnyb + ".py"
      ];
      dzproc(cmd);

      if dumpfile_copyin is not None:
         
         if multi_flag:
            
            mdf = "";
            for item in dumpfile_copyin.split(','):
               (pref,file) = item.split(':');
               
               mdf2 = os.path.basename(file);
               print("Loading local " + item + " dumpfile " + file + " into container.");
               cmd = ["docker","compose","cp",file,"cip_jp:/home/jovyan/loading_dock/" + mdf2];
               dzproc(cmd);
            
               mdf += pref + ":" + mdf2 + ",";

            mdf = mdf.rstrip(',');               
            
         else:
            mdf = os.path.basename(dumpfile_copyin);
            print("Loading local " + ipnyb + " dumpfile " + mdf + " into container.");
            cmd = ["docker","compose","cp",dumpfile_copyin,"cip_jp:/home/jovyan/loading_dock/" + mdf];
            dzproc(cmd);
            
         cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_" + ipnyb + ".py","--use_existing",dumpfile_parm,mdf];
         
         if override_username is not None:
            cmd.append("--override_username");
            cmd.append(override_username);
            
         dzproc(cmd);
         
      else:
         if dumpfile is None:
            print("Downloading and importing default " + ipnyb + " data.");
            cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_" + ipnyb + ".py"];
            
         else:
            print("Downloading and importing " + dumpfile + " of " + ipnyb + " data.");
            cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_" + ipnyb + ".py",dumpfile_parm,dumpfile];
         
         if override_username is not None:
            cmd.append("--override_username");
            cmd.append(override_username);

         dzproc(cmd);

   ################################################################################## 
   def cipgt(
       ipnyb
      ,override_git_branch  = None
      ,force_nogit_uselocal = False
      ,override_username    = None
   ):
      print("Fetching and loading " + ipnyb + " logic.");
      cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/git_checkout_" + ipnyb + ".ipynb","--to","python","--output","/tmp/git_checkout_" + ipnyb + ".py"];
      dzproc(cmd);
      
      if force_nogit_uselocal:
         print("Loading " + ipnyb + "_deploy.sql into container.");
         cmd = ["docker","compose","cp","../src/database/" + ipnyb + "/" + ipnyb + "_deploy.sql","cip_jp:/tmp/" + ipnyb + "_deploy.sql"];
         dzproc(cmd);
         
         cmd = ["docker","compose","exec","cip_jp","python3","/tmp/git_checkout_" + ipnyb + ".py","--use_existing_sql","/tmp/" + ipnyb + "_deploy.sql"];
         
         if override_username is not None:
            cmd.append("--override_username");
            cmd.append(override_username);
            
         dzproc(cmd);
         
      else:
         cmd = ["docker","compose","exec","cip_jp","python3","/tmp/git_checkout_" + ipnyb + ".py"];
         
         if override_git_branch is not None:
            cmd.append("--override_branch");
            cmd.append(override_git_branch);
         
         if override_username is not None:
            cmd.append("--override_username");
            cmd.append(override_username);
      
         dzproc(cmd);
   
   ###############################################################################
   if mr_dumpfile_copyin is not None:
      if not os.path.exists(mr_dumpfile_copyin):
         raise Exception("mr_dumpfile_copyin not found - " + str(mr_dumpfile_copyin));
   
   if hr_dumpfile_copyin is not None:
      if not os.path.exists(hr_dumpfile_copyin):
         raise Exception("hr_dumpfile_copyin not found - " + str(hr_dumpfile_copyin));
   
   if mrgf_dumpfile_copyin is not None:
      if not os.path.exists(mrgf_dumpfile_copyin):
         raise Exception("mrgf_dumpfile_copyin not found - " + str(mrgf_dumpfile_copyin));
   
   if hrgf_dumpfile_copyin is not None:
      if not os.path.exists(hrgf_dumpfile_copyin):
         raise Exception("hrgf_dumpfile_copyin not found - " + str(hrgf_dumpfile_copyin));
   
   if mr2_dumpfile_copyin is not None:
      if not os.path.exists(mr2_dumpfile_copyin):
         raise Exception("mr2_dumpfile_copyin not found - " + str(mr2_dumpfile_copyin));
   
   if hr2_dumpfile_copyin is not None:
      if not os.path.exists(hr2_dumpfile_copyin):
         raise Exception("hr2_dumpfile_copyin not found - " + str(hr2_dumpfile_copyin));
   
   if mrgrid_dumpfile_copyin is not None:
      if not os.path.exists(mrgrid_dumpfile_copyin):
         raise Exception("mrgrid_dumpfile_copyin not found - " + str(mrgrid_dumpfile_copyin));
   
   if hrgrid_dumpfile_copyin is not None:
      if not os.path.exists(hrgrid_dumpfile_copyin):
         raise Exception("hrgrid_dumpfile_copyin not found - " + str(hrgrid_dumpfile_copyin));
   
   if mrtp_dumpfile_copyin is not None:
      if not os.path.exists(mrtp_dumpfile_copyin):
         raise Exception("mrtp_dumpfile_copyin not found - " + str(mrtp_dumpfile_copyin));
   
   if hrtp_dumpfile_copyin is not None:
      if not os.path.exists(hrtp_dumpfile_copyin):
         raise Exception("hrtp_dumpfile_copyin not found - " + str(hrtp_dumpfile_copyin));
   
   if mrws_dumpfile_copyin is not None:
      if not os.path.exists(mrws_dumpfile_copyin):
         raise Exception("mrws_dumpfile_copyin not found - " + str(mrws_dumpfile_copyin));
   
   if hrws_dumpfile_copyin is not None:
      if not os.path.exists(hrws_dumpfile_copyin):
         raise Exception("hrws_dumpfile_copyin not found - " + str(hrws_dumpfile_copyin));
         
   if owld_dumpfiles_copyin is not None:
      for item in owld_dumpfiles_copyin.split(','):
         (pref,file) = item.split(':');
         if not os.path.exists(file):
            raise Exception("owld_dumpfiles_copyin for " + pref + " not found - " + str(file));
            
   if override_username is None:
      override_username = getpass.getuser();
   
   ###############################################################################
   # set context to project directory
   ###############################################################################
   os.chdir(os.path.dirname(os.path.realpath(__file__)));   
   
   ###############################################################################
   print("Downing any existing compose services");

   os.chdir('../engine');
   if os.path.exists("docker-compose.yml") \
   or os.path.exists("docker-compose.j2"):
      print(".  engine");
      cmd = ["docker","compose","down"];
      if down_volumes:
         print(".   forcing engine volumes down");
         cmd.append("-v");
         
      dzproc(cmd);
      
      if down_volumes:
         dzproc(["docker","volume","rm","cip-service-engine_pgdata"],nofail=True);
         dzproc(["docker","volume","rm","cip-service-engine_pgdata"],nofail=True);
      
   os.chdir('../admin');
   if os.path.exists("docker-compose.yml") \
   or os.path.exists("docker-compose.j2"):
      print(".  admin");
      cmd = ["docker","compose","down"];
      if down_volumes:
         print(".   forcing admin volumes down");
         cmd.append("-v");
         
      dzproc(cmd);
      
      if down_volumes:
         dzproc(["docker","volume","rm","cip-service-admin_home-jovyan"],nofail=True);
         dzproc(["docker","volume","rm","cip-service-admin_jupyter"],nofail=True);
      
   os.chdir('../demo');
   if os.path.exists("docker-compose.yml") \
   or os.path.exists("docker-compose.j2"):
      print(".  demo");
      cmd = ["docker","compose","down"];
      if down_volumes:
         print(".   forcing demo volumes down");
         cmd.append("-v");      
      
      dzproc(cmd);
      
   os.chdir('../gis');
   if os.path.exists("docker-compose.yml") \
   or os.path.exists("docker-compose.j2"):
      print(".  gis");
      cmd = ["docker","compose","down"];
      if down_volumes:
         print(".   forcing gis volumes down");
         cmd.append("-v"); 
      
      dzproc(cmd);
      
      if down_volumes:
         dzproc(["docker","volume","rm","cip-service-gis_geoserver-data"],nofail=True);    
      
   ###############################################################################
   # set context to config directory
   ###############################################################################
   os.chdir('../config');
   
   ###############################################################################
   if recipe is None:
      recipe = 'MRONLY'; 

   elif recipe == 'VPU09':
      print("using recipe VPU09");
      mr_dumpfile      = 'cipsrv_nhdplus_m_v21_vpu09_1.dmp';
      mrgf_dumpfile    = 'cipsrv_epageofab_m_v21_vpu09_1.dmp';
      hr_dumpfile      = 'cipsrv_nhdplus_h_beta_vpu09_1.dmp';
      hrgf_dumpfile    = 'cipsrv_epageofab_h_beta_hr1_vpu09_1.dmp';
      support_dumpfile = 'cipsrv_support_1_vpu09.dmp';

   ###############################################################################
   if override_engine_profile is not None:
      print("Configuring compose for engine containers using " + override_engine_profile + " profile.");
      cc.main(
          bundle    = "engine"
         ,bprofile  = override_engine_profile
      );
      
   else:
      print("Configuring compose for engine containers using default settings.");
      cc.main(
          bundle    = "engine"
         ,bprofile  = "default"
      );

   ###############################################################################
   print("Configuring compose for admin containers using default settings.");
   cc.main(
       bundle    = "admin"
      ,bprofile  = "default"
   );

   ###############################################################################
   print("Configuring compose for demo containers using default settings.");
   cc.main(
       bundle    = "demo"
      ,bprofile  = "default"
   );

   ###############################################################################
   print("Configuring compose for gis containers using default settings.");
   cc.main(
       bundle    = "gis"
      ,bprofile  = "default"
   );

   ###############################################################################
   # set context to engine container
   ###############################################################################
   os.chdir('../engine');
   ###############################################################################
   
   if override_postgresql_port is not None:
      shutil.move('docker-compose.yml','docker-compose.yml.bak');
      
      with open('docker-compose.yml.bak',"r") as s:
         dcomp = yaml.safe_load(s);
         
      dcomp['services']['cip_pg']['ports'][0]['published'] = int(override_postgresql_port);
         
      with open('docker-compose.yml',"w") as d:
         yaml.dump(dcomp,d);

   print("Running compose build for engine containers.");
   cmd = ["docker","compose","build"];
   if build_nocache:
      cmd.append("--no-cache");
   dzproc(cmd);

   ###############################################################################
   print("Running compose up for engine containers.");
   cmd = ["docker","compose","up","-d"];
   dzproc(cmd);

   ###############################################################################
   # set context to demo container
   ###############################################################################
   os.chdir('../demo');
   ###############################################################################

   if override_demo_pgrst_port is not None:
      shutil.move('.env','.env.bak');
      
      with open('.env.bak',"r") as s:
         with open('.env',"w") as d:
            for line in s:
               if line.strip() == "#POSTGREST_PORT=3000" or line[15:] == "POSTGREST_PORT=":
                  d.write("POSTGREST_PORT=" + str(override_demo_pgrst_port) + "\n");
               else:
                  d.write(line);
   
   if override_demo_pgrst_host is not None:
      shutil.move('.env','.env.bak');
      
      with open('.env.bak',"r") as s:
         with open('.env',"w") as d:
            for line in s:
               if line.strip() == "#POSTGREST_HOST=localhost" or line[15:] == "POSTGREST_HOST=":
                  d.write("POSTGREST_HOST=" + str(override_demo_pgrst_host) + "\n");
               else:
                  d.write(line);
                  
   if override_demo_gis_prot is not None:
      shutil.move('.env','.env.bak');
      
      with open('.env.bak',"r") as s:
         with open('.env',"w") as d:
            for line in s:
               if line.strip() == "#GIS_PROT=http" or line[9:] == "GIS_PROT=":
                  d.write("GIS_PROT=" + str(override_demo_gis_prot) + "\n");
               else:
                  d.write(line);
                  
   if override_demo_gis_port is not None:
      shutil.move('.env','.env.bak');
      
      with open('.env.bak',"r") as s:
         with open('.env',"w") as d:
            for line in s:
               if line.strip() == "#GIS_PORT=3000" or line[9:] == "GIS_PORT=":
                  d.write("GIS_PORT=" + str(override_demo_gis_port) + "\n");
               else:
                  d.write(line);
   
   if override_demo_gis_host is not None:
      shutil.move('.env','.env.bak');
      
      with open('.env.bak',"r") as s:
         with open('.env',"w") as d:
            for line in s:
               if line.strip() == "#GIS_HOST=localhost" or line[9:] == "GIS_HOST=":
                  d.write("GIS_HOST=" + str(override_demo_gis_host) + "\n");
               else:
                  d.write(line);

   print("Running compose build for demo containers.");
   cmd = ["docker","compose","build"];
   if build_nocache:
      cmd.append("--no-cache");
   dzproc(cmd);

   ###############################################################################
   print("Running compose up for demo containers.");
   cmd = ["docker","compose","up","-d"];
   dzproc(cmd);
   
   ###############################################################################
   os.chdir('../gis');
   print("Running compose build for gis containers.");
   cmd = ["docker","compose","build"];
   if build_nocache:
      cmd.append("--no-cache");
   dzproc(cmd);

   ###############################################################################
   print("Running compose up for gis containers.");
   cmd = ["docker","compose","up","-d"];
   dzproc(cmd);

   ###############################################################################
   # set context to admin container
   ###############################################################################
   os.chdir('../admin');
   ###############################################################################
   
   ###############################################################################
   print("Running compose build for admin containers.");
   cmd = ["docker","compose","build"];
   if build_nocache:
      cmd.append("--no-cache");
   dzproc(cmd);

   ###############################################################################
   print("Running compose up for admin containers.");
   cmd = ["docker","compose","up","-d"];
   dzproc(cmd);

   ###############################################################################
   # Support
   cipld(
       ipnyb                = 'cipsrv_support'
      ,dumpfile             = support_dumpfile
      ,dumpfile_copyin      = support_dumpfile_copyin
      ,dumpfile_parm        = '--support_dumpfile'
      ,override_username    = override_username
   );
   
   # Support Logic
   cipgt(
       ipnyb                = 'cipsrv_support'
      ,override_git_branch  = override_git_branch
      ,force_nogit_uselocal = force_nogit_uselocal
      ,override_username    = override_username
   );

   z = 0;
   ###############################################################################
   if recipe in ['MRONLY','ALL','VPU09','EXTENDED']:
      # NHDPlus MR
      cipld(
          ipnyb                = 'cipsrv_nhdplus_m'
         ,dumpfile             = mr_dumpfile
         ,dumpfile_copyin      = mr_dumpfile_copyin
         ,dumpfile_parm        = '--mr_dumpfile'
         ,override_username    = override_username
      );
      # EPAGeoFab MR
      cipld(
          ipnyb                = 'cipsrv_epageofab_m'
         ,dumpfile             = mrgf_dumpfile
         ,dumpfile_copyin      = mrgf_dumpfile_copyin
         ,dumpfile_parm        = '--mrgf_dumpfile'
         ,override_username    = override_username
      );
            
      # NHDPlus MR Logic
      cipgt(
          ipnyb                = 'cipsrv_nhdplus_m'
         ,override_git_branch  = override_git_branch
         ,force_nogit_uselocal = force_nogit_uselocal
         ,override_username    = override_username
      );
      z += 1;
      
   ###############################################################################
   if recipe in ['HRONLY','ALL','VPU09','EXTENDED']:
      # NHDPlus HR
      cipld(
          ipnyb                = 'cipsrv_nhdplus_h'
         ,dumpfile             = hr_dumpfile
         ,dumpfile_copyin      = hr_dumpfile_copyin
         ,dumpfile_parm        = '--hr_dumpfile'
         ,override_username    = override_username
      );
      # EPAGeoFab HR
      cipld(
          ipnyb                = 'cipsrv_epageofab_h'
         ,dumpfile             = hrgf_dumpfile
         ,dumpfile_copyin      = hrgf_dumpfile_copyin
         ,dumpfile_parm        = '--hrgf_dumpfile'
         ,override_username    = override_username
      );
      
      # HR Logic      
      cipgt(
          ipnyb                = 'cipsrv_nhdplus_h'
         ,override_git_branch  = override_git_branch
         ,force_nogit_uselocal = force_nogit_uselocal
         ,override_username    = override_username
      );
      z += 1;

   ###############################################################################
   if z == 0:
      print("Error no NHDPlus datasets loaded using these parameters");
      sys.exit(-50);
   
   ###############################################################################
   # NHDPLUS 2
   ###############################################################################
   if recipe in ['EXTENDED']:
      # NHDPlus MR 2
      cipld(
          ipnyb                = 'cipsrv_nhdplus2_m'
         ,dumpfile             = mr2_dumpfile
         ,dumpfile_copyin      = mr2_dumpfile_copyin
         ,dumpfile_parm        = '--mr2_dumpfile'
         ,override_username    = override_username
      );
           
   ###############################################################################
   if recipe in ['EXTENDED']:
      # NHDPlus HR 2
      cipld(
          ipnyb                = 'cipsrv_nhdplus2_h'
         ,dumpfile             = hr2_dumpfile
         ,dumpfile_copyin      = hr2_dumpfile_copyin
         ,dumpfile_parm        = '--hr2_dumpfile'
         ,override_username    = override_username
      );
   
   ###############################################################################
   # GRIDS
   ###############################################################################
   if recipe in ['EXTENDED']:
      # NHDPlus MR Grid
      cipld(
          ipnyb                = 'cipsrv_nhdplusgrid_m'
         ,dumpfile             = mrgrid_dumpfile
         ,dumpfile_copyin      = mrgrid_dumpfile_copyin
         ,dumpfile_parm        = '--mrgrid_dumpfile'
         ,override_username    = override_username
      );

   ###############################################################################
   if recipe in ['EXTENDED']:
      # NHDPlus HR Grid
      cipld(
          ipnyb                = 'cipsrv_nhdplusgrid_h'
         ,dumpfile             = hrgrid_dumpfile
         ,dumpfile_copyin      = hrgrid_dumpfile_copyin
         ,dumpfile_parm        = '--hrgrid_dumpfile'
         ,override_username    = override_username
      );
            
   ###############################################################################
   # TOPO
   ###############################################################################
   if recipe in ['EXTENDED']:
      # NHDPlus MR TOPO
      cipld(
          ipnyb                = 'cipsrv_nhdplustopo_m'
         ,dumpfile             = mrtp_dumpfile
         ,dumpfile_copyin      = mrtp_dumpfile_copyin
         ,dumpfile_parm        = '--mrtp_dumpfile'
         ,override_username    = override_username
      );

   ###############################################################################
   if recipe in ['EXTENDED']:
      # NHDPlus HR Topo
      cipld(
          ipnyb                = 'cipsrv_nhdplustopo_h'
         ,dumpfile             = hrtp_dumpfile
         ,dumpfile_copyin      = hrtp_dumpfile_copyin
         ,dumpfile_parm        = '--hrtp_dumpfile'
         ,override_username    = override_username
      );
      
   ###############################################################################
   # Watersheds
   ###############################################################################
   if recipe in ['EXTENDED']:
      # NHDPlus MR Watersheds
      cipld(
          ipnyb                = 'cipsrv_nhdpluswshd_m'
         ,dumpfile             = mrws_dumpfile
         ,dumpfile_copyin      = mrws_dumpfile_copyin
         ,dumpfile_parm        = '--mrws_dumpfile'
         ,override_username    = override_username
      );
      
   ###############################################################################
   # OWLD
   ###############################################################################
   if recipe in ['EXTENDED']:
      # OWLD datasets
      cipld(
          ipnyb                = 'cipsrv_owld'
         ,dumpfile             = owld_dumpfiles
         ,dumpfile_copyin      = owld_dumpfiles_copyin
         ,dumpfile_parm        = '--owld_dumpfiles'
         ,multi_flag           = True
         ,override_username    = override_username
      );
      
      cipgt(
          ipnyb                = 'cipsrv_owld'
         ,override_git_branch  = override_git_branch
         ,force_nogit_uselocal = force_nogit_uselocal
         ,override_username    = override_username
      );
      
   ###############################################################################
   # GIS
   # The views deployed are crafted to bounce if the underlying data is absent
   # If this causes issues in the deployment, just alter the if to avoid
   ###############################################################################
   if 1==1:
      
      cipgt(
          ipnyb                = 'cipsrv_gis'
         ,override_git_branch  = override_git_branch
         ,force_nogit_uselocal = force_nogit_uselocal
         ,override_username    = override_username
      );
      
   ###############################################################################
   # Engine
   ###############################################################################
   cipgt(
       ipnyb                = 'cipsrv_engine'
      ,override_git_branch  = override_git_branch
      ,force_nogit_uselocal = force_nogit_uselocal
      ,override_username    = override_username
   );

   ###############################################################################
   # PGRest
   ###############################################################################
   cipgt(
       ipnyb                = 'cipsrv_pgrest'
      ,override_git_branch  = override_git_branch
      ,force_nogit_uselocal = force_nogit_uselocal
      ,override_username    = override_username
   );

   ###############################################################################
   print("CIP-Service loaded and ready for evaluation.");

if __name__ == '__main__':

   args = parser.parse_args();

   main(
       recipe                   = args.recipe
       
      ,mr_dumpfile              = args.mr_dumpfile
      ,mr_dumpfile_copyin       = args.mr_dumpfile_copyin
      ,hr_dumpfile              = args.hr_dumpfile
      ,hr_dumpfile_copyin       = args.hr_dumpfile_copyin
      
      ,mrgf_dumpfile            = args.mrgf_dumpfile
      ,mrgf_dumpfile_copyin     = args.mrgf_dumpfile_copyin
      ,hrgf_dumpfile            = args.hrgf_dumpfile
      ,hrgf_dumpfile_copyin     = args.hrgf_dumpfile_copyin
      
      ,mr2_dumpfile             = args.mr2_dumpfile
      ,mr2_dumpfile_copyin      = args.mr2_dumpfile_copyin
      ,hr2_dumpfile             = args.hr2_dumpfile
      ,hr2_dumpfile_copyin      = args.hr2_dumpfile_copyin
      
      ,mrgrid_dumpfile          = args.mrgrid_dumpfile
      ,mrgrid_dumpfile_copyin   = args.mrgrid_dumpfile_copyin
      ,hrgrid_dumpfile          = args.hrgrid_dumpfile
      ,hrgrid_dumpfile_copyin   = args.hrgrid_dumpfile_copyin
      
      ,mrtp_dumpfile            = args.mrtp_dumpfile
      ,mrtp_dumpfile_copyin     = args.mrtp_dumpfile_copyin
      ,hrtp_dumpfile            = args.hrtp_dumpfile
      ,hrtp_dumpfile_copyin     = args.hrtp_dumpfile_copyin
      
      ,mrws_dumpfile            = args.mrws_dumpfile
      ,mrws_dumpfile_copyin     = args.mrws_dumpfile_copyin
      ,hrws_dumpfile            = args.hrws_dumpfile
      ,hrws_dumpfile_copyin     = args.hrws_dumpfile_copyin
      
      ,owld_dumpfiles           = args.owld_dumpfiles
      ,owld_dumpfiles_copyin    = args.owld_dumpfiles_copyin
      
      ,support_dumpfile         = args.support_dumpfile
      ,support_dumpfile_copyin  = args.support_dumpfile_copyin
      
      ,override_postgresql_port = args.override_postgresql_port
      
      ,override_demo_pgrst_port = args.override_demo_pgrst_port
      ,override_demo_pgrst_host = args.override_demo_pgrst_host
      ,override_demo_gis_prot   = args.override_demo_gis_prot
      ,override_demo_gis_port   = args.override_demo_gis_port
      ,override_demo_gis_host   = args.override_demo_gis_host
      
      ,override_engine_profile  = args.override_engine_profile
      ,override_git_branch      = args.override_git_branch
      ,override_username        = args.override_username
      ,force_nogit_uselocal     = args.force_nogit_uselocal
      ,down_volumes             = args.down_volumes
      ,build_nocache            = args.build_nocache
   );
