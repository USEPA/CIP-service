# This setup script uses Python 3 to set up a local CIP-Service with the medium resolution NHDPlus dataset using the default profiles.
# Note it will use default cip123 passwords so be sure not to expose the containers to the outside world.

# Your host Python will require the jinja2 package which can be installed via
# >pip install Jinja2
#
import os,sys,importlib,argparse,shutil,yaml;
from subprocess import Popen, PIPE;
cc = importlib.import_module("config-compose");

script_root = os.path.realpath(os.path.dirname(__file__));
parser = argparse.ArgumentParser(description='quickconfig script for cipsrv');
parser.add_argument("--recipe"                  ,required=False,default=None);
parser.add_argument("--mr_dumpfile"             ,required=False,default=None);
parser.add_argument("--mr_dumpfile_copyin"      ,required=False,default=None);
parser.add_argument("--hr_dumpfile"             ,required=False,default=None);
parser.add_argument("--hr_dumpfile_copyin"      ,required=False,default=None);
parser.add_argument("--support_dumpfile"        ,required=False,default=None);
parser.add_argument("--support_dumpfile_copyin" ,required=False,default=None);
parser.add_argument("--override_postgresql_port",required=False,default=None);
parser.add_argument("--override_demo_pgrst_host",required=False,default=None);
parser.add_argument("--down_volumes"            ,required=False,default=False,action=argparse.BooleanOptionalAction);
parser.add_argument("--build_nocache"           ,required=False,default=False,action=argparse.BooleanOptionalAction);

def main(
    recipe
   ,mr_dumpfile
   ,mr_dumpfile_copyin
   ,hr_dumpfile
   ,hr_dumpfile_copyin
   ,support_dumpfile
   ,support_dumpfile_copyin
   ,override_postgresql_port
   ,override_demo_pgrst_host
   ,down_volumes
   ,build_nocache
):

   def dzproc(cmd):
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
      if rc != 0:
         while True:
            line = proc.stderr.readline();
            if not line:
               break;
            if line.strip() != "":
               print(".  " + ' '.join(line.split()));
         sys.exit(rc);
         
   ###############################################################################
   print("Downing any existing compose services");

   os.chdir('../engine');
   if os.path.exists("docker-compose.yml"):
      print(".  engine");
      cmd = ["docker","compose","down"];
      if down_volumes:
         cmd.append("-v");
         
      dzproc(cmd);
      
   os.chdir('../admin');
   if os.path.exists("docker-compose.yml"):
      print(".  admin");
      cmd = ["docker","compose","down"];
      if down_volumes:
         cmd.append("-v");
         
      dzproc(cmd);
      
   os.chdir('../demo');
   if os.path.exists("docker-compose.yml"):
      print(".  demo");
      cmd = ["docker","compose","down"];
      if down_volumes:
         cmd.append("-v");      
      
      dzproc(cmd);
      
   os.chdir('../config');

   ###############################################################################
   if recipe is None:
      recipe = 'MRONLY'; 

   elif recipe == 'VPU09':
      print("using recipe VPU09");
      mr_dumpfile = 'cipsrv_nhdplus_m_3_vpu09.dmp';
      hr_dumpfile = 'cipsrv_nhdplus_h_3_vpu09.dmp';

   ###############################################################################
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
   os.chdir('../engine');

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
   os.chdir('../demo');

   if override_demo_pgrst_host is not None:
      shutil.move('.env','.env.bak');
      
      with open('.env.bak',"r") as s:
         with open('.env',"w") as d:
            for line in s:
               if line.strip() == "#POSTGREST_HOST=localhost" or line[15:] == "POSTGREST_HOST=":
                  d.write("POSTGREST_HOST=" + str(override_demo_pgrst_host) + "\n");
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
   os.chdir('../admin');
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
   cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/pg_restore_cipsrv_support.ipynb","--to","python","--output","/tmp/pg_restore_cipsrv_support.py"];
   dzproc(cmd);

   if support_dumpfile_copyin is not None:
      sdf = os.path.basename(support_dumpfile_copyin);
      print("Copying in external support dumpfile " + sdf + " into container.");
      cmd = ["docker","compose","cp",support_dumpfile_copyin,"cip_jp:/home/jovyan/loading_dock/" + sdf];
      dzproc(cmd);
      cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_support.py","--use_existing","--support_dumpfile",sdf];
      dzproc(cmd);

   else:
      if support_dumpfile is None:
         print("Downloading and importing default CIP support data.");
         cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_support.py"];
         dzproc(cmd);
         
      else:
         print("Downloading and importing " + support_dumpfile + " support data.");
         cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_support.py","--support_dumpfile",support_dumpfile];
         dzproc(cmd);
      
   print("Fetching and loading CIP support logic.");
   cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/git_checkout_cipsrv_support.ipynb","--to","python","--output","/tmp/git_checkout_cipsrv_support.py"];
   dzproc(cmd);
   cmd = ["docker","compose","exec","cip_jp","python3","/tmp/git_checkout_cipsrv_support.py"];
   dzproc(cmd);

   z = 0;
   ###############################################################################
   if recipe in ['MRONLY','ALL','VPU09']:
      cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/pg_restore_cipsrv_nhdplus_m.ipynb","--to","python","--output","/tmp/pg_restore_cipsrv_nhdplus_m.py"];
      dzproc(cmd);

      if mr_dumpfile_copyin is not None:
         mdf = os.path.basename(mr_dumpfile_copyin);
         print("Loading local mr dumpfile " + mdf + " into container.");
         cmd = ["docker","compose","cp",mr_dumpfile_copyin,"cip_jp:/home/jovyan/loading_dock/" + mdf];
         dzproc(cmd);
         cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_nhdplus_m.py","--use_existing","--mr_dumpfile",mdf];
         dzproc(cmd);
         
      else:
         if mr_dumpfile is None:
            print("Downloading and importing default NHDPlus MR data.");
            cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_nhdplus_m.py"];
            dzproc(cmd);
            
         else:
            print("Downloading and importing " + mr_dumpfile + " NHDPlus MR data.");
            cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_nhdplus_m.py","--mr_dumpfile",mr_dumpfile];
            dzproc(cmd);
            
      print("Fetching, building and loading NHDPlus MR logic.");
      cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/git_checkout_cipsrv_nhdplus_m.ipynb","--to","python","--output","/tmp/git_checkout_cipsrv_nhdplus_m.py"];
      dzproc(cmd);
      cmd = ["docker","compose","exec","cip_jp","python3","/tmp/git_checkout_cipsrv_nhdplus_m.py"];
      dzproc(cmd);
      z += 1;

   ###############################################################################
   if recipe in ['HRONLY','ALL','VPU09']:
      cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/pg_restore_cipsrv_nhdplus_h.ipynb","--to","python","--output","/tmp/pg_restore_cipsrv_nhdplus_h.py"];
      dzproc(cmd);

      if hr_dumpfile_copyin is not None:
         hdf = os.path.basename(hr_dumpfile_copyin);
         print("Loading local hr dumpfile " + hdf + " into container.");
         cmd = ["docker","compose","cp",hr_dumpfile_copyin,"cip_jp:/home/jovyan/loading_dock/" + hdf];
         dzproc(cmd);
         cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_nhdplus_h.py","--use_existing","--hr_dumpfile",hdf];
         dzproc(cmd);
         
      else:
         if hr_dumpfile is None:
            print("Downloading and importing default NHDPlus HR data.");
            cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_nhdplus_h.py"];
            dzproc(cmd);
            
         else:
            print("Downloading and importing " + hr_dumpfile + " NHDPlus HR data.");
            cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_nhdplus_h.py","--hr_dumpfile",hr_dumpfile];
            dzproc(cmd);
       
      print("Fetching, building and loading NHDPlus HR logic.");
      cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/git_checkout_cipsrv_nhdplus_h.ipynb","--to","python","--output","/tmp/git_checkout_cipsrv_nhdplus_h.py"];
      dzproc(cmd);
      cmd = ["docker","compose","exec","cip_jp","python3","/tmp/git_checkout_cipsrv_nhdplus_h.py"];
      dzproc(cmd);
      z += 1;

   ###############################################################################
   if z == 0:
      print("Error no NHDPlus datasets loaded using these parameters");
      sys.exit(-50);

   ###############################################################################
   print("Fetching and loading CIP Engine logic.");
   cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/git_checkout_cipsrv_engine.ipynb","--to","python","--output","/tmp/git_checkout_cipsrv_engine.py"];
   dzproc(cmd);
   cmd = ["docker","compose","exec","cip_jp","python3","/tmp/git_checkout_cipsrv_engine.py"];
   dzproc(cmd);

   ###############################################################################
   print("Fetching and loading CIP PostgREST logic.");
   cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/git_checkout_cipsrv_pgrest.ipynb","--to","python","--output","/tmp/git_checkout_cipsrv_pgrest.py"];
   dzproc(cmd);
   cmd = ["docker","compose","exec","cip_jp","python3","/tmp/git_checkout_cipsrv_pgrest.py"];
   dzproc(cmd);

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
      ,support_dumpfile         = args.support_dumpfile
      ,support_dumpfile_copyin  = args.support_dumpfile_copyin
      ,override_postgresql_port = args.override_postgresql_port
      ,override_demo_pgrst_host = args.override_demo_pgrst_host
      ,down_volumes             = args.down_volumes
      ,build_nocache            = args.build_nocache
   );
