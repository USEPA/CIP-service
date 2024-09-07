# This setup script uses Python 3 to set up a local CIP-Service with the medium resolution NHDPlus dataset using the default profiles.
# Note it will use default cip123 passwords so be sure not to expose the containers to the outside world.

# Your host Python will require the jinja2 package which can be installed via
# >pip install Jinja2
#
import os,sys,importlib;
from subprocess import Popen, PIPE;
cc = importlib.import_module("config-compose");

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
print("Running compose build for engine containers.");
cmd = ["docker","compose","build"];
dzproc(cmd);

###############################################################################
print("Running compose up for engine containers.");
cmd = ["docker","compose","up","-d"];
dzproc(cmd);

###############################################################################
os.chdir('../demo');
print("Running compose build for demo containers.");
cmd = ["docker","compose","build"];
dzproc(cmd);

###############################################################################
print("Running compose up for demo containers.");
cmd = ["docker","compose","up","-d"];
dzproc(cmd);

###############################################################################
os.chdir('../admin');
print("Running compose build for admin containers.");
cmd = ["docker","compose","build"];
dzproc(cmd);

###############################################################################
print("Running compose up for admin containers.");
cmd = ["docker","compose","up","-d"];
dzproc(cmd);

###############################################################################
print("Downloading and importing CIP support data.");
cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/pg_restore_cipsrv_support.ipynb","--to","python","--output","/tmp/pg_restore_cipsrv_support.py"];
dzproc(cmd);
cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_support.py"];
dzproc(cmd);

###############################################################################
print("Downloading and importing NHDPlus MR data.");
cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/pg_restore_cipsrv_nhdplus_m.ipynb","--to","python","--output","/tmp/pg_restore_cipsrv_nhdplus_m.py"];
dzproc(cmd);
cmd = ["docker","compose","exec","cip_jp","python3","/tmp/pg_restore_cipsrv_nhdplus_m.py"];
dzproc(cmd);

###############################################################################
print("Fetching and loading CIP support logic.");
cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/git_checkout_cipsrv_support.ipynb","--to","python","--output","/tmp/git_checkout_cipsrv_support.py"];
dzproc(cmd);
cmd = ["docker","compose","exec","cip_jp","python3","/tmp/git_checkout_cipsrv_support.py"];
dzproc(cmd);

###############################################################################
print("Fetching, building and loading NHDPlus MR logic.");
cmd = ["docker","compose","exec","cip_jp","jupyter","nbconvert","/home/jovyan/notebooks/setup/git_checkout_cipsrv_nhdplus_m.ipynb","--to","python","--output","/tmp/git_checkout_cipsrv_nhdplus_m.py"];
dzproc(cmd);
cmd = ["docker","compose","exec","cip_jp","python3","/tmp/git_checkout_cipsrv_nhdplus_m.py"];
dzproc(cmd);

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
print("CIP-Service fully loaded with NHDPlus MR and ready for evaluation.");

