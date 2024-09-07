# This setup script uses Python 3 to set up a local CIP-Service with the medium resolution NHDPlus dataset using the default profiles.
# Note it will use default cip123 passwords so be sure not to expose the containers to the outside world.

# Your host Python will require the jinja2 package which can be installed via
# >pip install Jinja2
#
import importlib; 
cc = importlib.import_module("config-compose");

print("Configuring compose for engine containers using default settings.");
cc.main(
    bundle    = "engine"
   ,bprofile  = "default"
);

print("Configuring compose for admin containers using default settings.");
cc.main(
    bundle    = "admin"
   ,bprofile  = "default"
);

print("Configuring compose for demo containers using default settings.");
cc.main(
    bundle    = "demo"
   ,bprofile  = "default"
);
 
os.chdir('../engine');
print("Running compose build for engine containers.");
cmd = ["docker","compose","build"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);
 
print("Running compose up for engine containers.");
cmd = ["docker","compose","up","-d"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);
 
os.chdir('../demo');
print("Running compose build for demo containers.");
cmd = ["docker","compose","build"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);
 
print("Running compose up for demo containers.");
cmd = ["docker","compose","up","-d"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);
  
os.chdir('../admin');
print("Running compose build for admin containers.");
cmd = ["docker","compose","build"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);
 
print("Running compose up for admin containers.");
cmd = ["docker","compose","up","-d"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);

print("Downloading and importing CIP support data.");
cmd = ["docker","compose","exec","cip_jp","jupyter","notebook","/home/jovyan/notebooks/setup/'pg_restore cipsrv_support.ipynb'"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);

print("Downloading and importing NHDPlus MR data.");
cmd = ["docker","compose","exec","cip_jp","jupyter","notebook","/home/jovyan/notebooks/setup/'pg_restore cipsrv_nhdplus_m.ipynb'"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);

print("Fetching and loading CIP support logic.");
cmd = ["docker","compose","exec","cip_jp","jupyter","notebook","/home/jovyan/notebooks/setup/'git_checkout cipsrv_support.ipynb'"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);

print("Fetching, building and loading NHDPlus MR logic.");
cmd = ["docker","compose","exec","cip_jp","jupyter","notebook","/home/jovyan/notebooks/setup/'git_checkout cipsrv_nhdplus_m.ipynb'"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);

print("Fetching and loading CIP Engine logic.");
cmd = ["docker","compose","exec","cip_jp","jupyter","notebook","/home/jovyan/notebooks/setup/'git_checkout cipsrv_engine.ipynb'"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);

print("Fetching and loading CIP PostgREST logic.");
cmd = ["docker","compose","exec","cip_jp","jupyter","notebook","/home/jovyan/notebooks/setup/'git_checkout cipsrv_pgrest.ipynb'"];
popen = subprocess.Popen(
    cmd
   ,stdin              = PIPE
   ,stdout             = PIPE
   ,stderr             = PIPE
   ,universal_newlines = True
);

print("CIP-Service fully loaded with NHDPlus MR and ready for evaluation.");

