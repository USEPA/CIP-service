# Your host Python will require the jinja2 package which can be installed via
# >pip install Jinja2
#
import os,sys,importlib,argparse,shutil,yaml,getpass;
from subprocess import Popen, PIPE;
cc = importlib.import_module("config-compose");

script_root = os.path.realpath(os.path.dirname(__file__));
parser = argparse.ArgumentParser(description='stand alone cipsrv admin');

parser.add_argument("--outdir"                  ,required=True ,default=None);
parser.add_argument("--loading_dock_bind"       ,required=False,default=None);

def main(
    outdir
   ,loading_dock_bind = None
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

   ###############################################################################
   # set context to project directory
   ###############################################################################
   os.chdir(os.path.dirname(os.path.realpath(__file__)));
   
   if loading_dock_bind is not None:
      if loading_dock_bind != "":
         os.environ['LOADING_DOCK_BIND'] = loading_dock_bind;
         
   ###############################################################################
   if not os.path.exists(outdir):
      os.mkdir(outdir);
      
   if not os.path.exists(os.path.join(outdir,'.env')) \
   and os.path.exists(os.path.join('../admin','.env')):
      shutil.copyfile(
          os.path.join('../admin','.env')
         ,os.path.join(outdir,'.env')
      );

   else:
      if not os.path.exists(os.path.join(outdir,'.env')):
         shutil.copyfile(
             os.path.join('../admin','env.example')
            ,os.path.join(outdir,'.env')
         );
   
   if not os.path.exists(os.path.join(outdir,'jupyter')):
      shutil.copytree(
          os.path.join('../admin','jupyter')
         ,os.path.join(outdir,'jupyter')
      );     
   
   ###############################################################################
   print("Configuring compose for admin containers using standalone settings.");
   cc.main(
       bundle    = "admin"
      ,bprofile  = "standalone"
      ,outdir    = outdir
   );  

   ###############################################################################
   # set context to standalone admin container
   ###############################################################################
   os.chdir(outdir);
   ###############################################################################
   
   ###############################################################################
   print("Running compose build for standalone admin.");
   cmd = ["docker","compose","build"];
   dzproc(cmd);

   ###############################################################################
   print("Running compose up for standalone admin.");
   cmd = ["docker","compose","up","-d"];
   dzproc(cmd);
   
   ###############################################################################
   cmd = [
       "docker","compose","exec","cip_jp","cp"
      ,"/home/jovyan/notebooks/common.py"
      ,"/tmp/common.py"
   ];
   dzproc(cmd);

   ###############################################################################
   print("Standalone admin loaded.");

if __name__ == '__main__':

   args = parser.parse_args();

   main(
       outdir                   = args.outdir
      ,loading_dock_bind        = args.loading_dock_bind
   );
