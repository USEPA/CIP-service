import os,sys,jinja2,yaml,argparse,datetime;

script_root = os.path.realpath(os.path.dirname(__file__));
parser = argparse.ArgumentParser(description='configuration script for cipsrv');
parser.add_argument("--bundle");
parser.add_argument("--bprofile");
parser.add_argument("--templates");
parser.add_argument("--output");
args = parser.parse_args();

bundle    = args.bundle;
bprofile  = args.bprofile;
templates = args.templates;
outdir    = args.output;

if bundle is None:
   raise Exception("bundle parameter is empty");
   
if bprofile is None:
   raise Exception("bprofile parameter is empty");
else:
   if bprofile[-4:] != '.yml':
      bprofile = bprofile + '.yml';
   
if templates is None:
   templates = os.path.join('..',bundle);
   
   if not os.path.exists(templates):
      templates = os.path.join(bundle);

if outdir is None:
   outdir = templates;

profiles = os.path.join(templates,"profiles");
time_now = datetime.datetime.utcnow();

environment = jinja2.Environment(loader=jinja2.FileSystemLoader(templates));

if os.path.exists(os.path.join(profiles,"_common.yml")):
   print(".  reading _common.yml");
   with open(os.path.join(profiles,"_common.yml"),'r') as file:
      common = yaml.safe_load(file.read());

   print(".  reading " + bprofile);
   with open(os.path.join(profiles,bprofile),'r') as file:
      stream = common | yaml.safe_load(file.read());

else:
   print(".  reading " + bprofile);
   with open(os.path.join(profiles,bprofile),'r') as file:
      stream = yaml.safe_load(file.read());
      
for file in os.listdir(templates):

   if file.endswith('.j2'):
   
      if file.startswith('docker-compose'):
         output = os.path.splitext(file)[0] + '.yml';
      elif file.startswith('Dockerfile'):
         output = 'Dockerfile.' + file[10:12];
      else:
         output = None;
         
      if output is not None:  
         print(".  processing " + file + " into " + output + ".");     
         template = environment.get_template(file);
         template.globals['now'] = time_now;
         
         outrender = template.render(stream);  
         with open(os.path.join(outdir,output),"w") as file:
            file.write(outrender);

print(".  configuration complete.");
