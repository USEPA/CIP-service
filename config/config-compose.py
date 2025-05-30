import os,sys,jinja2,yaml,argparse,datetime,shutil,getpass;

script_root = os.path.realpath(os.path.dirname(__file__));
parser = argparse.ArgumentParser(description='configuration script for cipsrv');
parser.add_argument("--bundle",    required = True);
parser.add_argument("--bprofile",  required = True);
parser.add_argument("--templates", required = False);
parser.add_argument("--output",    required = False);
   
def main(
    bundle
   ,bprofile
   ,templates = None
   ,outdir    = None
):
   if bundle is None:
      raise Exception("bundle parameter is empty");
      
   if bprofile is None:
      raise Exception("bprofile parameter is empty");

   else:
      if bprofile[-4:] == '.yml':
         bprofile     = bprofile[:-4];
         bprofile_yml = bprofile;
      
      else:
         bprofile_yml = bprofile + '.yml';
      
   if templates is None:
      templates = os.path.join('..',bundle);
      
      if not os.path.exists(templates):
         templates = os.path.join(bundle);

   if outdir is None:
      outdir = templates;

   profiles = os.path.join(templates,"profiles");
   time_now = datetime.datetime.now(datetime.timezone.utc);

   environment = jinja2.Environment(loader=jinja2.FileSystemLoader(templates));

   if os.path.exists(os.path.join(profiles,"_common.yml")):
      print(".  reading _common.yml");
      with open(os.path.join(profiles,"_common.yml"),'r') as file:
         common = yaml.safe_load(file.read());

      if bprofile == 'default':
         if 'default' in common:
            bprofile_yml = common['default'] + '.yml';
            print('.  using ' + bprofile_yml + ' as default profile.');
            
         else:
            raise Exception('unable to determine default profile.');
      
      print(".  reading " + bprofile_yml);
      with open(os.path.join(profiles,bprofile_yml),'r') as file:
         stream = common | yaml.safe_load(file.read());

   else:
      print(".  reading " + bprofile_yml);
      with open(os.path.join(profiles,bprofile_yml),'r') as file:
         stream = yaml.safe_load(file.read());
         
   has_env = False;
   has_sample_env = False;
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
            template.globals['cip_installer'] = getpass.getuser();
            
            outrender = template.render(stream);  
            with open(os.path.join(outdir,output),"w") as file:
               file.write(outrender);

      if str(file) == '.env':
         has_env = True;
         
      if str(file) == 'env.example':
         has_sample_env = True;

   if has_env is True:
      print(".  ignoring existing .env file, please make sure existing secrets are appropriate.");

   elif has_env is False and has_sample_env is True:
      print(".  creating new .env file, secrets defaults should be reviewed and strengthened per your requirements.");
      
      shutil.copyfile(
          os.path.join(outdir,'env.example')
         ,os.path.join(outdir,'.env')
      );
      
   print(".  configuration complete.");

if __name__ == '__main__':

   args = parser.parse_args();

   main(
       bundle    = args.bundle
      ,bprofile  = args.bprofile
      ,templates = args.templates
      ,outdir    = args.output
   );
