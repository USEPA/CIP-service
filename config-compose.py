import os,sys,jinja2,yaml,argparse,datetime;

script_root = os.path.realpath(os.path.dirname(__file__));
parser = argparse.ArgumentParser(description='configuration script for cip20');
parser.add_argument("--profile");
parser.add_argument("--templates");
parser.add_argument("--output");
args = parser.parse_args();

profile = args.profile;
profiles = os.path.join(script_root,"profiles");

templates = args.templates;
if templates is None:
   templates = os.path.join(script_root,"templates");

outdir = args.output;
if outdir is None:
   outdir = script_root;

time_now = datetime.datetime.utcnow();

environment = jinja2.Environment(loader=jinja2.FileSystemLoader(templates));

print(".  reading _common.yml");
with open(os.path.join(profiles,"_common.yml"),'r') as file:
   common = yaml.safe_load(file.read());

print(".  reading " + profile);
with open(os.path.join(profiles,profile),'r') as file:
   stream = common | yaml.safe_load(file.read());

print(".  processing docker-compose.yml");
template = environment.get_template("docker-compose.j2");
template.globals['now'] = time_now;

output = template.render(stream);   
with open(os.path.join(outdir,"docker-compose.yml"),"w") as file:
   file.write(output);

print(".  processing Dockerfile.pg");
template = environment.get_template("Dockerfilepg.j2");
template.globals['now'] = time_now;

output = template.render(stream);   
with open(os.path.join(outdir,"Dockerfile.pg"),"w") as file:
   file.write(output);
   
print(".  processing Dockerfile.pr");
template = environment.get_template("Dockerfilepr.j2");
template.globals['now'] = time_now;

output = template.render(stream);   
with open(os.path.join(outdir,"Dockerfile.pr"),"w") as file:
   file.write(output);

print(".  processing Dockerfile.jp");
template = environment.get_template("Dockerfilejp.j2");
template.globals['now'] = time_now;

output = template.render(stream);   
with open(os.path.join(outdir,"Dockerfile.jp"),"w") as file:
   file.write(output);
   
print(".  processing Dockerfile.ng");
template = environment.get_template("Dockerfileng.j2");
template.globals['now'] = time_now;

output = template.render(stream);   
with open(os.path.join(outdir,"Dockerfile.ng"),"w") as file:
   file.write(output);

print(".  configuration complete.");
