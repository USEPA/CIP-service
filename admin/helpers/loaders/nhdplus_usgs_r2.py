from osgeo import ogr;
import os,sys,psycopg2,requests,zipfile,shutil,subprocess,shlex;
from requests.adapters import HTTPAdapter;
from urllib3.util import Retry;

ogr.UseExceptions()

dbse = os.environ['POSTGRESQL_DB'];
host = os.environ['POSTGRESQL_HOST'];
port = os.environ['POSTGRESQL_PORT'];
user = 'cipsrv';
pasw = os.environ['POSTGRESQL_CIP_PASS'];

cs = "dbname=%s user=%s password=%s host=%s port=%s" % (
     dbse
    ,user
    ,pasw
    ,host
    ,port
);

try:
    conn = psycopg2.connect(cs);
except:
    raise Exception("database connection error");

print("database is ready");

with open('nhdplus_usgs_r2.txt', 'r') as file:
    for line in file:
        if line[0] != '#':
            (vpuid,url) = line.strip().split(',');

            if os.path.exists('vpu' + vpuid + '.zip'):
                os.remove('vpu' + vpuid + '.zip');

            if os.path.exists('vpu' + vpuid):
                shutil.rmtree('vpu' + vpuid);
                
            r = requests.get(url);

            try:
                retry = Retry(
                    total=5,
                    backoff_factor=2,
                    status_forcelist=[429, 500, 502, 503, 504],
                );
            
                adapter = HTTPAdapter(max_retries=retry);
            
                session = requests.Session();
                session.mount('https://', adapter);
                r = session.get(url, timeout=180);
            
            except Exception as e:
                print(e)

            
            with open('vpu' + vpuid + '.zip',"wb") as f:
                f.write(r.content);

            with zipfile.ZipFile('vpu' + vpuid + '.zip','r') as zip_ref:
                zip_ref.extractall('vpu' + vpuid);

            gdb = None;
            for item in os.listdir('vpu' + vpuid):
                
                if '.gdb' in item:
                    gdb = item;
                
            ogrgdb = ogr.Open(os.path.join('vpu' + vpuid,gdb));
            for item in ogrgdb:
                lyr = item.GetName();
                
                if 'NHDPlusIncr' in lyr:

                    cmd = 'ogr2ogr -f "PostgreSQL" PG:"' + cs + '" vpu' + vpuid + os.sep + gdb + ' ' + lyr + ' '\
                        + '-nln ' + 'cipsrv_upload.vpu' + vpuid + '_' + lyr.lower();
                    cmd = shlex.split(cmd);
                    
                    output, error = subprocess.Popen(
                         cmd
                        ,shell = False
                        ,universal_newlines = True
                        ,stdout = subprocess.PIPE
                        ,stderr = subprocess.PIPE
                    ).communicate();

            print('vpu' + vpuid);