import os,sys,wget,re,getpass;
import psycopg2,base64,shlex;
from subprocess import PIPE,Popen;
from contextlib import closing;

def download(s3,src,ld,trg):
   
   if os.path.exists(os.path.join(ld,trg)):
      os.remove(os.path.join(ld,trg));

   wget.download(s3 + src,os.path.join(ld,trg));
    
   return True;
   
def pg_restore(host_name,host_port,database_name,user_name,database_password,dumpfile,threads=1):
    
   command = 'pg_restore -c --if-exists -h {0} -p {1} -d {2} -U {3} -j {5} {4}'\
             .format(host_name,host_port,database_name,user_name,dumpfile,threads);

   command = shlex.split(command);
    
   p = Popen(command,shell=False,stdin=PIPE,stdout=PIPE,stderr=PIPE);
    
   return p.communicate(bytes('{}\n'.format(database_password),'utf-8'));

def ogr2ogr(cmdstring):
    
   command = 'ogr2ogr {0}'.format(cmdstring);

   command = shlex.split(command);

   p = Popen(command,shell=False,stdin=PIPE,stdout=PIPE,stderr=PIPE);
    
   return p.communicate();
   
def load_sqlfile(conn,sqlfile,echo=False):
    
   resp = [];
    
   splitters = [
       '^CREATE .*'
      ,'^ALTER .*'
      ,'^GRANT .*'
      ,'^DROP .*'
      ,r'^DO \$\$.*'
      ,'^ANALYZE .*'
   ];
   splitmatch = "(" + ")|(".join(splitters) + ")";
    
   if not os.path.exists(sqlfile):
      raise Exception(sqlfile + ' not found.');

   with closing(conn.cursor()) as cursor:
        
      with open(sqlfile,'r') as file:
            
         sqltxt = None;
         sqlbuf = [];
         for line in file:
            
            if not re.match(r"(^--\*\*\*\*\*\*.*)|(^-----)",line):
                    
               if re.match(splitmatch,line) and len(sqlbuf) > 0:
                        
                  sqltxt = ''.join(sqlbuf);
                  cursor.execute(sqltxt);
                  sm = cursor.statusmessage;
                  tx = sqlbuf[0][:80].strip() + ' => ' + sm;
                  resp.append(tx);
                        
                  if echo:
                     print(tx);
                        
                  conn.commit();
                  sqlbuf = [];
                    
               # remove empty lines outside commands
               if len(sqlbuf) == 0 and re.match('^\n',line):
                  None;
               else:
                  sqlbuf.append(line);
                        
         if len(sqlbuf) > 0:
            sqltxt = ''.join(sqlbuf);
            cursor.execute(sqltxt);
            sm = cursor.statusmessage;
            tx = sqlbuf[0][:80].strip() + ' => ' + sm;
            resp.append(tx);
               
            if echo:
               print(tx);
               
            conn.commit();
    
   return resp;

def upsert_registry(conn,component_name,component_type,src,override_username,notes=None,echo=False):
   
   if override_username is None:
      username = getpass.getuser();
   else:
      username = override_username;
      
   sqltxt = """
      INSERT INTO cipsrv.registry(
          component
         ,component_type
         ,component_vintage
         ,installer_username
         ,installation_date
         ,notes
      ) VALUES (
          %s,%s,%s,%s,CURRENT_TIMESTAMP,%s
      );
   """;
   
   with closing(conn.cursor()) as cursor:
      cursor.execute(sqltxt,(component_name,component_type,src,username,notes));
      sm = cursor.statusmessage;
      tx = sqltxt[:80].strip() + ' => ' + sm;
   
   if echo:
      print(tx);
               
   conn.commit();
   
   return 0;
   