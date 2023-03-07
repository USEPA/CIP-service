import sys,requests,time;

if len(sys.argv) > 1:
   api_endpoint = sys.argv[1];
else:
   api_endpoint = r"http://localhost:3000/rpc";
   
###############################################################################
# Hit the domains service to wake things up
###############################################################################
resp = requests.get(api_endpoint + r"/cipsrv_domains");
if resp.status_code == 400:
   time.sleep(1);
   resp = requests.get(api_endpoint + r"/cipsrv_domains");
   if resp.status_code == 503:
      time.sleep(1);
      resp = requests.get(api_endpoint + r"/cipsrv_domains");

print("testing domains endpoint:"); 
assert(resp.status_code == 200),str(resp.status_code);
rez = resp.json();
print("   found " + str(len(rez['states'])) + " states and " + str(len(rez['tribes'])) + " tribes.");

###############################################################################
# Small navigation UM for 15 km
###############################################################################
print("simple point indexing 1:");
payload = {
   "start_nhdplusid": 10000500053951,
   "start_measure": 34.0,
   "max_distancekm": 15,
   "search_type": "UM",
   "nhdplus_version": "nhdplus_h"
}
resp = requests.post(api_endpoint + r"/cipsrv_nav",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowline_count'] == 26),'flowline_count';
coords = rez['flowlines']['features'][0]['geometry']['coordinates'];
assert(coords[0][0] == -78.615283845)
assert(coords[0][1] == 38.737728073)
print("   done.");
