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
# Small navigation Shorty 
###############################################################################
print("simple Shorty navigation 1:");
payload = {
   "start_nhdplusid": 22338561,
   "start_measure": 12.62881,
   "max_distancekm": 0.1,
   "search_type": "UM",
   "nhdplus_version": "nhdplus_m"
}
resp = requests.post(api_endpoint + r"/cipsrv_nav",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowline_count'] == 1),'flowline_count';
coords = rez['flowlines']['features'][0]['geometry']['coordinates'];
assert(coords[0] == -77.054462319)
assert(coords[1] == 38.906565387)
print("   done.");

###############################################################################
# Small navigation UM for 15 km
###############################################################################
print("simple UM navigation 2:");
payload = {
   "start_nhdplusid": 22338561,
   "start_measure": 12.62881,
   "max_distancekm": 15,
   "search_type": "UM",
   "nhdplus_version": "nhdplus_m"
}
resp = requests.post(api_endpoint + r"/cipsrv_nav",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowline_count'] == 2),'flowline_count';
coords = rez['flowlines']['features'][0]['geometry']['coordinates'];
assert(coords[0] == -77.049498124)
assert(coords[1] == 38.943983806)
print("   done.");

###############################################################################
# Small navigation UT for 15 km
###############################################################################
print("simple UT navigation 3:");
payload = {
   "start_nhdplusid": 22338561,
   "start_measure": 12.62881,
   "max_distancekm": 15,
   "search_type": "UT",
   "nhdplus_version": "nhdplus_m"
}
resp = requests.post(api_endpoint + r"/cipsrv_nav",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowline_count'] == 3),'flowline_count';
coords = rez['flowlines']['features'][0]['geometry']['coordinates'];
assert(coords[0] == -77.049498124)
assert(coords[1] == 38.943983806)
print("   done.");

###############################################################################
# Small navigation DM for 15 km
###############################################################################
print("simple DM navigation 4:");
payload = {
   "start_nhdplusid": 22338561,
   "start_measure": 12.62881,
   "max_distancekm": 15,
   "search_type": "DM",
   "nhdplus_version": "nhdplus_m"
}
resp = requests.post(api_endpoint + r"/cipsrv_nav",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowline_count'] == 10),'flowline_count';
coords = rez['flowlines']['features'][0]['geometry']['coordinates'];
assert(coords[0] == -77.054918367)
assert(coords[1] == 38.905739374)
print("   done.");
 
###############################################################################
# Small navigation DD for 15 km
###############################################################################
print("simple DD navigation 5:");
payload = {
   "start_nhdplusid": 22338561,
   "start_measure": 12.62881,
   "max_distancekm": 15,
   "search_type": "DD",
   "nhdplus_version": "nhdplus_m"
}
resp = requests.post(api_endpoint + r"/cipsrv_nav",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowline_count'] == 15),'flowline_count';
coords = rez['flowlines']['features'][0]['geometry']['coordinates'];
assert(coords[0] == -77.054918367)
assert(coords[1] == 38.905739374)
print("   done.");
 
###############################################################################
# Small navigation PP 
###############################################################################
print("simple PP navigation 6:");
payload = {
   "start_nhdplusid": 22338561,
   "start_measure": 12.62881,
   "stop_nhdplusid": 22340577
   "stop_measure": 50
   "search_type": "PP",
   "nhdplus_version": "nhdplus_m"
}
resp = requests.post(api_endpoint + r"/cipsrv_nav",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowline_count'] == 9),'flowline_count';
coords = rez['flowlines']['features'][0]['geometry']['coordinates'];
assert(coords[0] == -77.054918367)
assert(coords[1] == 38.905739374)
print("   done.");

###############################################################################
# Small navigation PPALL 
###############################################################################
print("simple PPALL navigation 7:");
payload = {
   "start_nhdplusid": 22338561,
   "start_measure": 12.62881,
   "stop_nhdplusid": 22340577
   "stop_measure": 50
   "search_type": "PPALL",
   "nhdplus_version": "nhdplus_m"
}
resp = requests.post(api_endpoint + r"/cipsrv_nav",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowline_count'] == 14),'flowline_count';
coords = rez['flowlines']['features'][0]['geometry']['coordinates'];
assert(coords[0] == -77.032302524)
assert(coords[1] == 38.832048806)
print("   done.");
 