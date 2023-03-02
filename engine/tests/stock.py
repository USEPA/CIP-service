import sys,requests,time;

if len(sys.argv) > 1:
   api_endpoint = sys.argv[1];
else:
   api_endpoint = r"http://localhost:3000/rpc";
   
###############################################################################
# Hit the domains service
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
# Simple point indexing #1
###############################################################################
print("simple point indexing 1:");
payload = {
   "geometry": {
      "type": "Point",
      "coordinates": [-96.724834, 46.904425]
   },
   "geometry_clip": ["USPS:MN"],
   "geometry_clip_stage": "BEFORE",
   "catchment_filter": ["MN"],
   "nhdplus_version": "nhdplus_m",
   "default_point_indexing_method": "point_simple",
   "default_line_indexing_method": "line_levelpath",
   "default_ring_indexing_method": "area_simple",
   "default_area_indexing_method": "area_centroid",
   "default_line_threshold": "10",
   "default_areacat_threshold": "50",
   "default_areaevt_threshold": "1",
   "return_catchment_geometry": True,
   "return_flowlines": False,
   "return_flowline_geometry": True,
   "return_huc12s": False
}
resp = requests.post(api_endpoint + r"/cipsrv_index",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowlines'] is None),'flowlines';
coords = rez['indexed_points']['features'][0]['geometry']['coordinates'];
assert(coords[0] == -96.724834)
assert(coords[1] == 46.904425)
assert(rez['indexed_lines'] is None),'indexed_lines';
assert(rez['indexed_areas'] is None),'indexed_areas';
assert(rez['nhdplus_version'] == 'nhdplus_m'),'nhdplus_version';
assert(rez['catchment_count'] == 1),'catchment_count: ' + str(rez['catchment_count']);
print("   done.");

###############################################################################
# Simple point indexing #2
###############################################################################
print("simple point indexing 2:");
payload = {
   "geometry": {
      "type": "Point",
      "coordinates": [-96.724834, 46.904425]
   },
   "geometry_clip": ["USPS:MN"],
   "geometry_clip_stage": "BEFORE",
   "catchment_filter": ["MN"],
   "nhdplus_version": "nhdplus_m",
   "default_point_indexing_method": "point_simple",
   "default_line_indexing_method": "line_levelpath",
   "default_ring_indexing_method": "area_simple",
   "default_area_indexing_method": "area_centroid",
   "default_line_threshold": "10",
   "default_areacat_threshold": "50",
   "default_areaevt_threshold": "1",
   "return_indexed_features": False,
   "return_indexed_collection": True,
   "return_catchment_geometry": True,
   "return_flowlines": False,
   "return_flowline_geometry": True,
   "return_huc12s": False
}
resp = requests.post(api_endpoint + r"/cipsrv_index",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowlines'] is None),'flowlines';
assert(rez['indexed_points'] is None),'indexed_points';
assert(rez['indexed_lines'] is None),'indexed_lines';
assert(rez['indexed_areas'] is None),'indexed_areas';
coords = rez['indexed_collection']['geometry']['coordinates'];
assert(coords[0] == -96.724834)
assert(coords[1] == 46.904425)
assert(rez['nhdplus_version'] == 'nhdplus_m'),'nhdplus_version';
assert(rez['catchment_count'] == 1),'catchment_count: ' + str(rez['catchment_count']);
print("   done.");

###############################################################################
# Simple point indexing #3 - smallest possible footprint
###############################################################################
print("simple point indexing 3:");
payload = {
   "geometry": {
      "type": "Point",
      "coordinates": [-96.724834, 46.904425]
   },
   "geometry_clip": None,
   "geometry_clip_stage": "BEFORE",
   "catchment_filter": ["MN"],
   "nhdplus_version": "nhdplus_m",
   "default_point_indexing_method": "point_simple",
   "default_line_indexing_method": "line_levelpath",
   "default_ring_indexing_method": "area_simple",
   "default_area_indexing_method": "area_centroid",
   "default_line_threshold": "10",
   "default_areacat_threshold": "50",
   "default_areaevt_threshold": "1",
   "return_indexed_features": False,
   "return_indexed_collection": False,
   "return_catchment_geometry": False,
   "return_flowlines": False,
   "return_flowline_geometry": False,
   "return_huc12s": False
}
resp = requests.post(api_endpoint + r"/cipsrv_index",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['flowlines'] is None),'flowlines';
assert(rez['indexed_points'] is None),'indexed_points';
assert(rez['indexed_lines'] is None),'indexed_lines';
assert(rez['indexed_areas'] is None),'indexed_areas';
assert(rez['indexed_collection'] is None),'indexed_collection';
assert(rez['nhdplus_version'] == 'nhdplus_m'),'nhdplus_version';
assert(rez['catchment_count'] == 1),'catchment_count: ' + str(rez['catchment_count']);
print("   done.");

###############################################################################
# Simple line indexing #1
###############################################################################
print("simple line indexing 1:");
payload = {
   "geometry": {
      "type": "LineString",
      "coordinates": [[-96.72226, 46.904776], [-96.747665, 46.86958], [-96.721573, 46.842816], [-96.677628, 46.859252]]
   },
   "nhdplus_version": "nhdplus_m",
   "default_point_indexing_method": "point_simple",
   "default_line_indexing_method": "line_simple",
   "default_ring_indexing_method": "area_simple",
   "default_area_indexing_method": "area_simple",
   "default_line_threshold": "10",
   "return_catchment_geometry": True,
   "return_flowlines": False,
   "return_flowline_geometry": True,
   "return_huc12s": False
}
resp = requests.post(api_endpoint + r"/cipsrv_index",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['nhdplus_version'] == 'nhdplus_m'),'nhdplus_version';
assert(rez['catchment_count'] == 4),'catchment_count: ' + str(rez['catchment_count']);
print("   done.");

###############################################################################
# Ring conversion indexing #1
###############################################################################
print("ring conversion indexing 1:");
payload = {
   "geometry": {
      "type": "LineString",
      "coordinates": [[-96.730156,46.889058],[-96.746292,46.859487],[-96.726379,46.838354],[-96.689987,46.845869],[-96.679001,46.864416],[-96.700287,46.886243],[-96.730156,46.889058]]
   },
   "nhdplus_version": "nhdplus_m",
   "default_point_indexing_method": "point_simple",
   "default_line_indexing_method": "line_simple",
   "default_ring_indexing_method": "area_simple",
   "default_area_indexing_method": "area_simple",
   "default_line_threshold": "10",
   "default_areacat_threshold": "50",
   "default_areaevt_threshold": "1",
   "return_catchment_geometry": True,
   "return_flowlines": False,
   "return_flowline_geometry": True,
   "return_huc12s": False
}
resp = requests.post(api_endpoint + r"/cipsrv_index",json=payload);
rez = resp.json();
assert(resp.status_code == 200),str(rez);
assert(rez['return_code'] == 0),'return_code: ' + str(rez['return_code']);
assert(rez['nhdplus_version'] == 'nhdplus_m'),'nhdplus_version';
assert(rez['catchment_count'] == 3),'catchment_count: ' + str(rez['catchment_count']);
chk = rez['indexed_areas']['features'][0];
assert(chk['geometry']['type'] == 'Polygon'),'not polygon';
assert(chk['properties']['converted_to_ring'] is True);
print("   done.");
