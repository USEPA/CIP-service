## Demo Bundle

The demo bundle is provided as an easy way to quickly stand up web interfaces to the provided API.  EPA welcomes your additions to the sample set.  However, in some case one might wish to forgo the extra server **and** yet occasionally run a demo to check a particular result.

Assuming folks interested in CIP indexing also have a desktop GIS such ArcGIS Pro or [QGIS](https://qgis.org/en/site/index.html) available, the current set of included demos located [here](https://github.com/USEPA/CIP-service/tree/main/demo/nginx/html) can be run in a stand-alone fashion using the Python command prompt.  To launch a local server, first change directory to the nginx location:

* cd \<\<my file location\>\>\CIP-service\demo\nginx
* python -m http.server

This will launch a small webserver on port 8080 on your local machine.  There are then two options to access your API:

1. If your CIP-service API resides at a stable address, alter your local [config.js](https://github.com/USEPA/CIP-service/blob/main/demo/nginx/html/config.js) file with that information.  Then just open the demo web page (http://localhost:8000/html/cipsrv_indexer.html).  Be careful not to check back in this information if you submit pulls to the repository.
2. Or if your CIP-service API moves around you can create a more dynamic urlstring pointing to the API as in http://localhost:8000/html/cipsrv_indexer.html?postgrest_host=cip-api.aws.epa.gov&postgrest_port=443.  This method is a tad safer than hard-coding your local network details in the config file.

Either method will allow the demo logic to call your CIP-service API.
