# CIP (Catchment Index Processing) Service

The Catchment Index Processing Service provides the ability to associate hydrologic features with NHDPlus catchments. The current project is divided into three "bundles" providing functionality through several combinations of containers and standalone resources.

1. **Engine**: The engine bundle provides the core functionality of CIP.  At present there are two components, the database and the middleware API.
2. **Admin**: The admin bundle provides a set of Jupyter notebooks used in the deployment of CIP data and code to the engine.  The admin notebooks also provide a simple interface for batch processing.
3. **Demo**: The demo bundle provides a simple Nginx web server that hosts simple examples of CIP indexing.

[Streamlined Installation Instructions](docs/streamlined_installation.md)

### Capabilities

To explore the CIP-service API capabilities, see the [OpenAPI documentation](docs/openapi.yml).  Various OpenAPI 3.1 online renderers may be used to see the API rendered as document. For example, see the [Swagger Next Editor](https://editor-next.swagger.io/?spec=https://raw.githubusercontent.com/USEPA/CIP-service/main/docs/openapi.yml). Such viewers may not allow direct linking in which case the [direct link](https://raw.githubusercontent.com/USEPA/CIP-service/main/docs/openapi.yml) can be used to work around such issues.

### Overview

At this time hosting options for deploying the CIP-service bundles are quite varied ranging from Windows desktops to Linux servers to cloud hosting.  As such a single compose environment is challenging to present.  To accommodate this the project uses a jinja2 template combined with varied profile configurations to dynamically generate the needed compose and dockerfile artifacts that drive each bundle.  

To keep things simple the project promotes two general approaches, a complete local desktop containerized installation and a cloud option where the engine components are hosted in the EPA cloud. 

__Local Desktop__

![Local Desktop](docs/architecture_local.drawio.png)

__Cloud Hosted__

![Cloud-Hosted](docs/architecture_cloud.drawio.png)

### Prerequisites

- container runtime such as Docker Engine or Kubernetes\
  (Windows users may wish to utilize container managment software such as [Docker Desktop](https://www.docker.com/products/docker-desktop/) or [Rancher Desktop](https://rancherdesktop.io/))
- at least 16 GB of memory
- at least 80 GB of free disk
- internet access

### Containers Utilized

The project uses the following containers which are standard, stable and trusted images.  If you have any reason to distrust or find problematic any of the following, please open a ticket.

- postgis/postgis:15-3.4
- postgrest/postgrest:v12.0.3
- ghcr.io/osgeo/gdal:ubuntu-full-3.9.0
- nginx:1.25.5

### Profile Configuration

- inspect the provided profiles or generate a new profile meeting your host specifications.  Generally for testing purposes the default keyword will choose a profile which will work for most users.

- generate the needed compose artifacts against the selected profile:

  cd config

  - windows powershell: 
    - config-compose.ps1 engine default
    - config-compose.ps1 admin  default
    - config-compose.ps1 demo   default
  - linux shell:   
    - config-compose.sh  engine default
    - config-compose.sh  admin  default
    - config-compose.sh  demo   default
  - python 3:  
    - python config-compose.py --bundle engine --bprofile default
    - python config-compose.py --bundle admin  --bprofile default
    - python config-compose.py --bundle demo   --bprofile default

Note running the Python configuration code directly on your host is the quickest and simplest of the above methods but requires Python 3 with the jinja2 package installed.

These steps will generate the needed compose and dockerfiles in each bundle directory.  If no existing .env secrets file is found, the env.sample file will be deployed.  Make sure to inspect the resulting .env files for appropriate settings.  No changes will be made to preexisting .env files.

### Typical Deployment Steps

__Engine__

1. in the engine directory, type **docker compose -p engine build**.  This will download and configure the engine containers.  It may take a while.

2. next type **docker compose -p engine up --detach**.  This will spin up the engine containers.  Note the cip_pr container will spin up fast and throw errors while it waits for the cip_pg database to come up.  These errors can be ignored.

3. next type **docker ps** to make sure the engine containers are up and running as expected.  You should see cip_pg and cip_pr containers running.

4. At this point the engine API endpoint should be available.  For information on how to use the API, see the [OpenAPI specification here](https://petstore.swagger.io/?url=https://raw.githubusercontent.com/USEPA/CIP-service/main/docs/openapi.yml)

__Admin__

1. in the admin directory, type **docker compose -p admin build**.  The admin_local profile will attach o the engine_cip network created above.  

2. then type **docker compose -p admin up --detach**.

__[Demo](./docs/demo.md)__

1. in the demo directory, type **docker compose -p demo build**. The nginx_demo profile expects to find the engine API at localhost, port 3000.

2. then type **docker compose -p demo up --detach**. 

### Typical Engine Setup Steps

1. open the admin bundle jupyter notebook using the token you provided, e.g. http://localhost:8888/?token=easy

2. pilot to the **setup** folder.

3. for a straightforward load of the medium resolution NHDPlus, choose the **recipe_quick_setup_medonly** notebook and execute all cells.  This will download and stage the data and logic required.  Alternatively the **recipe_quick_setup_all** notebook will load the same plus the high resolution NHDPlus.  Note the high resolution datasets will require significant disk and time to stage and load.

After completion of the steps, point your browser to the demo bundle nginx server, probably at http://localhost:8080/cipsrv_indexer.html
The indexer application should load up and allow you to draw a point, line or polygon and submit it for indexing.  Note the high resolution NHDPlus option will only work if the high resolution NHDPlus data and code was loaded.

Alternatively, try the ATTAINS comparison application, probably at http://localhost:8080/cipsrv_attains.html.  This web app is meant to show the results of CIP Service indexing against indexing previous done by ATTAINS.

### Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use. EPA has relinquished control of the information and no longer has responsibility to protect the integrity, confidentiality, or availability of the information. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA. The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
