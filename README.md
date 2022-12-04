# CIP (Catchment Index Processing) Service

Note the CIP Service project entails several functionality layers of which only the first is presented here.  For the moment those layers might be explained as:

1. **Engine** to do one-by-one event indexing and proof/document the process.
2. **Batch** to process entire GIS files through event indexing returning results as file downloads.
3. **QA/QC** to allow persistance of batched processed datasets including authenticated review through visual inspection and alteration of batched results.

### Overview

At this time hosting options for deploying the CIP-service project are quite varied ranging from Windows desktops to Linux servers to cloud hosting.  As such a single docker-compose environment is challenging to present.  
To accommodate this the project uses a jinja2 template combined with a specific profile configuration to dynamically generate the needed compose and dockerfile artifacts.  

Generally, the primary two forms would be local desktop and cloud-hosted options

![Local Desktop](docs/architecture_local.drawio.png)


![Cloud-Hosted](docs/architecture_cloud.drawio.png)

### Prerequisites

- docker
- docker-compose (usually bundled nowadays with docker)
- at least 16 GB of memory
- at least 80 GB of free disk
- access to a public or deploy-key secured CIP-service code repository

### Containers Utilized

The project uses the following containers which are standard, stable, trusted dockerhub images.  If you have any reason to distrust or find problematic any of the following, please open a ticket.

- postgis/postgist:14
- postgrest/postgrest:v10
- osgeo/gdal:ubuntu
- nginx

### Configuration

- inspect the provided profiles or generate a new profile meeting your host specifications.  Generally for testing purposes the desktop.yml profile is expected to work for most users.

- generate the needed compose artifacts against the selected profile:
  - windows: ./config-compose.ps1 desktop.yml
  - linux:   ./config-compose.sh  desktop.yml
  - python:  ./config-compose.py --profile desktop.yml
  
  this step will generate the needed docker-compose.yml and Dockerfiles in the root of the project.  Note running the Python configuration code directly on your host is the quickest and simplest of the above methods but requires Python 3 with the jinja2 package installed.

- next create an .env file in the root of project that contains needed security values.  See the sample .env file [here](../profiles/env.example) for values to provide.

### Typical Deployment Steps

1. in the root of the project, type **docker-compose build**.  This will download and configure the containers.  It may take a while.

2. next type **docker-compose up --detach**.  This will spin up the containers applying the final alterations.  Note the cip_pr container will spin up fast and throw errors while it waits for the cip_pg database to come up.  These errors can be ignored.

3. next type **docker ps** to make sure all four containers are up and running as expected.  You should see cip_pg, cip_pr, cip_jp and cip_ng containers running.

4. next open the jupyter notebook using the cip_jp external port and the token you provided, e.g. http://localhost:8888/?token=easy

5. pilot to the **utilities** folder.

6. for a straightforward load of the medium resolution NHDPlus, choose the **recipe_quick_setup_medonly** notebook and execute all cells.  This will download and stage the data and logic required.  Alternatively the **recipe_quick_setup_all** notebook will load the same plus the high resolution NHDPlus.  Note the high resolution datasets will require significant disk anmd time to stage and load.

After completion of the steps, point your browser to the nginx server, probably at http://localhost:8080/cipsrv_indexer.html
The indexer application should load up and allow you to draw a point, line or polygon and submit it for indexing.  That is all the project does at the moment.  Note the high resolution NHDPlus option will only work if the high resolution NHDPlus data and code was loaded.

Alternatively, try the ATTAINS comparison application, probably at http://localhost:8080/cipsrv_attains.html.  This web app is meant to show the results of CIP Service indexing against indexing previous done by ATTAINS.

### Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use. EPA has relinquished control of the information and no longer has responsibility to protect the integrity, confidentiality, or availability of the information. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA. The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
