# ![USEPA](https://avatars.githubusercontent.com/u/1304320?s=70) CIP (Catchment Index Processing) Service

### Overview

CIP-service is a project of the [US Environmental Protection Agency](https://www.epa.gov) [Office of Water](https://www.epa.gov/aboutepa/about-office-water) providing containers, logic and [data](docs/data.md) for the task of associating or _indexing_ hydrologic features with [NHDPlus](https://www.epa.gov/waterdata/nhdplus-national-hydrography-dataset-plus) features at multiple resolutions.  CIP-service supports a variety of purposes indexing to catchments, reaches or navigating the NHDPlus network for discovery or flow analysis.  The majority of logic occurs within a containerized [PostgreSQL](https://www.postgresql.org/) database with additional containers providing support products such as an [API](https://docs.postgrest.org/en/v12/), [Jupyter Notebooks](https://jupyter.org/) and sample demo applications.  All components of the provided container stack are open source. 

### Requirements

* Docker compose compatible containerization environment such [Docker engine](https://docs.docker.com/engine/), [Docker Desktop](https://www.docker.com/products/docker-desktop/), [Rancher Desktop](https://rancherdesktop.io/) or the ability to translate compose projects into other environments such as Kubernetes.  Each CIP-service container is portable and can be built and deployed individually into Kubernetes-like environments with minor effort.  However, the prepackaged deployment logic in the project uses compose commands.  If using Windows, the [Rancher Desktop](https://rancherdesktop.io/) environment is an open source alternative to Docker Desktop.

* Knowledge of containers, [Python coding](https://www.python.org/), GIS, PostgreSQL and the [PLpgSQL stored procedure language](https://www.postgresql.org/docs/current/plpgsql.html) is recommended.

* Usage of the quickconfig.py setup utility requires Python 3 to be available on the host system.  Most flavors of Linux will provide Python by default.  Python can be added to Windows by hand, or you may already have it as part of ArcGIS Pro, QGIS or other software installations.  Using [Windows Subsystem for Linux](https://learn.microsoft.com/en-us/windows/wsl/about) is also a convenient way to have Python at the ready.

### Install

The quickconfig.py utility is meant to provide a quick and easy way to spin up a complete working environment.  If seeking to evaluate whether CIP-service is useful for your needs the *VPU09* recipe is ideal.  This recipe pulls in a very small subset of NHDPlus data confined to the Red River/Rainy watershed in the Upper Midwest.  Total disk space requirements are about 7 GB.

    python config/quickconfig.py --recipe=VPU09

Other recipe values:

* **ALL** Loads both medium and high resolution national datasets supporting catchment indexing and network navigation.  Requires about 250 GB of disk.
* **MEDONLY** Loads only the medium resolution national dataset supporting catchment indexing and network navigation.  Requires about 25 GB of disk.
* **EXTENDED** Load all datasets of both medium and high resolution supporting extended capabilities including watershed delineation.  Requires about 510 GB.

Each recipe downloads and loads one or more PostgreSQL [dump files](docs/data.md) from an [EPA Simple Storage System](https://dmap-data-commons-ow.s3.amazonaws.com/index.html#data/cipsrv/).  Some of these files are rather large and downloading them more than once is unideal.  If you have the downloads prepositioned in a location on the host, use the copyin parameters to skip the downloading.

Please review additional notes on [security](docs/security.md).

### Getting Help

* Open a GitHub issue.
* Email WATERS Support at WATERS_SUPPORT@epa.gov.

### Components

The current project is divided into "bundles" providing functionality through several combinations of containers and standalone resources.

1. **Engine**: The engine bundle provides the core functionality of CIP.  At present there are two components, the database and the middleware API.
2. **Admin**: The admin bundle provides a set of Jupyter notebooks used in the deployment of CIP data and code to the engine.  The admin notebooks also provide a simple interface for batch processing.
3. **Demo**: The demo bundle provides a simple Nginx web server that hosts simple examples of CIP indexing.
4. **GIS**: The GIS bundle provides a [Geoserver](https://geoserver.org/) for visualizing indexing and navigation results in cartographic form.

### Capabilities

To explore the CIP-service API capabilities, see the [OpenAPI documentation](docs/openapi.yml).  Various OpenAPI 3.1 online renderers may be used to see the API rendered as document. For example, see the [Swagger Next Editor](https://editor-next.swagger.io/?spec=https://raw.githubusercontent.com/USEPA/CIP-service/main/docs/openapi.yml). Such viewers may not allow direct linking in which case the [direct link](https://raw.githubusercontent.com/USEPA/CIP-service/main/docs/openapi.yml) can be used to work around such issues.

### Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use. EPA has relinquished control of the information and no longer has responsibility to protect the integrity, confidentiality, or availability of the information. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA. The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
