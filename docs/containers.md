# Containers

The current project is divided into "bundles" each representing a docker compose configuration providing functionality through several containerized servers.

1. **Engine**: The engine bundle provides the core functionality of CIP.  At present there are two components, the PostgreSQL database and the PostgREST middleware API.
2. **Admin**: The admin bundle provides a set of Jupyter notebooks used in the deployment of CIP data and code to the engine.  The admin notebooks also provide a simple interface for batch processing.
3. **Demo**: The demo bundle provides a simple Nginx web server that hosts simple examples of CIP indexing, navigation and watershed delneation.
4. **GIS**: The GIS bundle provides a [Geoserver](https://geoserver.org/) for visualizing indexing and navigation results in cartographic form.

![local](docs/architecture_local.drawio.png)

The bundle architecture was chosen to allow users to easily remove or substitute components without needing major changes to the overall system.  For example the heart of the system is the PostgreSQL database and one might wish to utilize a remote cloud solution such as Amazon RDS or Microsoft Azure for this component.  In such a scenario the entire engine bundle might be substituted with cloud resources.

![cloud](docs/architecture_cloud.drawio.png)

While efforts have been made to allow these kinds of flexible change, improvements or suggestions for different approaches is always welcome via a project issue.
