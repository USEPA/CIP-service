## Streamlined Installation Instructions

CIP-service is designed to be highly flexible to fit a variety of platforms and architectures.  However, that comes at a cost of complexity.  At heart the original vision of the project was a single containerized environment hosting all components.  In this deployment, security concerns are lessened as presumably the container environment is itself secure from outsiders.

### Windows Setup Steps

1. Install [Rancher Desktop](https://rancherdesktop.io/) or [Docker Desktop](https://www.docker.com/products/docker-desktop/).\
   Note the Docker Desktop licensing requirements.  For Rancher Desktop set the container runtime to [dockerd](https://docs.rancherdesktop.io/ui/preferences/container-engine/general).

2. Set your WSL to a distro with Python 3.\
   You can't go wrong with [Ubuntu](https://canonical-ubuntu-wsl.readthedocs-hosted.com/en/latest/guides/install-ubuntu-wsl2/).

3. Configure [Rancher Desktop](https://docs.rancherdesktop.io/ui/preferences/wsl/integrations) or [Docker Desktop](https://docs.docker.com/desktop/wsl/#enabling-docker-support-in-wsl-2-distros) to integrate with your WSL distro.

4. While all CIP-service images are public, dockerhub may argue with your desire for anonymous access.  If presented with authentication problems, log into dockerhub using your credentials via
```
    docker login 
```
5. Add the Python library jinja2 to Python via
```
    pip3 install jinja2
```
6. Checkout the CIP-service project and navigate to the project root.
  
7. Then launch the quickconfig.py script
```
    cd config
    python3 quickconfig.py --recipe=VPU09
```
### Linux Setup Steps

1. Install [Docker Engine](https://docs.docker.com/engine/install/).\
   Note the engine itself is [open source](https://docs.docker.com/engine/).
2. While all CIP-service images are public, dockerhub may argue with your desire for anonymous access.  If presented with authentication problems, log into dockerhub using your credentials via
```
    docker login 
```
3.  Add the Python library jinja2 to your local Python via
```
    pip3 install jinja2
```
4. Checkout the CIP-service project and navigate to the project root.

5. Then launch the quickconfig.py script
```
    cd config
    python3 quickconfig.py --recipe=VPU09
```
### quickconfig script Options
docker 
* **--recipe** set to \
    **ALL** - load both the high and medium resolution NHDPlus datasets. \
    **MRONLY** - load only the medium resolution NHDPlus dataset. \
    **HRONLY** - load only the high resolution NHDPlus dataset. \
    **VPU09** - load a small subset of the high and medium resolution NHDPlus datasets limited to VPU 09 (Souris-Red-Rainy) only.

* **--mr_dumpfile_copyin** set to an external dump file to copy into the container replacing the need to download the medium resolution NHDPlus dumpfile.

* **--hr_dumpfile_copyin** set to an external dump file to copy into the container replacing the need to download the high resolution NHDPlus dumpfile.

* **--support_dumpfile_copyin** set to an external dump file to copy into the container replacing the need to download the support dumpfile.

* **--down_volumes** forces the initial docker compose down actions to also purge any existing volumes.  Useful when "clearing the decks" of any preexisting data.

* **--override_postgresql_port** set to nonstandard PostgreSQL port (such as 5433) to avoid conflicts with other preexisting databases on the container host.
