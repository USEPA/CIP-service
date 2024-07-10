## Streamlined Installation Instructions

CIP-service is designed to be highly flexible to fit a variety of platforms and architectures.  However, that comes at a cost of complexity.  At heart the original vision of the project was a single docker environment hosting all components.  In this deployment, security concerns are lessened as presumably the docker environment is itself secure from outsiders.

### Setup Steps
```
> cd config
> python config-compose.py --bundle engine --bprofile default
> python config-compose.py --bundle admin  --bprofile default
> python config-compose.py --bundle demo   --bprofile default
```
At this point one should inspect the .env files in the engine, admin and demo directories to make sure values are appropriate.
```
> cd ../engine
> docker compose build
> docker compose up -d

> cd ../admin
> docker compose build
> docker compose up -d

> cd ../demo
> docker compose build
> docker compose up -d
```  
