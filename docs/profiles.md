## Current Project Profiles:

- [desktop](#desktop) - [yml](../profiles/desktop.yml)
- [server](#server) - [yml](../profiles/server.yml)
- [remote_db](#remote-database) - [yml](../profiles/remote_db.yml)
- [awsrds](#aws-rds) - [yml](../profiles/awsrds.yml)

- [common settings](#common-settings) - [yml](../profiles/_common.yml)

### Desktop

The desktop profile is intended for smallish Windows hosts seeking to run the entire project locally via a container runtime without any external resources.  
Performance and time to receive results may vary depending on the size of the host machine.  
However, such a system should suffice to run the project against the medium resolution NHDPlus dataset.  

### Server

The server profile is intended for an enterprise-sized Linux host seeking to run the entire project locally via a container runtime without any external resources.  
In particular this profile is a good starting point for deploying and testing the high resolution NHDPlus dataset.

### Remote Database

The remote_db profile is intended for a development environment whereby the PostgreSQL database component is provided by a free-standing database on a local network or even on 
the same host as the container runtime.  

### AWS RDS

The awsrds profile is intended for an EPA development environment whereby the PostgreSQL database component is provided by the EPA Amazon cloud.  In particular this  
profile respects the details of deploying and utilizing an Amazon RDS managed service.

### Common Settings

The \_common.yml file provides the means to define settings common to all profiles.  The setting are merged such that specific profile values will override common values.

### Further Notes:

All profiles are similar and primarily differ in the resources allocated and how those resources are wired together.  
