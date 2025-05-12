# Security

CIP-service deploys and ties together several containers to support its use cases.  The servers in these containers authenticate to each other via credentials stored in .env files under each bundle directory.  By default these passwords are set to cip123.  When used in an isolated local desktop or local server environment this may suffice.  But in situations where others may have access to the containers, it is recommended to fortify the passwords.

Before running the quickconfig.py script, do the following.

1. For each bundle directory: engine, admin, demo and gis.
2. Copy the provided [env.example](../engine/env.example) files in the same locations as .env.  These [files](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/#env-file-syntax) are read by docker compose when the containers are created.
3. In the new file, update the password entries from cip123 to something stronger.
4. Redeploy CIP-service using the quickconfig.py script.

CIP-service was created with the general idea that the container host would be isolated or tightly controlled.  Deploying into a shared or public container environment would involve far more custom configuration outside the scope of a simple compose setup script.  If you have suggestions for improving the overall security of the project please open a project issue.
