
$env:CURRENT_UID='1000:1000' 
$env:COMPOSE_PROJECT_NAME="cip20-config-compose"
$env:COMPOSE_PROFILE=$args[0]
$env:PROJDIR=$args[1]

& 'docker-compose' '-f' './config-compose.yml' 'up'
