# Aliases para comandos dbt via Docker
# Adicione ao seu ~/.bashrc ou ~/.zshrc:
# source /home/decode/workspace/Inteligencia-Energetica/docker/dbt/aliases.sh

DBT_DOCKER_DIR="/home/decode/workspace/Inteligencia-Energetica/docker/dbt"

alias dbt_docker="$DBT_DOCKER_DIR/dbt_docker.sh"
alias dbt_run="$DBT_DOCKER_DIR/dbt_docker.sh run"
alias dbt_test="$DBT_DOCKER_DIR/dbt_docker.sh test"
alias dbt_build="$DBT_DOCKER_DIR/dbt_docker.sh build"
alias dbt_debug="$DBT_DOCKER_DIR/dbt_docker.sh debug"
alias dbt_deps="$DBT_DOCKER_DIR/dbt_docker.sh deps"
alias dbt_compile="$DBT_DOCKER_DIR/dbt_docker.sh compile"
alias dbt_seed="$DBT_DOCKER_DIR/dbt_docker.sh seed"
alias dbt_docs="$DBT_DOCKER_DIR/dbt_docker.sh docs"
alias dbt_clean="$DBT_DOCKER_DIR/dbt_docker.sh clean"
alias dbt_shell="$DBT_DOCKER_DIR/dbt_docker.sh shell"

echo "✅ Aliases dbt carregados!"
echo "Comandos disponíveis: dbt_run, dbt_test, dbt_build, dbt_debug, dbt_deps, dbt_compile, dbt_seed, dbt_docs, dbt_clean, dbt_shell"
