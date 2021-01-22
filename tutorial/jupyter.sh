#!/usr/bin/env bash

JUPYTER_CONTAINERS_DIR="$(pwd)/$(dirname "${BASH_SOURCE[0]}")"

function start_slurm() {
    cd "$JUPYTER_CONTAINERS_DIR/docker_config/slurm"
      ./start-slurm.sh
    cd -

    docker exec slurmctld /bin/bash -c "conda install -c conda-forge jupyterlab"
    docker exec slurmctld /bin/bash -c "conda install -c conda-forge distributed"
    docker exec slurmctld /bin/bash -c "conda install -c conda-forge nodejs"
    docker exec slurmctld /bin/bash -c "pip install dask-labextension"
    docker exec slurmctld /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec c1 /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec c2 /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec slurmctld /bin/bash -c "adduser slurmuser; chown -R slurmuser /jobqueue_features;"
    docker exec c1 /bin/bash -c "adduser slurmuser; chown -R slurmuser /jobqueue_features;"
    docker exec c2 /bin/bash -c "adduser slurmuser; chown -R slurmuser /jobqueue_features;"
    docker exec slurmctld /bin/bash -c "yes|sacctmgr create account slurmuser; yes | sacctmgr create user name=slurmuser Account=slurmuser"
    docker exec slurmctld /bin/bash -c "mkdir -p /home/slurmuser/.config/dask/"
    cd "$JUPYTER_CONTAINERS_DIR/docker_config/slurm"
      docker cp labextension.yaml slurmctld:/home/slurmuser/.config/dask/labextension.yaml
    cd -
    docker exec slurmctld /bin/bash -c "chown -R slurmuser /home/slurmuser/"
    docker exec -u slurmuser slurmctld /bin/bash -c "jupyter lab workspace import /jobqueue_features/tutorial/docker_config/slurm/workspace.json"
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /jobqueue_features/tutorial/docker_config/slurm/tutorial_tasks; jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.notebook_dir='/jobqueue_features/tutorial/docker_config/slurm/tutorial_tasks'&"

    echo
    echo -e "\e[32mSLURM properly configured\e[0m"
    echo
    echo -e "\tOpen your browser at http://localhost:8888/lab/workspaces/lab"
    echo
}

function test_slurm() {
    docker cp $JUPYTER_CONTAINERS_DIR/../jobqueue_features/tests/. slurmctld:/jobqueue_features/jobqueue_features/tests
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /jobqueue_features; pytest /jobqueue_features --verbose -E slurm -s --ignore=/jobqueue_features/jobqueue_features/tests/test_cluster.py"
}

function stop_slurm() {
    for machin in c1 c2 slurmctld slurmdbd mysql
    do
      docker stop $machin
      docker rm $machin
    done
}

function clean_slurm() {
    for machin in slurm_c1:latest slurm_c2:latest slurm_slurmctld:latest slurm_slurmdbd:latest mysql:5.7.29 daskdev/dask-jobqueue:slurm
    do
      docker rmi $machin
    done

    docker network rm slurm_default

    for vol in slurm_etc_munge slurm_etc_slurm slurm_slurm_jobdir slurm_var_lib_mysql slurm_var_log_slurm
    do
      docker volume rm $vol
    done

}
