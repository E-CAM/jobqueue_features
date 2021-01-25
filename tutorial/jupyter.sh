#!/usr/bin/env bash

JUPYTER_CONTAINERS_DIR="$(pwd)/$(dirname "${BASH_SOURCE[0]}")"

function start_slurm() {
    cd "$JUPYTER_CONTAINERS_DIR/docker_config/slurm"
      ./start-slurm.sh
    cd -

    # Install JupyterLab and the Dask extension
    docker exec slurmctld /bin/bash -c "conda install -c conda-forge jupyterlab distributed nodejs dask-labextension"
    # Add a slurmuser so we don't run as root
    docker exec slurmctld /bin/bash -c "adduser slurmuser; chown -R slurmuser /jobqueue_features;"
    docker exec c1 /bin/bash -c "adduser slurmuser; chown -R slurmuser /jobqueue_features;"
    docker exec c2 /bin/bash -c "adduser slurmuser; chown -R slurmuser /jobqueue_features;"
    docker exec slurmctld /bin/bash -c "chown -R slurmuser /home/slurmuser/"
    docker exec slurmctld /bin/bash -c "chown -R slurmuser /data"
    docker exec slurmctld /bin/bash -c "yes|sacctmgr create account slurmuser; yes | sacctmgr create user name=slurmuser Account=slurmuser"
    # Install devel version of jobqueue_features (which is already cloned into the image)
    docker exec slurmctld /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec c1 /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec c2 /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    # Add the default cluster configuration for Dask Lab Extension plugin
    docker exec slurmctld /bin/bash -c "mkdir -p /home/slurmuser/.config/dask/"
    cd "$JUPYTER_CONTAINERS_DIR/docker_config/slurm"
      docker cp labextension.yaml slurmctld:/home/slurmuser/.config/dask/labextension.yaml
    cd -
    # Clone the tutorials, import the workspace and start the JupyterLab
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /data; git clone https://github.com/E-CAM/jobqueue_features_workshop_materials.git"
    docker exec -u slurmuser slurmctld /bin/bash -c "jupyter lab workspace import /data/jobqueue_features_workshop_materials/workspace.json"
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /data/jobqueue_features_workshop_materials; jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.notebook_dir='/data/jobqueue_features_workshop_materials'&"

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
