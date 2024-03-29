#!/usr/bin/env bash

JUPYTER_CONTAINERS_DIR="$(pwd)/$(dirname "${BASH_SOURCE[0]}")"

function check_command() {
    if ! command -v $1 &> /dev/null
    then
        echo "$1 could not be found, please install this before continuing!"
        return 1
    fi
    return 0
}

function start_slurm() {
    check_command docker-compose
    have_docker_compose=$?
    check_command docker
    have_docker=$?
    if [ "${have_docker_compose}" != 0 ] || [ "${have_docker}" != "0" ]; then
        echo "One of docker or docker-compose is not available, exiting!" 
        return 1
    fi
    export REQUIREMENTS=$(cat $JUPYTER_CONTAINERS_DIR/../requirements.txt | grep -v jobqueue)
    cd "$JUPYTER_CONTAINERS_DIR/docker_config/slurm"
      ./start-slurm.sh
    cd -

    # Install JupyterLab and the Dask extension
    docker exec slurmctld /bin/bash -c "mamba install --yes -c conda-forge jupyterlab distributed nodejs dask-labextension"
    # Remove the LAMMPS python package from the login node (so environment is different to compute nodes)
    docker exec slurmctld /bin/bash -c "rm -r /opt/anaconda/lib/python*/site-packages/lammps"
    # Add a slurmuser so we don't run as root
    docker exec slurmctld /bin/bash -c "adduser slurmuser; chown -R slurmuser /jobqueue_features;"
    docker exec c1 /bin/bash -c "adduser slurmuser; chown -R slurmuser /jobqueue_features;"
    docker exec c2 /bin/bash -c "adduser slurmuser; chown -R slurmuser /jobqueue_features;"
    docker exec slurmctld /bin/bash -c "chown -R slurmuser /home/slurmuser/"
    docker exec slurmctld /bin/bash -c "chown -R slurmuser /data"
    docker exec slurmctld /bin/bash -c "yes|sacctmgr create account slurmuser; yes | sacctmgr create user name=slurmuser Account=slurmuser"
    # Add the default cluster configuration for Dask Lab Extension plugin
    docker exec slurmctld /bin/bash -c "mkdir -p /home/slurmuser/.config/dask/"
    cd "$JUPYTER_CONTAINERS_DIR/docker_config/slurm"
      docker cp labextension.yaml slurmctld:/home/slurmuser/.config/dask/labextension.yaml
    cd -
    echo
    echo -e "\e[32mSLURM properly configured\e[0m"
    echo

    return 0
}

function _report_links() {
    # Retrieve the host port
    hostport=$(docker port slurmctld 8888 | cut -d ":" -f2)
    daskport=$(docker port slurmctld 8787 | cut -d ":" -f2)
    echo -e "\t\e[32mOpen your browser at http://localhost:$hostport/lab/workspaces/lab\e[0m"
    echo -e "\tDefault Dask dashboard will be available at http://localhost:$daskport"
    echo
}

function launch_tutorial_slurm() {
    TUTORIAL="jobqueue_features_workshop_materials"
    # Clone the tutorials, import the workspace and start the JupyterLab
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /data; git clone https://github.com/E-CAM/${TUTORIAL}.git"
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /data/${TUTORIAL}; git pull"
    docker exec -u slurmuser slurmctld /bin/bash -c "jupyter lab workspace import /data/${TUTORIAL}/workspace.json"
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /data/${TUTORIAL}; jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.notebook_dir='/data/${TUTORIAL}'&"
    _report_links
}


function start_tutorial() {
    start_slurm
    cluster_status=$?
    if [ "${cluster_status}" != 0 ]; then
        return 1
    fi
    launch_tutorial_slurm
    return 0
}


function start_jobqueue_tutorial() {
    start_slurm
    cluster_status=$?
    if [ "${cluster_status}" != 0 ]; then
        return 1
    fi
    TUTORIAL="workshop-Dask-Jobqueue-cecam-2021-02"
    # Clone the tutorials, import the workspace and start the JupyterLab
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /data; git clone https://github.com/E-CAM/${TUTORIAL}.git"
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /data/${TUTORIAL}; git pull"
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /data/${TUTORIAL}/notebooks; jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.notebook_dir='/data/${TUTORIAL}/notebooks'&"
    _report_links
}

function test_slurm() {
    start_slurm
    cluster_status=$?
    if [ "${cluster_status}" != 0 ]; then
        return 1
    fi
    docker cp $JUPYTER_CONTAINERS_DIR/../jobqueue_features/tests/. slurmctld:/jobqueue_features/jobqueue_features/tests
    docker exec -u slurmuser slurmctld /bin/bash -c "cd /jobqueue_features; pytest /jobqueue_features --verbose -E slurm -s --ignore=/jobqueue_features/jobqueue_features/tests/test_cluster.py"
}

function stop_slurm() {
    for machine in c1 c2 slurmctld slurmdbd mysql
    do
      docker stop $machine
      docker rm $machine
    done
}

function clean_tutorials() {
    for vol in slurm_etc_munge slurm_etc_slurm slurm_slurm_jobdir slurm_var_lib_mysql slurm_var_log_slurm
    do
      docker volume rm $vol
    done
}

function clean_slurm() {
    clean_tutorials

    for machine in slurm_c1:latest slurm_c2:latest slurm_slurmctld:latest slurm_slurmdbd:latest mysql:5.7.29 daskdev/dask-jobqueue:slurm
    do
      docker rmi $machine
    done

    docker network rm slurm_default

}
