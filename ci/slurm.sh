#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # start slurm cluster
    cd ./ci/slurm
    ./start-slurm.sh
    cd -

    docker ps -a
    docker images
}

function jobqueue_install {
    docker exec -it slurmctld /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
}

function jobqueue_script {
    docker exec -it slurmctld /bin/bash -c "pip list; OMPI_MCA_rmaps_base_oversubscribe=1 OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 pytest /jobqueue_features/jobqueue_features --verbose -E slurm -s"
}

function jobqueue_after_script {
    docker exec -it slurmctld bash -c 'sinfo'
    docker exec -it slurmctld bash -c 'squeue'
    docker exec -it slurmctld bash -c 'sacct -l'
}
