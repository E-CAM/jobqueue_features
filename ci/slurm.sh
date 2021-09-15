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
    docker exec slurmctld /bin/bash -c "cd /jobqueue_features; pip install --upgrade -r requirements.txt; pip install --no-deps -e ."
    docker exec c1 /bin/bash -c "cd /jobqueue_features; pip install --upgrade -r requirements.txt; pip install --no-deps -e ."
    docker exec c2 /bin/bash -c "cd /jobqueue_features; pip install --upgrade -r requirements.txt; pip install --no-deps -e ."
}

function jobqueue_script {
    docker exec slurmctld /bin/bash -c "pip list; cd /jobqueue_features; pytest /jobqueue_features --verbose -E slurm -s"
}

function jobqueue_after_script {
    docker exec slurmctld bash -c 'sinfo'
    docker exec slurmctld bash -c 'squeue'
    docker exec slurmctld bash -c 'sacct -l'
}
