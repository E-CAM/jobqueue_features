#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version
    # start sge cluster
    cd ./ci/sge
    ./start-sge.sh
    cd -

    docker ps -a
    docker images
}

function jobqueue_install {
    docker exec sge-master /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec slave-one /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec slave-two /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
}

function jobqueue_script {
    docker exec sge-master /bin/bash -c "pip list; cd /jobqueue_features; OMPI_MCA_rmaps_base_oversubscribe=1 OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 pytest /jobqueue_features --verbose -E sge -s"
}

function jobqueue_after_script {
    docker exec sge-master bash -c 'cat /tmp/sge*'
    docker exec slave-one bash -c 'cat /tmp/exec*'
    docker exec slave-two bash -c 'cat /tmp/exec*'
}
