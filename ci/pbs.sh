#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # start pbs cluster
    cd ./ci/pbs
    ./start-pbs.sh
    cd -

    docker exec -it -u pbsuser pbs_master pbsnodes -a
    docker ps -a
    docker images
}

function jobqueue_install {
    docker exec -it slurmctld /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
}

function jobqueue_script {
    docker exec -it slurmctld /bin/bash -c "pip list; cd /jobqueue_features; OMPI_MCA_rmaps_base_oversubscribe=1 OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 pytest /jobqueue_features --verbose -E pbs -s"
}

function jobqueue_after_script {
    docker exec -it -u pbsuser pbs_master qstat -fx
    docker exec -it pbs_master bash -c 'cat /var/spool/pbs/sched_logs/*'
    docker exec -it pbs_master bash -c 'cat /var/spool/pbs/server_logs/*'
    docker exec -it pbs_master bash -c 'cat /var/spool/pbs/server_priv/accounting/*'
    docker exec -it pbs_slave_1 bash -c 'cat /var/spool/pbs/mom_logs/*'
    docker exec -it pbs_slave_1 bash -c 'cat /var/spool/pbs/spool/*'
    docker exec -it pbs_slave_1 bash -c 'cat /tmp/*.e*'
    docker exec -it pbs_slave_1 bash -c 'cat /tmp/*.o*'
    docker exec -it pbs_slave_2 bash -c 'cat /var/spool/pbs/mom_logs/*'
    docker exec -it pbs_slave_2 bash -c 'cat /var/spool/pbs/spool/*'
    docker exec -it pbs_slave_2 bash -c 'cat /tmp/*.e*'
    docker exec -it pbs_slave_2 bash -c 'cat /tmp/*.o*'
}
