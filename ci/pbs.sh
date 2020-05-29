#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # start pbs cluster
    cd ./ci/pbs
    ./start-pbs.sh
    cd -

    docker exec -u pbsuser pbs-master pbsnodes -a
    docker ps -a
    docker images
}

function jobqueue_install {
    docker exec pbs-master /bin/bash -c "cd /jobqueue_features; mkdir -p dask-worker-space; chmod 777 dask-worker-space; mkdir -p .pytest_cache; chmod 777 .pytest_cache"
    docker exec pbs-master /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec pbs-slave-1 /bin/bash -c "cd /jobqueue_features; mkdir -p dask-worker-space; chmod 777 dask-worker-space; mkdir -p .pytest_cache; chmod 777 .pytest_cache"
    docker exec pbs-slave-1 /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec pbs-slave-2 /bin/bash -c "cd /jobqueue_features; mkdir -p dask-worker-space; chmod 777 dask-worker-space; mkdir -p .pytest_cache; chmod 777 .pytest_cache"
    docker exec pbs-slave-2 /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    # Configure passwordless ssh between the slaves for the pbsuser to work around
    # incomplete MPI configuration:
    # as root, start ssh (both)
    docker exec pbs-slave-1 /bin/bash -c "ssh-keygen -A"
    docker exec pbs-slave-1 /bin/bash -c "/usr/sbin/sshd"
    docker exec pbs-slave-2 /bin/bash -c "ssh-keygen -A"
    docker exec pbs-slave-2 /bin/bash -c "/usr/sbin/sshd"
    # create a place to share the ssh key
    mkdir ssh
    chmod 777 ssh
    # as user on 1
    docker exec -u pbsuser pbs-slave-1 /bin/bash -c "ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa"
    docker exec -u pbsuser pbs-slave-1 /bin/bash -c "cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys"
    docker exec -u pbsuser pbs-slave-1 /bin/bash -c "chmod go-rw ~/.ssh/authorized_keys"
    docker exec -u pbsuser pbs-slave-1 /bin/bash -c "cp -r ~/.ssh/* /jobqueue_features/ssh"
    docker exec -u pbsuser pbs-slave-1 /bin/bash -c "ssh-keyscan pbs-slave-1.pbs_default >> ~/.ssh/known_hosts"
    docker exec -u pbsuser pbs-slave-1 /bin/bash -c "ssh-keyscan pbs-slave-2.pbs_default >> ~/.ssh/known_hosts"
    # as user on 2
    docker exec -u pbsuser pbs-slave-2 /bin/bash -c "mkdir -p ~/.ssh && chmod 700 ~/.ssh"
    docker exec -u pbsuser pbs-slave-2 /bin/bash -c "cp /jobqueue_features/ssh/* ~/.ssh"
    docker exec -u pbsuser pbs-slave-2 /bin/bash -c "ssh-keyscan pbs-slave-1.pbs_default >> ~/.ssh/known_hosts"
    docker exec -u pbsuser pbs-slave-2 /bin/bash -c "ssh-keyscan pbs-slave-2.pbs_default >> ~/.ssh/known_hosts"
    # fiddle with the PATH on the slaves so they find the conda env in an MPI job
    docker exec -u pbsuser pbs-slave-1 /bin/bash -c "echo 'export PATH=/opt/anaconda/bin:$PATH' >> ~/.bashrc"
    docker exec -u pbsuser pbs-slave-2 /bin/bash -c "echo 'export PATH=/opt/anaconda/bin:$PATH' >> ~/.bashrc"
    # remove ssh dir we created
    rm -rf ssh
}

function jobqueue_script {

    docker exec -u pbsuser pbs-master /bin/bash -c "pip list; cd /jobqueue_features; OMPI_MCA_rmaps_base_oversubscribe=1 OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 pytest /jobqueue_features --verbose -E pbs -s"
}

function jobqueue_after_script {
    docker exec -u pbsuser pbs-master qstat -fx
    docker exec pbs-master bash -c 'cat /var/spool/pbs/sched_logs/* || true'
    docker exec pbs-master bash -c 'cat /var/spool/pbs/server_logs/* || true'
    docker exec pbs-master bash -c 'cat /var/spool/pbs/server_priv/accounting/* || true'
    docker exec pbs-slave-1 bash -c 'cat /var/spool/pbs/mom_logs/* || true'
    docker exec pbs-slave-1 bash -c 'cat /var/spool/pbs/spool/* || true'
    docker exec pbs-slave-1 bash -c 'cat /tmp/*.e* || true'
    docker exec pbs-slave-1 bash -c 'cat /tmp/*.o* || true'
    docker exec pbs-slave-2 bash -c 'cat /var/spool/pbs/mom_logs/* || true'
    docker exec pbs-slave-2 bash -c 'cat /var/spool/pbs/spool/* || true'
    docker exec pbs-slave-2 bash -c 'cat /tmp/*.e* || true'
    docker exec pbs-slave-2 bash -c 'cat /tmp/*.o* || true'
}
