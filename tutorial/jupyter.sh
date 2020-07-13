#!/usr/bin/env bash

function start_slurm() {
    cd ./tutorial/docker_config/slurm
    ./start_slurm.sh
    cd -

    docker exec slurmctld /bin/bash -c "conda install -c conda-forge jupyterlab"
    docker exec slurmctld /bin/bash -c "conda install -c conda-forge notebook"
    docker exec slurmctld /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec c1 /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec c2 /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec slurmctld /bin/bash -c "jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.notebook_dir='/jobqueue_features'&"

    echo "SLURM properly configured"
}

function start_pbs() {
    cd ./tutorial/docker_config/pbs
      ./start_pbs.sh
    cd -

    docker exec pbs-master /bin/bash -c "cd /jobqueue_features; mkdir -p dask-worker-space; chmod 777 dask-worker-space; mkdir -p .pytest_cache; chmod 777 .pytest_cache"
    docker exec pbs-master /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec pbs-slave-1 /bin/bash -c "cd /jobqueue_features; mkdir -p dask-worker-space; chmod 777 dask-worker-space; mkdir -p .pytest_cache; chmod 777 .pytest_cache"
    docker exec pbs-slave-1 /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."
    docker exec pbs-slave-2 /bin/bash -c "cd /jobqueue_features; mkdir -p dask-worker-space; chmod 777 dask-worker-space; mkdir -p .pytest_cache; chmod 777 .pytest_cache"
    docker exec pbs-slave-2 /bin/bash -c "cd /jobqueue_features; pip install -r requirements.txt; pip install --no-deps -e ."

    docker exec pbs-slave-1 /bin/bash -c "ssh-keygen -A"
    docker exec pbs-slave-1 /bin/bash -c "/usr/sbin/sshd"
    docker exec pbs-slave-2 /bin/bash -c "ssh-keygen -A"
    docker exec pbs-slave-2 /bin/bash -c "/usr/sbin/sshd"

    # as user on 1
    docker exec -u 0 pbs-slave-1 /bin/bash -c "ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa"
    docker exec -u 0 pbs-slave-1 /bin/bash -c "cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys"
    docker exec -u 0 pbs-slave-1 /bin/bash -c "chmod go-rw ~/.ssh/authorized_keys"
    docker exec -u 0 pbs-slave-1 /bin/bash -c "ssh-keyscan pbs-slave-1.pbs_default >> ~/.ssh/known_hosts"
    # as user on 2
    docker exec -u 0 pbs-slave-2 /bin/bash -c "ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa"
    docker exec -u 0 pbs-slave-2 /bin/bash -c "cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys"
    docker exec -u 0 pbs-slave-2 /bin/bash -c "chmod go-rw ~/.ssh/authorized_keys"
    docker exec -u 0 pbs-slave-2 /bin/bash -c "ssh-keyscan pbs-slave-2.pbs_default >> ~/.ssh/known_hosts"

    # fiddle with the PATH on the slaves so they find the conda env in an MPI job
    docker exec -u 0 pbs-slave-1 /bin/bash -c "echo 'export PATH=/opt/anaconda/bin:$PATH' >> ~/.bashrc"
    docker exec -u 0 pbs-slave-2 /bin/bash -c "echo 'export PATH=/opt/anaconda/bin:$PATH' >> ~/.bashrc"

    docker exec pbs-master /bin/bash -c "qmgr -c 'set server flatuid=true'"
    docker exec pbs-master /bin/bash -c "qmgr -c 'set server acl_roots+=root@*'"
    docker exec pbs-master /bin/bash -c "qmgr -c 'set server operators+=root@*'"

    docker exec pbs-master /bin/bash -c "conda install -c conda-forge jupyterlab"
    docker exec pbs-master /bin/bash -c "conda install -c conda-forge notebook"
    docker exec -u 0 pbs-master /bin/bash -c "jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.notebook_dir='/jobqueue_features'&"

    echo "PBS properly configured"
}

function erase_slurm() {
    for machin in c1 c2 slurmctld slurmdbd mysql
    do
      docker stop $machin
      docker rm $machin
    done
}

function erase_pbs() {
    for machin in pbs-master pbs-slave-1 pbs-slave-2
    do
      docker stop $machin
      docker rm $machin
    done
}