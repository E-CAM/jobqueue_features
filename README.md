[![Build Status](https://travis-ci.com/E-CAM/jobqueue_features.svg?branch=master)](https://travis-ci.com/E-CAM/jobqueue_features)
# jobqueue_features
This library provides some useful decorators for [`dask_jobqueue`](https://github.com/dask/dask-jobqueue). It also expands it's scope to
include MPI workloads, including extending configuration options for such workloads and heterogeneous resources.

## Tutorial
To help people try out this library, we have created sets of Docker containers that allow you to test the usage from within a notebook on
(toy) SLURM or PBS clusters. The dockers containers can be found in the [`tutorial`](https://github.com/E-CAM/jobqueue_features/tree/master/tutorial)
folder. Either option will start a couple of docker containers with chosen scheduler and a JupyterLab instance linked to the head node. You should be able to
access the JupyterLab instance from your browser on `localhost:8888`. Feel free to try, learn and explore using the example notebooks you find there.

Requirements:
* [Docker](https://docs.docker.com/get-docker/)
* [docker-compose](https://docs.docker.com/compose/install/)
* [Manage `docker` as a non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user)

### Basic usage  

SLURM:
```
# configure our commands to start/stop/clean our containers
source tutorial/jupyter.sh

# start containers
start_slurm

# you should now be able to open your browser at localhost:8888 to access JupyterLab

# stop containers (also erases the containers, but does not remove the docker images)
stop_slurm

# when you a finished, remove docker images related to containers to free space
clean_slurm
```
PBS:
```
# configure our commands to start/stop/clean our containers
source tutorial/jupyter.sh

# start containers
start_pbs

# you should now be able to open your browser at localhost:8888 to access JupyterLab

# stop containers (also erases the containers, but does not remove the docker images)
stop_pbs

# when you a finished, remove docker images related to containers to free space
clean_pbs
```

If your configuration does not allow you to start `docker`/`docker-compose` without sudo, you can work around this: you would use
```
sudo bash -c "$(declare -f start_slurm); start_slurm"
```
instead of simply `start_slurm` (with the same approach for `stop_slurm` and `clean_slurm`).


**IMPORTANT:** 
- Please be aware that each docker image uses quite a lot of disk space, you should have at least 3GB available for SLURM and at least 2GB
  available for PBS.  
- Containers mentioned above are designed only for your local machine and tutorial usage: they are not intended to be used for a heavy
  workload.
- Please start one set of scheduler containers at a time, they are not intended to work next to each other.  
