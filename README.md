[![Build Status](https://github.com/E-CAM/jobqueue_features/workflows/CI/badge.svg)](https://github.com/E-CAM/jobqueue_features/actions?query=workflow%3ACI)
# jobqueue_features
This library provides some useful decorators for
[`dask_jobqueue`](https://github.com/dask/dask-jobqueue). It also expands its scope to
include MPI workloads, including extending configuration options for such workloads and
heterogeneous resources.

The documentation for the library is a WIP but we have a
[tutorial repository](https://github.com/E-CAM/jobqueue_features_workshop_materials)
which goes into quite some detail about the scope and capabilities of the package.

## Tutorial Configuration

To help people try out this library, we have created a set of Docker containers that
allow you to test the usage from within a notebook on a
(toy) SLURM cluster.

The commands below will start a set of docker containers consisting of a login node and
two compute nodes. The SLURM resource manager
and a JupyterLab instance linked to the head node. Feel free to try,
learn and explore using the tutorial notebooks you find there.

Requirements:
* **The setup requires ~5GB of diskspace and will download ~1GB of data over the
  internet.**
* [Docker](https://docs.docker.com/get-docker/)
* [docker-compose](https://docs.docker.com/compose/install/)
* *You may also need to
  [manage `docker` as a non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user)
  depending on the permissions you have on the system where you are executing the
  commands*

The dockers containers can be found in the
[`tutorial`](https://github.com/E-CAM/jobqueue_features/tree/master/tutorial)
subdirectory and leverage the continuous integration infrastructure used by the project.
We maintain a separate
[tutorial repository](https://github.com/E-CAM/jobqueue_features_workshop_materials)
which is home to the notebooks used within the tutorial setup.

### Setup

We have packaged the various commands necessary to set up the infrastructure into a set
of bash functions. In order to use these bash functions you need to `source` the script
that defines them. This requires you to first clone the repository:

```
# Clone the repository
git clone https://github.com/E-CAM/jobqueue_features.git

# Enter the directory
cd jobqueue_features

# Configure our commands to start/stop/clean our containers
source tutorial/jupyter.sh
```

The bash functions hide away the details of what is done to start, stop and clean up
the infrastructure. If you are curious as to what is actually happening, you can look
into the file `tutorial/jupyter.sh` or use the `type` command to see how the function is
defined (e.g., `type start_slurm`).

### Basic Usage

There are three bash functions. The first of which sets the environment up:
```
# Start and configure the cluster
start_slurm
```
This step includes cloning the tutorial (which can be found at
https://github.com/E-CAM/jobqueue_features_workshop_materials) *inside* the cluster.

You should now be able to access the JupyterLab instance from your browser on
`http://localhost:8888/lab/workspaces/lab` and will find a number of notebooks for you
to work through there.

If you would like to stop the tutorial you can use
```
# Stop containers
stop_slurm
```
This command also erases the containers, but does not remove the docker images (which
means you won't need to make a big download again) or user data (which means any
changes you made to notebooks or files will still be available).

### Cleaning up

The infrastructure consumes quite a bit of space and once you have completed the
tutorial, you will probably want to reclaim this. The following command will completely
remove all traces of the infrastructure from your system:
```
# Do a complete clean up of the infrastructure
clean_slurm
```


### IMPORTANT NOTES:
- Please be aware that each docker image uses quite a lot of disk space, as a rule of
  thumb you should have at least 5GB of diskspace available.
- This setup is designed only for your local machine and tutorial usage, it is not
  intended to be used for a heavy workload.
- If your configuration does not allow you to start `docker`/`docker-compose` without
  sudo, you can work around this with
  ```
  sudo bash -c "$(declare -f start_slurm); start_slurm"
  ```
  instead of simply `start_slurm` (with the same approach for `stop_slurm` and
  `clean_slurm`).
