[![Build Status](https://travis-ci.com/E-CAM/jobqueue_features.svg?branch=master)](https://travis-ci.com/E-CAM/jobqueue_features)
# jobqueue_features
This library provides some useful decorators for [`dask_jobqueue`](https://github.com/dask/dask-jobqueue). It also expands it's scope to include MPI workloads, including extending configuration options for such workloads and heterogeneous resources.

### Tutorial
To try usage of this library, you are welcome to use SLURM or PBS containers placed in [`tutorial`](https://github.com/E-CAM/jobqueue_features/tree/master/tutorial) folder. Both of them, start couple of docker containers with chosen scheduler and jupyter notebook. To that last one, you will have access on `localhost:8888`. Feel free to try, learn and explore by your browser. 

Basic usage is look like forward:  

SLURM:
```
# start containers
source tutorial/jupyter.sh
start_slurm
# stop and erase containers
erase_slurm
```
PBS:
```
# start containers
source tutorial/jupyter.sh
start_pbs
# stop and erase containers
erase_pbs
```

In case your configuration not allow you start docker/docker-compose without sudo, here are examples how to avoid problems related to that:

SLURM:
```
# start containers
source tutorial/jupyter.sh
sudo bash -c "$(declare -f start_slurm); start_slurm"
# stop and erase containers
sudo bash -c "$(declare -f erase_slurm); erase_slurm"
```
PBS:
```
# start containers
source tutorial/jupyter.sh
sudo bash -c "$(declare -f start_pbs); start_pbs"
# stop and erase containers
sudo bash -c "$(declare -f erase_pbs); erase_pbs"
```

**IMPORTANT:** 
- Containers mentioned above are designed only for local machine, tutorial usage.
- Please start one scheduler containers at once, there are not designed to work next to each other.  