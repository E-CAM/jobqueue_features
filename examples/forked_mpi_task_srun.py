from __future__ import print_function
import os

from dask.distributed import LocalCluster

from jobqueue_features.clusters import CustomSLURMCluster
from jobqueue_features.decorators import on_cluster, mpi_task
from jobqueue_features.mpi_wrapper import mpi_wrap, MPIEXEC
from jobqueue_features.functions import set_default_cluster

import logging

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)

# set_default_cluster(LocalCluster)
set_default_cluster(CustomSLURMCluster)

custom_cluster = CustomSLURMCluster(
    interface="",
    name="mpiCluster",
    walltime="00:04:00",
    nodes=2,
    cores_per_node=2,
    minimum_cores=2,
    hyperthreading_factor=1,
    ntasks_per_node=2,
    memory="256 MB",
    mpi_mode=True,
    fork_mpi=True,
    env_extra=[
        "export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1",
        "export OMPI_ALLOW_RUN_AS_ROOT=1",
    ],
    mpi_launcher=MPIEXEC,
)


@on_cluster(cluster=custom_cluster, cluster_id="mpiCluster")
@mpi_task(cluster_id="mpiCluster")
def mpi_wrap_task(**kwargs):
    return mpi_wrap(**kwargs)


# @on_cluster()  # LocalCluster
def main():
    script_path = os.path.join(os.path.dirname(__file__), "resources", "helloworld.py")
    t = mpi_wrap_task(executable="python", exec_args=script_path)
    print(t.result())


if __name__ == "__main__":
    main()
