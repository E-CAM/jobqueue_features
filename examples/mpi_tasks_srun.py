from __future__ import print_function
import os

from dask.distributed import LocalCluster

from jobqueue_features.clusters import CustomSLURMCluster
from jobqueue_features.decorators import on_cluster, mpi_task
from jobqueue_features.functions import set_default_cluster
from jobqueue_features.mpi_wrapper import SRUN

import logging

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)

# set_default_cluster(LocalCluster)
set_default_cluster(CustomSLURMCluster)

custom_cluster = CustomSLURMCluster(
    name="mpiCluster",
    walltime="00:04:00",
    nodes=2,
    mpi_mode=True,
    queue_type="gpus",
    mpi_launcher=SRUN,
)


@on_cluster(cluster=custom_cluster, cluster_id="mpiCluster")
@mpi_task(cluster_id="mpiCluster")
def task1(task_name):
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    name = MPI.Get_processor_name()
    all_nodes = comm.gather(name, root=0)
    if all_nodes:
        all_nodes = set(all_nodes)
    else:
        all_nodes = []
    # Since it is a return  value it will only get printed by root
    return "Running %d tasks of type %s on nodes %s." % (size, task_name, all_nodes)


# @on_cluster()  # LocalCluster
def main():
    script_path = os.path.join(os.path.dirname(__file__), "resources", "helloworld.py")
    t = task1("task1")
    print(t.result())


if __name__ == "__main__":
    main()
