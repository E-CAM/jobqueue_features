from __future__ import print_function
import os

from jobqueue_features.clusters import CustomSLURMCluster
from jobqueue_features.decorators import on_cluster, mpi_task
from jobqueue_features.functions import set_default_cluster
from jobqueue_features.mpi_wrapper import SRUN

# import logging

# logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)

# set_default_cluster(LocalCluster)
set_default_cluster(CustomSLURMCluster)

custom_cluster = CustomSLURMCluster(
    name="mpiCluster", walltime="00:03:00", nodes=2, mpi_mode=True, mpi_launcher=SRUN
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
    return_string = "Running %d tasks of type %s on nodes %s." % (
        size,
        task_name,
        all_nodes,
    )
    print(return_string)
    return return_string


# @on_cluster()  # LocalCluster
def main():
    t1 = task1("task1")
    t2 = task1("task1, 2nd iteration")
    print(t1.result())
    print(t2.result())


if __name__ == "__main__":
    main()
