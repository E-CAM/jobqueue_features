from __future__ import print_function
import os

from dask.distributed import LocalCluster

from jobqueue_features.decorators import on_cluster, mpi_task
from jobqueue_features.mpi_wrapper import mpi_wrap, MPIEXEC
from jobqueue_features.functions import set_default_cluster

# import logging
# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

set_default_cluster(LocalCluster)


@mpi_task()
def mpi_wrap_task(**kwargs):
    return mpi_wrap(**kwargs)


@on_cluster()
def main():
    script_path = os.path.join(os.path.dirname(__file__), 'resources', 'helloworld.py')
    t = mpi_wrap_task(executable='python', exec_args=script_path, mpi_launcher=MPIEXEC)
    print(t.result())


if __name__ == '__main__':
    main()
