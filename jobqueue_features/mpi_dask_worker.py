#!/usr/bin/env python
"""
Distribution of MPI enabled tasks
"""

from jobqueue_features.mpi_wrapper import (
    mpi_deserialize_and_execute,
    serialize_function_and_args,
    shutdown_mpitask_worker,
    verify_mpi_communicator,
)
from distributed.cli.dask_worker import go

MPI_DASK_WRAPPER_MODULE = "jobqueue_features.mpi_wrapper.mpi_dask_worker"

def prepare_for_mpi_tasks(root=0, comm=None):
    if comm is None:
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
    verify_mpi_communicator(comm)

    rank = comm.Get_rank()

    if rank == root:
        # Start dask so root reports to scheduler and accepts tasks
        # Task distribution is part of task itself (via our wrapper)
        go()

        # As a final task, send a shutdown to the other MPI ranks
        serialized_object = serialize_function_and_args(shutdown_mpitask_worker)
        mpi_deserialize_and_execute(
            serialized_object=serialized_object, root=root, comm=comm
        )
    else:
        while True:
            # Calling with no arguments means these are non-root processes
            mpi_deserialize_and_execute(root=root, comm=comm)


if __name__ == "__main__":
    prepare_for_mpi_tasks()
