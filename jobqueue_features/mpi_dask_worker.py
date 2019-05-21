#!/usr/bin/env python
"""
Distribution of MPI enabled tasks
"""

from jobqueue_features.mpi_wrapper import (
    mpi_deserialize_and_execute,
    serialize_function_and_args,
    shutdown_mpitask_worker,
)
from distributed.cli.dask_worker import go
from mpi4py import MPI


def prepare_for_mpi_tasks():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:
        # Start dask so root reports to scheduler and accepts tasks
        # Task distribution is part of task itself (via our wrapper)
        go()

        # As a final task, send a shutdown to the other MPI ranks
        serialized_object = serialize_function_and_args(shutdown_mpitask_worker)
        mpi_deserialize_and_execute(serialized_object=serialized_object)
    else:
        while True:
            # Calling with no arguments means these are non-root processes
            mpi_deserialize_and_execute()


if __name__ == "__main__":
    prepare_for_mpi_tasks()
