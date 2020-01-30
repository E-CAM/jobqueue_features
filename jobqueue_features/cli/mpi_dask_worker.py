#!/usr/bin/env python
"""
Distribution of MPI enabled tasks
"""

from jobqueue_features.mpi_wrapper import (
    get_task_mpi_comm,
    mpi_deserialize_and_execute,
    serialize_function_and_args,
    set_task_mpi_comm,
    shutdown_mpitask_worker,
    verify_mpi_communicator,
)


# Add the no-nanny option so we don't fork additional processes
MPI_DASK_WRAPPER_MODULE = "jobqueue_features.cli.mpi_dask_worker --no-nanny"


def prepare_for_mpi_tasks(root=0, comm=None):

    if comm is None:
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
    verify_mpi_communicator(comm)

    # Using a setter for the communicator protects us from operating within the
    # MPI.COMM_WORLD context
    set_task_mpi_comm(parent_comm=comm)

    # the task communicator is now behind a getter, so we need to grab it
    task_comm = get_task_mpi_comm()

    rank = task_comm.Get_rank()

    if rank == root:
        # Start dask so root reports to scheduler and accepts tasks
        # Task distribution is part of task itself (via our wrapper)
        from distributed.cli import dask_worker

        dask_worker.go()

        # As a final task, send a shutdown to the other MPI ranks
        serialized_object = serialize_function_and_args(shutdown_mpitask_worker)
        mpi_deserialize_and_execute(
            serialized_object=serialized_object, root=root, comm=task_comm
        )
    else:
        while True:
            # Calling with no serialized_object means these are non-root processes
            # We use get_task_mpi_comm() to allow for dynamic redefinition of the
            # communicator group
            mpi_deserialize_and_execute(root=root, comm=get_task_mpi_comm())


if __name__ == "__main__":
    prepare_for_mpi_tasks()
