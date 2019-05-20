#!/usr/bin/env python
"""
Distribution of MPI enabled tasks
"""

from jobqueue_features.mpi_wrapper import serialize_function_and_args, mpi_deserialize_and_execute
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    # This is the task, which is only defined on root
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

    def task2(name, task_name="default"):
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        print("Hi %s, my rank is %d for task of type %s" % (name, rank, task_name))

    def shutdown():
        from mpi4py import MPI

        # Add a barrier to be careful
        comm = MPI.COMM_WORLD
        comm.Barrier()
        # Finalise MPI
        MPI.Finalize()
        # and then exit
        exit()

    serialized_object = serialize_function_and_args(task1, "task1")
    result = mpi_deserialize_and_execute(serialized_object=serialized_object)
    if result:
        print(result)
    serialized_object = serialize_function_and_args(task2, "alan", task_name="task2")
    mpi_deserialize_and_execute(serialized_object=serialized_object)
    serialized_object = serialize_function_and_args(shutdown)
    mpi_deserialize_and_execute(serialized_object=serialized_object)

else:
    while True:
        mpi_deserialize_and_execute()
