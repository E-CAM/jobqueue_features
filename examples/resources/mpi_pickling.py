#!/usr/bin/env python
"""
Distribution of MPI enabled tasks
"""

from mpi4py import MPI
from distributed.protocol import serialize, deserialize

def deserialize_and_execute(serialized_object=None):
    comm = MPI.COMM_WORLD
    # We only handle the case where root returns something
    if serialized_object:
        return_something = True
    else:
        return_something = False
    serialized_object = comm.bcast(serialized_object, root=0)
    func = deserialize(serialized_object['header'], serialized_object['frames'])
    if serialized_object.get('args_header'):
        args = deserialize(serialized_object['args_header'], serialized_object['args_frames'])
    else:
        args = False
    if serialized_object.get('kwargs_header'):
        kwargs = deserialize(serialized_object['kwargs_header'], serialized_object['kwargs_frames'])
    else:
        kwargs = False

    # Free memory space used by (potentially large) serialised object
    del serialized_object

    if args and kwargs:
        if return_something:
            return func(*args, **kwargs)
        else:
            func(*args, **kwargs)
    elif args:
        if return_something:
            return func(*args)
        else:
            func(*args)
    elif kwargs:
        if return_something:
            return func(**kwargs)
        else:
            func(**kwargs)
    else:
        if return_something:
            return func()
        else:
            func()


def serialize_function_and_args(func, *args, **kwargs):
    header, frames = serialize(func)
    serialized_object = {'header': header, 'frames': frames}
    if args:
        args_header, args_frames = serialize(args)
        serialized_object.update({'args_header': args_header, 'args_frames': args_frames})
    if kwargs:
        kwargs_header, kwargs_frames = serialize(kwargs)
        serialized_object.update({'kwargs_header': kwargs_header, 'kwargs_frames': kwargs_frames})

    return serialized_object


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
        return("Running %d tasks of type %s on nodes %s." % (size, task_name, all_nodes))

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
    result = deserialize_and_execute(serialized_object=serialized_object)
    if result:
        print(result)
    serialized_object = serialize_function_and_args(task2, "alan", task_name="task2")
    deserialize_and_execute(serialized_object=serialized_object)
    serialized_object = serialize_function_and_args(shutdown)
    deserialize_and_execute(serialized_object=serialized_object)

else:
    while True:
        deserialize_and_execute()
