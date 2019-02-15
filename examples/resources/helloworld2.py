#!/usr/bin/env python
"""
Parallel Hello World
"""

from mpi4py import MPI
import os
import time

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()
gpus = ""  # Leave a blank default
all_nodes = comm.gather(name, root=0)
if rank == 0:
    all_nodes = set(all_nodes)
    if os.getenv("CUDA_HOME"):
        import GPUtil as GPU

        gpus = "Available GPUs: " + str(GPU.getAvailable(order="first", limit=999))
    print("Running %d tasks on nodes %s. %s" % (size, all_nodes, gpus))
    time.sleep(5)
MPI.Finalize()
