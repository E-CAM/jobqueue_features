from __future__ import print_function
import os

import numpy as np

from jobqueue_features.clusters import CustomSLURMCluster
from jobqueue_features.decorators import on_cluster, mpi_task
from jobqueue_features.mpi_wrapper import mpi_wrap
from jobqueue_features.functions import set_default_cluster

# import logging
# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

set_default_cluster(CustomSLURMCluster)

GROMACS_gpu_cluster = CustomSLURMCluster(
    name='GROMACS_gpu_cluster', walltime='00:15:00', nodes=2, mpi_mode=True, queue_type='gpus', maximum_scale=5,
    env_extra=[
        'module --force purge',
        'module use /usr/local/software/jureca/OtherStages',
        'module load Stages/Devel-2018b',
        'module load Intel/2019.0.117-GCC-7.3.0',
        'module load ParaStationMPI/5.2.1-1',
        'module load GROMACS/2018.3',  # Load this before Dask as it (unnecessarily) depends on Python2 via Boost
        'module load GPUtil/1.3.0-Python-2.7.15',  # Only required for our hello_world2.py example
        'module load Dask/Nov2018Bundle-Python-2.7.15',    # or Dask/Nov2018Bundle-Python-3.6.6
    ]
)

GROMACS_knl_cluster = CustomSLURMCluster(
    name='GROMACS_knl_cluster', walltime='00:15:00', nodes=4, mpi_mode=True, maximum_scale=10, queue_type='knl',
    python='python', env_extra=[
        'module --force purge',
        'unset SOFTWAREROOT',
        'module use /usr/local/software/jurecabooster/OtherStages',
        'module load Stages/Devel-2018b',
        'module load Intel/2019.0.117-GCC-7.3.0',
        'module load IntelMPI/2019.0.117',  # MUST use IntelMPI (don't know why yet)
        'module load GROMACS/2018.3',  # Load this before Dask as it (unnecessarily) depends on Python2 via Boost
        'module load Dask/Nov2018Bundle-Python-2.7.15',  # or Dask/Nov2018Bundle-Python-3.6.6
    ]
)

GROMACS_cluster = CustomSLURMCluster(
    name='GROMACS_cluster', walltime='00:15:00', nodes=2, mpi_mode=True, maximum_scale=10, env_extra=[
        'module --force purge',
        'module use /usr/local/software/jureca/OtherStages',
        'module load Stages/Devel-2018b',
        'module load Intel/2019.0.117-GCC-7.3.0',
        'module load ParaStationMPI/5.2.1-1',
        'module load GROMACS/2018.3',  # Load this before Dask as it (unnecessarily) depends on Python2 via Boost
        'module load GPUtil/1.3.0-Python-2.7.15',  # Only required for our hello_world2.py example
        'module load Dask/Nov2018Bundle-Python-2.7.15',    # or Dask/Nov2018Bundle-Python-3.6.6
    ]
)

openmm_gpu_cluster = CustomSLURMCluster(
    name='openmm_gpu_cluster', walltime='00:15:00', queue_type='gpus', maximum_scale=10, env_extra=[
        'module --force purge',
        'module use /usr/local/software/jureca/OtherStages',
        'module load Stages/Devel-2018b',
        'module load Intel/2019.0.117-GCC-7.3.0',
        'module load ParaStationMPI/5.2.1-1',
        'module load Dask/Nov2018Bundle-Python-2.7.15',
        'module load Miniconda2/4.5.11'  # Give full OPS environment (Python2)
    ]
)

OPS_cluster = CustomSLURMCluster(
    name='OPS_cluster', walltime='00:15:00', maximum_scale=10, env_extra=[
        'module --force purge',
        'module use /usr/local/software/jureca/OtherStages',
        'module load Stages/Devel-2018b',
        'module load Intel/2019.0.117-GCC-7.3.0',
        'module load ParaStationMPI/5.2.1-1',
        'module load Dask/Nov2018Bundle-Python-2.7.15',
        'module load Miniconda2/4.5.11'  # Give full OPS environment (Python2)
    ]
)


# Need to give functions a dummy argument otherwise dask is too clever and only executes the task once
@on_cluster(cluster=GROMACS_gpu_cluster, cluster_id='GROMACS_gpu_cluster', scale=10)
@mpi_task(cluster_id='GROMACS_gpu_cluster')
def run_mpi_gpu(**kwargs):
    script_path = os.path.join(os.path.dirname(__file__), 'resources', 'helloworld2.py')
    t = mpi_wrap(pre_launcher_opts='time -f "%e"', executable='python', exec_args=script_path, **kwargs)
    return t


@on_cluster(cluster=GROMACS_knl_cluster, cluster_id='GROMACS_knl_cluster')
@mpi_task(cluster_id='GROMACS_knl_cluster')
def run_mpi_knl(**kwargs):
    script_path = os.path.join(os.path.dirname(__file__), 'resources', 'helloworld2.py')
    t = mpi_wrap(pre_launcher_opts='time -f "%e"', executable='python', exec_args=script_path, **kwargs)
    return t


@on_cluster(cluster=GROMACS_cluster, cluster_id='GROMACS_cluster')
@mpi_task(cluster_id='GROMACS_cluster')
def run_mpi(**kwargs):
    script_path = os.path.join(os.path.dirname(__file__), 'resources', 'helloworld2.py')
    t = mpi_wrap(pre_launcher_opts='time -f "%e"', executable='python', exec_args=script_path, **kwargs)
    return t


def main():
    t_gpu = []
    t_knl = []
    t = []
    for x in range(50):
        t_gpu.append(run_mpi_gpu())
        t_knl.append(run_mpi_knl())
        t.append(run_mpi())

    runtimes_gpu = [float(i.result()['err']) for i in t_gpu]
    print("GPU Compute Total ", sum(runtimes_gpu), " : Average ", np.mean(runtimes_gpu), " +/- ", np.var(runtimes_gpu))
    runtimes = [float(i.result()['err']) for i in t]
    print("Cluster Compute Total ", sum(runtimes), " : Average ", np.mean(runtimes), " +/- ", np.var(runtimes))
    runtimes_knl = [float(i.result()['err']) for i in t_knl]
    print("KNL Compute Total ", sum(runtimes_knl), " : Average ", np.mean(runtimes_knl), " +/- ", np.var(runtimes_knl))


if __name__ == '__main__':
    main()
