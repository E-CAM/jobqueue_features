from __future__ import print_function
import os
import sys

import numpy as np

from jobqueue_features.clusters import CustomSLURMCluster
from jobqueue_features.decorators import on_cluster, mpi_task
from jobqueue_features.mpi_wrapper import mpi_wrap
from jobqueue_features.functions import set_default_cluster

# import logging
# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

HSW = True
KNL = True
GPU = True


set_default_cluster(CustomSLURMCluster)

if GPU:
    GROMACS_gpu_cluster = CustomSLURMCluster(
        name="GROMACS_gpu_cluster",
        walltime="00:15:00",
        nodes=2,
        mpi_mode=True,
        fork_mpi=True,
        queue_type="gpus",
        maximum_jobs=5,
        env_extra=[
            "module --force purge",
            "module use /usr/local/software/jureca/OtherStages",
            "module load Stages/Devel-2019a",
            "module load Intel",
            "module load ParaStationMPI",
            "module load GROMACS",
            "module load GPUtil",  # Only required for our hello_world2.py example
            "module load dask",
            "module load jobqueue_features",
        ],
    )

if KNL:
    GROMACS_knl_cluster = CustomSLURMCluster(
        name="GROMACS_knl_cluster",
        walltime="00:15:00",
        nodes=4,
        mpi_mode=True,
        fork_mpi=True,
        maximum_jobs=10,
        queue_type="knl",
        python="python",
        env_extra=[
            "module --force purge",
            "unset SOFTWAREROOT",
            "module use /usr/local/software/jurecabooster/OtherStages",
            "module load Stages/Devel-2019a",
            "module load Intel",
            "module load ParaStationMPI",
            "module load GROMACS",
            "module load dask",
            "module load jobqueue_features",
        ],
    )

if HSW:
    GROMACS_cluster = CustomSLURMCluster(
        name="GROMACS_cluster",
        walltime="00:15:00",
        nodes=2,
        mpi_mode=True,
        fork_mpi=True,
        maximum_jobs=10,
        env_extra=[
            "module --force purge",
            "module use /usr/local/software/jureca/OtherStages",
            "module load Stages/Devel-2019a",
            "module load Intel",
            "module load ParaStationMPI",
            "module load GROMACS",
            "module load dask",
            "module load jobqueue_features",
        ],
    )

if GPU:

    @on_cluster(cluster=GROMACS_gpu_cluster, cluster_id="GROMACS_gpu_cluster", jobs=10)
    @mpi_task(cluster_id="GROMACS_gpu_cluster")
    def run_mpi_gpu(**kwargs):
        script_path = os.path.join(
            os.getenv("JOBQUEUE_FEATURES_EXAMPLES"), "resources", "helloworld2.py"
        )
        t = mpi_wrap(
            pre_launcher_opts='time -f "%e"',
            executable="python",
            exec_args=script_path,
            **kwargs
        )
        return t


if KNL:

    @on_cluster(cluster=GROMACS_knl_cluster, cluster_id="GROMACS_knl_cluster")
    @mpi_task(cluster_id="GROMACS_knl_cluster")
    def run_mpi_knl(**kwargs):
        script_path = os.path.join(
            os.getenv("JOBQUEUE_FEATURES_EXAMPLES"), "resources", "helloworld2.py"
        )
        t = mpi_wrap(
            pre_launcher_opts='time -f "%e"',
            executable="python",
            exec_args=script_path,
            **kwargs
        )
        return t


if HSW:

    @on_cluster(cluster=GROMACS_cluster, cluster_id="GROMACS_cluster")
    @mpi_task(cluster_id="GROMACS_cluster")
    def run_mpi(**kwargs):
        script_path = os.path.join(
            os.getenv("JOBQUEUE_FEATURES_EXAMPLES"), "resources", "helloworld2.py"
        )
        t = mpi_wrap(
            pre_launcher_opts='time -f "%e"',
            executable="python",
            exec_args=script_path,
            **kwargs
        )
        return t


def main():
    t_gpu = []
    t_knl = []
    t = []

    if len(sys.argv) == 2:
        n_samples = int(sys.argv[1])
    else:
        n_samples = 50
    for x in range(n_samples):
        if GPU:
            t_gpu.append(run_mpi_gpu())
        if KNL:
            t_knl.append(run_mpi_knl())
        if HSW:
            t.append(run_mpi())

    if GPU:
        runtimes_gpu = [float((i.result()["err"]).split(b"\n")[-2]) for i in t_gpu]
        print(
            "GPU Compute Total (",
            len(runtimes_gpu),
            " samples) ",
            sum(runtimes_gpu),
            " : Average ",
            np.mean(runtimes_gpu),
            " +/- ",
            np.var(runtimes_gpu),
        )
    if HSW:
        runtimes = [float((i.result()["err"]).split(b"\n")[-2]) for i in t]
        print(
            "Cluster Compute Total (",
            len(runtimes),
            " samples)",
            sum(runtimes),
            " : Average ",
            np.mean(runtimes),
            " +/- ",
            np.var(runtimes),
        )
    if KNL:
        runtimes_knl = [float((i.result()["err"]).split(b"\n")[-2]) for i in t_knl]
        print(
            "KNL Compute Total (",
            len(runtimes_knl),
            " samples)",
            sum(runtimes_knl),
            " : Average ",
            np.mean(runtimes_knl),
            " +/- ",
            np.var(runtimes_knl),
        )


if __name__ == "__main__":
    main()
