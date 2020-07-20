from __future__ import print_function

import os
import sys

import numpy as np

from jobqueue_features.clusters import CustomSLURMCluster

from jobqueue_features.decorators import on_cluster, mpi_task
from jobqueue_features.mpi_wrapper import mpi_wrap, CCC_MPRUN, SRUN
from jobqueue_features.functions import set_default_cluster
from jobqueue_features.irene import IreneCluster

# import logging
# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

KNL = True


set_default_cluster(IreneCluster)

NODES = 1
WALLTIME = "120"
MEMORY = "20GB"
if KNL:
    GROMACS_knl_cluster = IreneCluster(
        name="GROMACS_knl_cluster",
        queue="knl",
        project="pa5236",
        walltime=WALLTIME,
        interface="",
        memory=MEMORY,
        nodes=NODES,
        mpi_mode=True,
        fork_mpi=True,
        maximum_jobs=10,
        python="python3",
        mpi_launcher=CCC_MPRUN,
        env_extra=["-m scratch",],
    )

    @on_cluster(cluster=GROMACS_knl_cluster, cluster_id="GROMACS_knl_cluster")
    @mpi_task(cluster_id="GROMACS_knl_cluster")
    def run_mpi_knl(**kwargs):
        # examples_path = os.getenv("JOBQUEUE_FEATURES_EXAMPLES")
        examples_path = (
            "/ccc/cont005/home/unipolog/wlodarca/packages/jobqueue_features/examples"
        )

        script_path = os.path.join(examples_path, "resources", "helloworld2.py")
        t = mpi_wrap(
            pre_launcher_opts='time -f "%e"',
            executable="python3",
            exec_args=script_path,
            **kwargs
        )
        return t


def main():
    t_gpu = []
    t_knl = []
    t_loc = []
    t = []

    if len(sys.argv) == 2:
        n_samples = int(sys.argv[1])
    else:
        n_samples = 50

    f = run_mpi_knl()
    print(f.result())
    # for x in range(n_samples):
    #     if KNL:
    #         t_knl.append(run_mpi_knl())
    #     if LOCAL:
    #         t_loc.append(run_mpi_local())
    #
    #
    # if KNL:
    #     runtimes_knl = [float((i.result()["err"]).split(b"\n")[-2]) for i in t_knl]
    #     print(
    #         "KNL Compute Total (",
    #         len(runtimes_knl),
    #         " samples)",
    #         sum(runtimes_knl),
    #         " : Average ",
    #         np.mean(runtimes_knl),
    #         " +/- ",
    #         np.var(runtimes_knl),
    #     )


if __name__ == "__main__":
    main()
