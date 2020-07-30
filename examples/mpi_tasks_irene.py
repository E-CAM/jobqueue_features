from __future__ import print_function
import sys

import numpy as np

from dask.distributed import as_completed

from jobqueue_features.clusters import CustomSLURMJob, CustomSLURMCluster
from jobqueue_features.decorators import on_cluster, mpi_task
from jobqueue_features.mpi_wrapper import SRUN, get_task_mpi_comm


class IreneJob(CustomSLURMJob):
    submit_command = "ccc_msub"
    cancel_command = "ccc_mdel"
    mpi_launcher = "ccc_mprun"
    parameters_map = {
        "-p": "-q",
        "-A": "-A",
        "-m": "-m",
        "-t": "-T",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        sbatch_prefix = "#SBATCH"
        msub_prefix = "#MSUB"
        header_lines = self.job_header.split("\n")
        modified_lines = []
        slurm_parameters = []
        for line in header_lines:
            if line.startswith(sbatch_prefix):
                stripped = line.replace(f"{sbatch_prefix} ", "")
                for k, v in self.parameters_map.items():
                    if stripped.startswith(k):
                        modified_lines.append(f"{msub_prefix} {stripped.replace(k, v)}")
                        break
                else:
                    slurm_parameters.append(stripped)
            else:
                modified_lines.append(line)

        idx = modified_lines.index("")
        self.job_header = "\n".join(modified_lines[:idx])
        self.job_header += f"\n{msub_prefix} -E '{' '.join(slurm_parameters)}'\n"
        self.job_header += "\n".join(modified_lines[idx:])

        self._command_template = self._command_template.replace(
            "srun", self.mpi_launcher
        )


class IreneCluster(CustomSLURMCluster):
    job_cls = IreneJob


PROJECT_NAME = "pa5236"


knl_kwargs = {
    'queue': 'knl',
    'cores_per_node': 64,
    'ntasks_per_node': 64,
    'memory': '96GB',
}

custom_cluster = IreneCluster(
    name="mpiCluster",
    project=PROJECT_NAME,
    walltime='3600',
    interface="",
    nodes=2,
    mpi_mode=True,
    maximum_jobs=10,
    python='python3',
    mpi_launcher=SRUN,
    job_extra=[
        "-m scratch",
    ],
    env_extra=[
        "module purge",
        "module load python3/3.7.2",
        "export PYTHONPATH=/ccc/cont005/home/unipolog/wlodarca/installed/lib/python3.7/site-packages/",

    ],
    **knl_kwargs
)


@on_cluster(cluster=custom_cluster, cluster_id="mpiCluster")
@mpi_task(cluster_id="mpiCluster")
def task():
    import time
    import os

    from mpi4py import MPI

    os.chdir('/tmp')


    comm = get_task_mpi_comm()
    rank = comm.Get_rank()

    return_string = ""
    elapsed_time = 0

    if rank == 0:
        t = time.perf_counter()

        time.sleep(0.01)

        return_string = "Done"

        elapsed_time = time.perf_counter() - t

    # The flush is required to ensure that the print statements appear in the job log
    # files
    sys.stdout.flush()

    return return_string, elapsed_time


def main():
    tasks = []

    if len(sys.argv) == 2:
        n_samples = int(sys.argv[1])
    else:
        n_samples = 10

    # Let's only allocated 2000 futures and replenish as they complete
    if n_samples > 1000:
        running_tasks = 2000
    else:
        # Let's facilitate failures (and work stealing)
        running_tasks = round(1.4 * n_samples)
    for t in range(running_tasks):
        tasks.append(task())

    timings = []
    count = 0

    sequence = as_completed(tasks, with_results=True)

    for future, result in sequence:
        timings.append(result[1])
        count += 1
        if count == n_samples:
            break
        else:
            if n_samples - count > 500:
                # If we haven't finished, replace the tasks (when it is worthwhile)
                sequence.add(task())

    # Shut down the cluster now
    custom_cluster.close()

    # Cancel any pending futures once finished
    # for future in tasks:
    #   future.cancel()

    print(
        "Cluster Compute Total (",
        len(timings),
        " samples)",
        sum(timings),
        " : Average ",
        np.mean(timings),
        " +/- ",
        np.var(timings),
    )


if __name__ == "__main__":
    main()
