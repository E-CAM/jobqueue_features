import shlex
import subprocess
from typing import Dict  # noqa


SRUN = "srun"
MPIEXEC = "mpiexec"

SUPPORTED_MPI_LAUNCHERS = [SRUN, MPIEXEC]


def mpi_wrap(
    executable=None,
    pre_launcher_opts="",
    mpi_launcher=None,
    launcher_args="",
    mpi_tasks=None,
    nodes=None,
    cpus_per_task=None,
    ntasks_per_node=None,
    exec_args="",
    **kwargs
):
    # type: (str, str, str, str, str, ...) -> Dict[str, str]
    def get_default_mpi_params(
        mpi_launcher, mpi_tasks, nodes, cpus_per_task, ntasks_per_node
    ):
        """
        Based on the requested process distribution, return default arguments for the MPI launcher.

        :param mpi_launcher: The MPI launcher (such as mpiexec, srun, mpirun,...)
        :param mpi_tasks: Total number of MPI tasks
        :param nodes: Number of nodes requested (optional)
        :param cpus_per_task: Number of CPUs per MPI task (most relevant for hybrid jobs)
        :param ntasks_per_node: Number of MPI tasks per node
        :return: string
        """
        if mpi_launcher == SRUN:
            # SLURM already has everything it needs from the environment variables set by the batch script
            return ""
        elif mpi_launcher == MPIEXEC:
            # mpiexec is defined by the standard and very basic, you can only tell it how many MPI tasks to start
            return "-np {}".format(mpi_tasks)
        else:
            raise NotImplementedError(
                "MPI launcher {mpi_launcher} is not yet supported.".format(
                    mpi_launcher=mpi_launcher
                )
            )

    if mpi_launcher is None:
        raise ValueError("The kwarg mpi_launcher must be set!")
    if not isinstance(executable, str):
        ValueError(
            "executable is interpreted as a simple basestring: %(executable)".format(
                executable=executable
            )
        )
    # Let's not error-check excessively, only the most obvious
    if mpi_tasks is None and any([nodes is None, ntasks_per_node is None]):
        raise ValueError(
            "If mpi_tasks is not set then nodes and ntasks_per_node must be set instead"
        )
    default_launcher_args = get_default_mpi_params(
        mpi_launcher, mpi_tasks, nodes, cpus_per_task, ntasks_per_node
    )
    cmd = "{pre_launcher_opts} {mpi_launcher} {default_launcher_args} {launcher_args} {executable} {exec_args}".format(
        pre_launcher_opts=pre_launcher_opts,
        mpi_launcher=mpi_launcher,
        default_launcher_args=default_launcher_args,
        launcher_args=launcher_args,
        executable=executable,
        exec_args=exec_args,
    )
    try:
        proc = subprocess.Popen(
            shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out = proc.stdout.read()
        err = proc.stderr.read()
    except OSError as err:
        raise OSError(
            "OS error caused by constructed command: {cmd}\n\n{err}".format(
                cmd=cmd, err=err
            )
        )
    return {"cmd": cmd, "out": out, "err": err}
