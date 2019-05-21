from distributed.protocol import serialize, deserialize
import shlex
import subprocess
import sys
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
    return_wrapped_command=False,
    **kwargs
):
    # type: (str, str, str, str, str, ...) -> Dict[str, str]
    def get_default_mpi_params(
        mpi_launcher, mpi_tasks, nodes, cpus_per_task, ntasks_per_node
    ):
        """
        Based on the requested process distribution, return default arguments for the
        MPI launcher.

        :param mpi_launcher: The MPI launcher (such as mpiexec, srun, mpirun,...)
        :param mpi_tasks: Total number of MPI tasks
        :param nodes: Number of nodes requested (optional)
        :param cpus_per_task: Number of CPUs per MPI task (most relevant for hybrid
        jobs)
        :param ntasks_per_node: Number of MPI tasks per node
        :return: string
        """
        if mpi_launcher == SRUN:
            # SLURM already has everything it needs from the environment variables set
            # by the batch script
            return ""
        elif mpi_launcher == MPIEXEC:
            # mpiexec is defined by the standard and very basic, you can only tell it
            # how many MPI tasks to start
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
    cmd = (
        "{pre_launcher_opts} {mpi_launcher} {default_launcher_args} {launcher_args}"
        " {executable} {exec_args}".format(
            pre_launcher_opts=pre_launcher_opts,
            mpi_launcher=mpi_launcher,
            default_launcher_args=default_launcher_args,
            launcher_args=launcher_args,
            executable=executable,
            exec_args=exec_args,
        )
    )
    if return_wrapped_command:
        result = cmd
    else:
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
        result = {"cmd": cmd, "out": out, "err": err}

    return result


def shutdown_mpitask_worker():
    from mpi4py import MPI

    # Finalise MPI
    MPI.Finalize()
    # and then exit
    exit()


def deserialize_and_execute(serialized_object):
    # Ensure the serialized object is of the expected type
    if isinstance(serialized_object, dict):
        # Make sure it has the expected entries
        if not ("header" in serialized_object and "frames" in serialized_object):
            raise RuntimeError(
                "serialized_object dict does not have expected keys [header, frames]"
            )
    else:
        raise RuntimeError("Cannot deserialize without a serialized_object")
    func = deserialize(serialized_object["header"], serialized_object["frames"])
    if serialized_object.get("args_header"):
        args = deserialize(
            serialized_object["args_header"], serialized_object["args_frames"]
        )
    else:
        args = []
    if serialized_object.get("kwargs_header"):
        kwargs = deserialize(
            serialized_object["kwargs_header"], serialized_object["kwargs_frames"]
        )
    else:
        kwargs = {}

    # Free memory space used by (potentially large) serialised object
    del serialized_object

    # Execute the function and return
    return func(*args, **kwargs)


def flush_and_abort(msg="Flushing print buffer and aborting", comm=None, error_code=1):
    from mpi4py import MPI

    if comm is None:
        comm = MPI.COMM_WORLD
    print(msg)
    sys.stdout.flush()
    if error_code == 0:
        print("To abort correctly, we need to use a non-zero error code")
        error_code = 1
    comm.Abort(error_code)


def mpi_deserialize_and_execute(serialized_object=None, root=0, comm=None):
    from mpi4py import MPI

    if comm is None:
        comm = MPI.COMM_WORLD

    # We only handle the case where root has the object and is the one who returns
    # something
    if serialized_object:
        # Check we have a valid communicator
        try:
            rank = comm.Get_rank()
        except AttributeError:
            flush_and_abort(
                msg="Looks like you did not pass a valid MPI communicator, aborting "
                "using global communicator"
            )
            sys.stdout.flush()
            MPI.COMM_WORLD.Abort(1)
        if rank != root:
            flush_and_abort(
                msg="Only root rank (%d) can contain a serialized object for this "
                "call, my rank is %d...aborting!" % (root, rank),
                comm=comm,
            )
        return_something = True
    else:
        return_something = False
    serialized_object = comm.bcast(serialized_object, root=root)
    result = deserialize_and_execute(serialized_object)

    if return_something and result:
        return result


def serialize_function_and_args(func, *args, **kwargs):
    header, frames = serialize(func)
    serialized_object = {"header": header, "frames": frames}
    if args:
        args_header, args_frames = serialize(args)
        serialized_object.update(
            {"args_header": args_header, "args_frames": args_frames}
        )
    if kwargs:
        kwargs_header, kwargs_frames = serialize(kwargs)
        serialized_object.update(
            {"kwargs_header": kwargs_header, "kwargs_frames": kwargs_frames}
        )

    return serialized_object
