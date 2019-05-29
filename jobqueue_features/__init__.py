# flake8: noqa
from . import config
from .clusters_controller import ClusterController
from .custom_exceptions import ClusterException
from .decorators import task, on_cluster, mpi_task
from .functions import set_default_cluster
from .mpi_wrapper import (
    mpi_wrap,
    MPIEXEC,
    SRUN,
    which,
    serialize_function_and_args,
    deserialize_and_execute,
    mpi_deserialize_and_execute,
    verify_mpi_communicator,
)

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
