from __future__ import division

import re
from contextlib import suppress

from dask import config
from dask_jobqueue import SLURMCluster, JobQueueCluster, PBSCluster
from dask.distributed import Client, LocalCluster
from dask_jobqueue.pbs import PBSJob
from dask_jobqueue.slurm import SLURMJob
from typing import TypeVar, Dict, List, Any  # noqa

from .cli.mpi_dask_worker import MPI_DASK_WRAPPER_MODULE
from .mpi_wrapper import mpi_wrap
from .custom_exceptions import ClusterException

import logging

logger = logging.getLogger(__name__)

SLURM = "slurm"
SUPPORTED_SCHEDULERS = [SLURM]

custom_cluster_attributes = """
    default_queue_type : str
        Default queue_type for the scheduler
    queue_type : str
        The queue_type for the scheduler
    cores_per_node : int
        Cores_per_node: number of physical cores in a node
    hyperthreading_factor : int
        Hyperthreading_factor: hyperthreading available per physical core
    minimum_cores : int
        Minimum amount of cores in a job allocation
    gpu_job_extra : List[str]
        Extra scheduler arguments when requesting a GPU job
    warnings : List[str]
        A string that holds any desired warning (is turned into a list of warnings in
        self.warnings)
    mpi_mode : bool
        Whether the cluster is to run MPI tasks
    mpi_launcher : str
        The command that launches MPI jobs (srun, mpiexec, mpirun,...)
    fork_mpi: bool
        If true, assume all tasks for the cluster fork MPI processes (using mpi_wrap())
        rather than that the task itself is MPI-enabled (jobqueue will then only manage
        a single core, the rest are managed by the mpi_launcher)
    nodes : int
        The number of nodes required for MPI
    ntasks_per_node : int
        The number of MPI tasks per node to be used
    cpus_per_task : int
        The number of cpus to be used per (MPI) task (typically this is for OpenMP)
    openmp_env_extra : List[str]
        List of additional environment settings for OpenMP workloads
        (similar to job_env_extra in jobqueue)
    maximum_jobs : int
        Maximum amount of jobs for the cluster to scale to
    pure : bool
        Whether the default for tasks submitted to the cluster are pure or not""".strip()


def get_cluster(scheduler=None, **kwargs):
    if scheduler is None:
        scheduler = config.get("jobqueue-features.scheduler", default=None)
    if scheduler is None:
        raise ValueError(
            "You must configure a scheduler either via a kwarg"
            " or in your configuration file"
        )
    if scheduler == SLURM:
        return CustomSLURMCluster(**kwargs)
    else:
        raise NotImplementedError(
            "Scheduler {} is not in list of supported schedulers: {}".format(
                scheduler, SUPPORTED_SCHEDULERS
            )
        )


def get_features_kwarg(name, scheduler=None, queue_type=None, default=None):
    """
    Search in the jobqueue_features config for a value for kw_name
    :param scheduler: scheduler name to search for in configuration
    :param name: string to search for in configuration
    :param queue_type: queue type to search for in config
    :param default: default value to give if nothing in config files
    :return: value or None
    """
    value = None
    # search for kw_name from bottom up queue_type -> scheduler -> jobqueue_features

    # Error checking
    if not isinstance(name, str):
        raise ValueError('"name" must be a string')
    # if scheduler is None and queue_type is not None:
    if scheduler is None and queue_type is not None:
        raise ValueError("Cannot search in queue_type without providing a scheduler")

    # Now do the config search
    # use default=None in calls since we set defaults ourselves
    if scheduler is not None and queue_type is not None:
        value = config.get(
            "jobqueue-features.{}.queue-type.{}.{}".format(scheduler, queue_type, name),
            default=None,
        )
    if value is None and scheduler is not None:
        value = config.get(
            "jobqueue-features.{}.{}".format(scheduler, name), default=None
        )
    if value is None:
        value = config.get("jobqueue-features.{}".format(name), default=None)
    if value is None and default is not None:
        value = default
    return value


class CustomSLURMJob(SLURMJob):
    def __init__(self, *args, **kwargs):
        self.mpi_tasks = kwargs.pop("mpi_tasks", 1)
        command_template = kwargs.pop("command_template", None)
        super().__init__(*args, **kwargs)
        self.job_header = self.job_header.replace(
            "#SBATCH -n 1\n", "#SBATCH -n {}\n".format(self.mpi_tasks)
        )
        if command_template:
            replacement_name = re.search(
                r"--name\s+(\S+)", self._command_template
            ).group(1)
            expected_name = re.search(r"--name\s+(\S+)", command_template).group(1)
            if expected_name == "name":
                self._command_template = re.sub(
                    "--name {}".format(expected_name),
                    "--name {}".format(replacement_name),
                    command_template,
                )
            else:
                raise ValueError(
                    "Found unexpected value for 'name' in command template: {}".format(
                        expected_name
                    )
                )


class CustomPBSJob(PBSJob):
    def __init__(self, *args, **kwargs):
        if kwargs.get("mpi_mode", False):
            kwargs["resource_spec"] = self.get_resource_spec(**kwargs)
        command_template = kwargs.pop("command_template", None)
        super().__init__(*args, **kwargs)
        if command_template:
            replacement_name = re.search(
                r"--name\s+(\S+)", self._command_template
            ).group(1)
            expected_name = re.search(r"--name\s+(\S+)", command_template).group(1)
            if expected_name == "name":
                self._command_template = re.sub(
                    "--name {}".format(expected_name),
                    "--name {}".format(replacement_name),
                    command_template,
                )
            else:
                raise ValueError(
                    "Found unexpected value for 'name' in command template: {}".format(
                        expected_name
                    )
                )

    def get_resource_spec(self, **kwargs):
        """
        Creates custom resource_spec for PBS jobs script.

        Assumes that:
            "nodes" reflects "nodes" selection option
            "cores_per_node" reflects "ncpus" selection option
            "ntasks_per_node" reflects "mpiprocs" selection option
            "cpus_per_task" reflects "ompthreads" selection option
            "ngpus_per_node" reflects "ngpus" selection option
        """
        nodes = kwargs.pop("nodes", 1)
        cores_per_node = kwargs.pop("cores_per_node", 1)
        mpi_procs = kwargs.pop("ntasks_per_node", 1)
        omp_threads = kwargs.pop("cpus_per_task", 1)
        n_gpus = kwargs.pop("ngpus_per_node", 0)
        resource_spec = f"select={nodes}:ncpus={cores_per_node}:mpiprocs={mpi_procs}"
        if omp_threads > 1:
            resource_spec += f":ompthreads={omp_threads}"
        if n_gpus:
            resource_spec += f":ngpus={n_gpus}"
        return resource_spec


class CustomClusterMixin(object):
    __doc__ = f"""Custom cluster mixin for Cluster kwargs customization.

    Attributes
    ----------
    {custom_cluster_attributes}
    """

    default_queue_type: str = "batch"
    queue_type: str = None
    cores_per_node: int = None
    hyperthreading_factor: int = None
    minimum_cores: int = None
    gpu_job_extra: List[str] = None
    warnings: List[str] = None
    mpi_mode: bool = None
    mpi_launcher: str = None
    fork_mpi: bool = None
    nodes: int = None
    ntasks_per_node: int = None
    cpus_per_task: int = None
    openmp_env_extra: List[str] = None
    maximum_jobs: int = None
    # We only set a pure attribute if it is required or requested
    # pure: bool = None

    def update_init_kwargs(self, **kwargs):
        # self.submit_command is set by the JobQueueCluster class, make sure it exists
        if self.job_cls.submit_command is None:
            raise NotImplementedError(
                "For inheritance to work as intended you need to create new "
                "CustomCluster class that inherits from the base CustomCluster class "
                "and your target class in JobQueue (such as *SLURMCluster*)"
            )

        self._get_queue_type(kwargs.get("queue_type"))

        # Let's do the system related kwargs first
        self._get_cores_per_node(kwargs.get("cores_per_node"))
        self._get_hyperthreading_factor(kwargs.get("hyperthreading_factor"))
        self._get_minimum_cores(kwargs.get("minimum_cores"))
        self._get_gpu_job_extra(kwargs.get("gpu_job_extra"))
        self._get_warnings(kwargs.get("warning"))

        # Now do MPI related kwargs.
        # Check if tasks use MPI runtime or will fork MPI processes
        self._get_fork_mpi(kwargs.get("fork_mpi"))
        # Gather parameters for distribution of MPI/OpenMP processes (this also
        # modifies the cores reported to dask by the worker)
        kwargs = self._update_kwargs_cores(**kwargs)

        # Control default nanny behaviour
        kwargs = self._update_kwargs_nanny(**kwargs)

        # Check for any updates to other modifiable jobqueue values:
        #   name, queue, memory
        kwargs = self._update_kwargs_modifiable(**kwargs)
        # update job_extra as needed, first check if we should initialise it
        kwargs = self._update_kwargs_job_extra(**kwargs)
        # update env_extra if needed
        kwargs = self._update_kwargs_env_extra(**kwargs)

        # Finally, define how many workers the cluster can scale out to
        self._get_maximum_jobs(kwargs.get("maximum_jobs"))
        # and whether tasks for this cluster are pure by default or not
        if self.mpi_mode:
            # in MPI mode we default pure to false
            default = False
            self.warnings.append(
                "For this cluster with mpi mode, default value of 'pure' set to {} (can be overridden by kwarg)".format(
                    default
                )
            )
        else:
            default = None
        self._get_pure(pure=kwargs.get("pure"), default=default)

        return kwargs

    def get_kwarg(self, name: str, default: Any = None) -> Any:
        return get_features_kwarg(
            name=name,
            scheduler=self.scheduler_name,
            queue_type=self.queue_type,
            default=default,
        )

    def validate_positive_integer(self, attr_name: str) -> None:
        value = getattr(self, attr_name, None)
        if not (isinstance(value, int) and value >= 1):
            raise ValueError("{} should be an integer >= 1".format(attr_name))

    def validate_cluster_name(self, name):
        from .clusters_controller import clusters_controller_singleton

        try:
            clusters_controller_singleton.get_cluster(id_=name)
        except:
            pass
        else:
            raise ClusterException('Cluster with name "{}" already exists'.format(name))

    def _add_to_cluster_controller(self):
        from .clusters_controller import clusters_controller_singleton

        clusters_controller_singleton.add_cluster(id_=self.name, cluster=self)

    def _get_queue_type(self, queue_type: str, default: Any = None) -> None:
        if default is None:
            default = self.default_queue_type
        # If the user sets the kwarg make sure that the queue_type actually exists
        if queue_type:
            # Get a list of the available options
            try:
                avail_queue_types = [
                    key
                    for key in config.config["jobqueue-features"][self.scheduler_name][
                        "queue-type"
                    ]
                ]
            except KeyError:
                avail_queue_types = []
            if self.default_queue_type not in avail_queue_types:
                avail_queue_types.append(self.default_queue_type)
            if queue_type not in avail_queue_types:
                raise ValueError(
                    "queue_type kwarg value '{queue_type}' not in available options: "
                    "{av_queue_types}".format(
                        queue_type=queue_type, av_queue_types=avail_queue_types
                    )
                )
        self.queue_type = queue_type or self.get_kwarg(
            name="default-queue-type", default=default
        )

    def _get_cores_per_node(self, cores_per_node: int, default: int = 1) -> None:
        self.cores_per_node = (
            cores_per_node
            if cores_per_node is not None
            else self.get_kwarg(name="cores-per-node", default=default)
        )
        self.validate_positive_integer("cores_per_node")

    def _get_hyperthreading_factor(
        self, hyperthreading_factor: int, default: int = 1
    ) -> None:
        self.hyperthreading_factor = (
            hyperthreading_factor
            if hyperthreading_factor is not None
            else self.get_kwarg(name="hyperthreading-factor", default=default)
        )
        self.validate_positive_integer("hyperthreading_factor")

    def _get_minimum_cores(self, minimum_cores: int, default: int = 1) -> None:
        self.minimum_cores = (
            minimum_cores
            if minimum_cores is not None
            else self.get_kwarg(name="minimum-cores", default=default)
        )
        self.validate_positive_integer("minimum_cores")
        if self.minimum_cores > self.cores_per_node * self.hyperthreading_factor:
            raise ValueError(
                "minimum_cores cannot be > {} (cores_per_node * "
                "hyperthreading_factor)".format(
                    self.cores_per_node * self.hyperthreading_factor
                )
            )

    def _get_gpu_job_extra(self, gpu_job_extra: List[str], default: Any = None) -> None:
        if default is None:
            default = []
        self.gpu_job_extra = gpu_job_extra or self.get_kwarg(
            name="gpu-job-extra", default=default
        )

    def _get_warnings(self, warning: List[str], default: Any = None) -> None:
        if default is None:
            default = []
        if not warning:
            warning = self.get_kwarg("warning", default=default)
        self.warnings = [warning]

    def _get_mpi_mode(self, mpi_mode: bool, default: bool = False) -> None:
        self.mpi_mode = (
            mpi_mode
            if isinstance(mpi_mode, bool)
            else self.get_kwarg(name="mpi-mode", default=default)
        )

    def _get_mpi_launcher(self, mpi_launcher: str, default: Any = None) -> None:
        self.mpi_launcher = mpi_launcher or self.get_kwarg(
            name="mpi-launcher", default=default
        )
        if self.mpi_mode and not self.mpi_launcher:
            raise ValueError(
                "When using MPI mode, an MPI launcher (such as srun, mpirun,...) must "
                "be set via the mpi_launcher kwarg or the yaml configuration"
            )

    def _get_fork_mpi(self, fork_mpi: bool, default: bool = False) -> None:
        self.fork_mpi = (
            fork_mpi
            if isinstance(fork_mpi, bool)
            else self.get_kwarg(name="fork-mpi", default=default)
        )

    def _get_maximum_jobs(self, maximum_jobs: int, default: int = 1) -> None:
        self.maximum_jobs = maximum_jobs if maximum_jobs is not None else default
        self.validate_positive_integer("maximum_jobs")

    def _get_pure(self, pure: bool, default: Any = None) -> None:
        if isinstance(pure, bool):
            self.pure = pure
        elif isinstance(default, bool):
            self.pure = default
        else:
            self.warnings.append(
                "No boolean value for 'pure' or default, not setting default value for cluster."
            )

    def _update_kwargs_cores(self, **kwargs) -> Dict[str, Any]:
        self._get_mpi_mode(kwargs.get("mpi_mode"))
        if self.mpi_mode:
            self._get_mpi_launcher(kwargs.get("mpi_launcher"))
            self.nodes = kwargs.get("nodes", self.get_kwarg("nodes"))
            self.ntasks_per_node = kwargs.get(
                "ntasks_per_node", self.get_kwarg("ntasks-per-node")
            )
            self.cpus_per_task = kwargs.get(
                "cpus_per_task", self.get_kwarg("cpus-per-task")
            )
            cores = kwargs.get("cores")
            if cores:
                expected_cores = cores
                self.warnings.append(
                    "In MPI mode we assume that when you provide 'cores' you mean "
                    "total number of required CPUs: (number of MPI tasks) * (number of "
                    "cpus per task)"
                )
            else:
                expected_cores = None

            # We should now have everything we need to define necessary values for any
            # MPI/OpenMP task
            #   self.nodes (optional)
            #   self.cpus_per_task
            #   self.mpi_tasks
            #   self.ntasks_per_node
            if self.nodes is None:
                # Defining 'nodes' is not essential
                if expected_cores is None:
                    raise ValueError(
                        "Not enough information to derive MPI/OpenMP configuration"
                    )
                if self.ntasks_per_node is None:
                    if self.cpus_per_task is None:
                        # assume expected_cores is total number of tasks
                        self.cpus_per_task = 1
                    elif expected_cores % self.cpus_per_task != 0:
                        raise ValueError('"cores" not divisible by "cpus_per_task"')
                    # assume physical cores when 'nodes' not used
                    self.ntasks_per_node = self.cores_per_node // self.cpus_per_task
                else:
                    if self.cpus_per_task is None:
                        if (
                            self.cores_per_node % self.ntasks_per_node != 0
                        ):  # assume physical
                            raise ValueError(
                                '"cores_per_node" not divisible by "ntasks_per_node"'
                            )
                        else:
                            self.cpus_per_task = (
                                self.cores_per_node // self.ntasks_per_node
                            )
                    if expected_cores % self.cpus_per_task != 0:
                        raise ValueError("'cores' not divisible by 'cpus_per_task'")
                # self.mpi_tasks is derived from expected_cores and cpus_per_task
                self.mpi_tasks = expected_cores // self.cpus_per_task
            else:
                if expected_cores is not None:
                    raise ValueError(
                        "Coupling 'cores' with 'nodes' is not expected, if using "
                        "'nodes' please stick to: 'nodes', 'ntasks-per-node' and (for "
                        "OpenMP/threading) 'cpus_per_task'"
                    )
                if self.cpus_per_task is None:
                    if self.ntasks_per_node is None:
                        self.cpus_per_task = 1  # assume no OpenMP
                        self.ntasks_per_node = self.cores_per_node  # assume physical
                    else:
                        if (
                            self.ntasks_per_node
                            > self.cores_per_node * self.hyperthreading_factor
                        ):
                            raise ValueError(
                                '"ntasks_per_node" cannot be higher than '
                                + '"cores_per_node" * "self.hyperthreading_factor"'
                            )
                        if (
                            self.ntasks_per_node < self.cores_per_node
                        ):  # assume physical is what we want
                            self.cpus_per_task = (
                                self.cores_per_node // self.ntasks_per_node
                            )  # safe since ints
                        else:
                            self.cpus_per_task = 1
                else:
                    if self.ntasks_per_node is None:
                        self.ntasks_per_node = 1
                    if (
                        self.ntasks_per_node * self.cpus_per_task
                        > self.cores_per_node * self.hyperthreading_factor
                    ):
                        raise ValueError(
                            '"ntasks_per_node" * "cpus_per_task" cannot be higher than '
                            + '"cores_per_node" * "self.hyperthreading_factor"'
                        )
                # Calculate total number of MPI tasks
                self.mpi_tasks = self.nodes * self.ntasks_per_node

            # If we have an OpenMP job we should check if there are some additional
            # environment flags we should set
            if self.cpus_per_task > 1:
                self.openmp_env_extra = self.get_kwarg("openmp-env-extra") or []

            # We need to "trick" jobqueue into managing an MPI job, we will pretend
            # there is on one core available (root) but we will actually allocate more.
            # It will then schedule tasks to this Cluster type that can, depending on
            # the value of 'fork_mpi', either:
            # - Leverage all available processes via the tasks MPI capabilities and
            #   MPI.COMM_WORLD
            # - or fork out MPI executables using our wrapper
            kwargs.update({"cores": 1})

        else:
            # If we are not in MPI mode we can do some simple checks
            if any(
                k in kwargs
                for k in (
                    "mpi_launcher",
                    "nodes",
                    "ntasks_per_node",
                    "cpus_per_task",
                    "openmp_env_extra",
                )
            ):
                self.warnings.append(
                    "kwargs mpi_launcher, nodes, ntasks_per_node, "
                    "cpus_per_task and openmp_env_extra "
                    "are only relevant in mpi_mode=True"
                )

            # Check the number of cores requested sits between the minimum and maximum
            cores = kwargs.get("cores")
            if not cores:
                features_cores = self.get_kwarg("cores")
                if not features_cores:
                    # grab the value from jobqueue configuration
                    features_cores = config.get(
                        "jobqueue.{}.cores".format(self.scheduler_name),
                        default=self.minimum_cores,
                    )
                    if not features_cores:
                        features_cores = self.minimum_cores
            else:
                features_cores = cores
            if features_cores > self.cores_per_node * self.hyperthreading_factor:
                raise ValueError(
                    "cores cannot be > {} (cores_per_node * "
                    "hyperthreading_factor)".format(
                        self.cores_per_node * self.hyperthreading_factor
                    )
                )
            if features_cores < self.minimum_cores:
                self.warnings.append(
                    "Increasing cores from {} to minimum value {}".format(
                        features_cores, self.minimum_cores
                    )
                )
                features_cores = self.minimum_cores
            # Can now safely update kwargs with the cores value
            kwargs.update({"cores": features_cores})
        return kwargs

    def _update_kwargs_nanny(self, **kwargs) -> Dict[str, Any]:
        # In (non-forked) MPI mode, the nanny is problematic, default to False there
        if self.mpi_mode and not self.fork_mpi and not kwargs.get("nanny"):
            self.warnings.append(
                "In (non-forked) MPI mode the nanny can be problematic, we change "
                "the default to False for this case, override with the 'nanny' kwarg "
                "boolean"
            )
            kwargs.update({"nanny": False})
        return kwargs

    def _update_kwargs_modifiable(self, **kwargs) -> Dict[str, Any]:
        for key in ("name", "queue", "memory"):
            if key not in kwargs:
                # search for the key in our config
                value = self.get_kwarg(key)
                if value is not None:
                    kwargs.update({key: value})
            # When we use `name`, we also mean `job_name`
            if key == "name":
                kwargs.update({"job_name": kwargs[key]})

        return kwargs

    def _update_kwargs_job_extra(self, **kwargs) -> Dict[str, Any]:
        job_extra = kwargs.get("job_extra", self.get_kwarg("job-extra"))
        if job_extra is None:
            job_extra = config.get(
                "jobqueue.{}.job_extra".format(self.scheduler_name), default=[]
            )

        # order matters, to ensure user has power to be in control make sure their
        # settings come last
        final_job_extra = self.gpu_job_extra
        final_job_extra.extend(job_extra)
        kwargs.update({"job_extra": final_job_extra})
        return kwargs

    def _update_kwargs_env_extra(self, **kwargs) -> Dict[str, Any]:
        if self.openmp_env_extra is None:
            return kwargs
        env_extra = kwargs.get("env_extra", self.get_kwarg("env-extra"))
        if not env_extra:
            env_extra = config.get(
                "jobqueue.{}.env_extra".format(self.scheduler_name), default=[]
            )
        # order matters, make sure user has power to be in control, explicit user set
        # stuff comes last
        final_env_extra = self.openmp_env_extra
        final_env_extra.extend(env_extra)
        kwargs.update({"env_extra": final_env_extra})
        return kwargs

    def _update_script_nodes(self, **kwargs) -> None:
        # If we're not in mpi_mode no need to do anything
        if not self.mpi_mode:
            return

        # When in MPI mode, after jobqueue has initialised we update the jobscript with
        # the `real` number of MPI tasks
        self._kwargs["mpi_tasks"] = self.mpi_tasks

        # The default for jobqueue is not to use an MPI launcher (since it is not MPI
        # aware). However, if self.fork_mpi=False then the tasks intended for this
        # cluster are MPI-enabled. In order to give them an MPI environment we need to
        # use our custom wrapper and launch with our MPI launcher
        if not self.fork_mpi:
            command_template = self._dummy_job._command_template
            dask_worker_module = "distributed.cli.dask_worker"
            if dask_worker_module in command_template:
                command_template = command_template.replace(
                    dask_worker_module, MPI_DASK_WRAPPER_MODULE
                )
            else:
                raise RuntimeError(
                    "Python module {} not found in command template:\n{}".format(
                        dask_worker_module, command_template
                    )
                )
            # The first part of the string is the python executable to use for the
            # worker
            python, arguments = command_template.split(" ", 1)

            # Wrap the launch command with our mpi wrapper

            # Make sure all appropriate kwargs are found and set
            mpi_kwargs = {}
            for attribute in ["mpi_tasks", "nodes", "cpus_per_task", "ntasks_per_node"]:
                try:
                    mpi_kwargs.update({attribute: getattr(self, attribute)})
                except AttributeError:
                    raise AttributeError(
                        "No attribute {} found in our custom class, this is needed to "
                        "wrap the MPI launch command in our job script".format(
                            attribute
                        )
                    )
            command_template = mpi_wrap(
                executable=python,
                exec_args=arguments,
                return_wrapped_command=True,
                **{**kwargs, **mpi_kwargs},
            )
            self.warnings.append(
                "Replaced command template\n\t{}\nwith\n\t{}\nin jobscript".format(
                    self._dummy_job._command_template, command_template
                )
            )
            self._kwargs["command_template"] = command_template


class CustomSLURMCluster(CustomClusterMixin, SLURMCluster):
    __doc__ = f"""Custom SLURMCluster class with CustomClusterMixin for initial kwargs tweak.

     Adds client attribute to SLURMCluster class.

     Attributes
     ----------
     {custom_cluster_attributes}
     """

    job_cls = CustomSLURMJob

    def __init__(self, **kwargs):
        self.scheduler_name = "slurm"
        kwargs = self.update_init_kwargs(**kwargs)
        self.validate_cluster_name(kwargs["name"])
        self.name = kwargs["name"]
        # Do custom initialisation here
        if self.mpi_mode:
            # Most obvious customisation is for when we use mpi_mode, relevant variables
            # are:
            # self.ntasks_per_node
            # self.cpus_per_task
            # self.nodes (optional)
            # self.mpi_tasks (total number of MPI tasks)
            if "job_cpu" in kwargs:
                raise ValueError(
                    "We don't allow 'job_cpu' as a kwarg in MPI mode since we"
                    " leverage this in jobqueue to set --cpus-per-task"
                )
            mpi_job_extra = ["--ntasks-per-node={}".format(self.ntasks_per_node)]
            # the kwarg is used in SLURMCluster to set  --cpus-per-task, don't duplicate
            kwargs.update({"job_cpu": self.cpus_per_task})

            # --nodes is optional
            if hasattr(self, "nodes"):
                if self.nodes:
                    mpi_job_extra.append("--nodes={}".format(self.nodes))
            # job_extra is guaranteed to exist in the kwargs in this case, we append
            # them so they have precedence
            if "job_extra" not in kwargs:
                raise KeyError("job_extra keyword should always be set in kwargs")
            mpi_job_extra.extend(kwargs["job_extra"])
            kwargs.update({"job_extra": mpi_job_extra})
        super().__init__(**kwargs)
        self._update_script_nodes(**kwargs)
        self.client: Client = Client(self)
        # Log all the warnings that we may have accumulated
        if self.warnings:
            logger.debug("Warnings generated by CustomSLURMCluster class instance:")
            for warning in self.warnings:
                logger.debug(warning)
            logger.debug("\n")
        self._add_to_cluster_controller()

    def __del__(self):
        with suppress(AttributeError):
            super().__del__()


class CustomPBSCluster(CustomClusterMixin, PBSCluster):
    __doc__ = f"""Custom PBS Cluster class.

    Attributes:
    ---------
    ngpus_per_node : int
        The number of gpus per node to be used
    {custom_cluster_attributes}
    """

    job_cls = CustomPBSJob

    def __init__(self, **kwargs):
        self.scheduler_name = "pbs"
        self.ngpus_per_node = kwargs.get("ngpus_per_node", 0)
        kwargs = self.update_init_kwargs(**kwargs)
        self.validate_cluster_name(kwargs["name"])
        self.name = kwargs["name"]
        if self.mpi_mode:
            if "job_cpu" in kwargs:
                raise ValueError(
                    "We don't allow 'job_cpu' as a kwarg in MPI mode since we"
                    " leverage this in jobqueue to set cpus-per-task"
                )
            mpi_job_extra = []
            if "job_extra" not in kwargs:
                raise KeyError("job_extra keyword should always be set in kwargs")
            mpi_job_extra.extend(kwargs["job_extra"])
            kwargs.update({"job_extra": mpi_job_extra})
        super().__init__(**kwargs)
        self._update_script_nodes(**kwargs)
        if hasattr(self, "mpi_tasks"):
            self._kwargs["mpi_tasks"] = self.mpi_tasks
        if self.ngpus_per_node > 0:
            self._kwargs["ngpus_per_node"] = self.ngpus_per_node
        self.client: Client = Client(self)
        # Log all the warnings that we may have accumulated
        if self.warnings:
            logger.debug("Warnings generated by CustomPBSCluster class instance:")
            for warning in self.warnings:
                logger.debug(warning)
            logger.debug("\n")
        self._add_to_cluster_controller()

    def __del__(self):
        with suppress(AttributeError):
            super().__del__()


ClusterType = TypeVar(
    "ClusterType", JobQueueCluster, LocalCluster, CustomSLURMCluster, CustomPBSCluster
)
