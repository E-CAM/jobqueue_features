from __future__ import print_function

from typing import Callable, List, Dict  # noqa
from functools import wraps

from dask.distributed import LocalCluster, Client, Future  # noqa

from .clusters_controller import clusters_controller_singleton, ClusterType  # noqa
from .custom_exceptions import ClusterException
from .mpi_wrapper import (
    MPIEXEC,
    serialize_function_and_args,
    mpi_deserialize_and_execute,
)


def _get_workers_number(client):
    return len(client._scheduler_identity.get("workers", {}))


class on_cluster(object):
    """Decorator gets or create clusters from clusters controller.
    Parameters
    ----------
    cluster_id : str
        Gets or create cluster with given id. If no id provided controller
        gets default cluster (cluster_id = 'default').
    cluster : ClusterType
        Pass own cluster for given cluster_id.
    scale : int
        Scale cluster (default or given by id or passes by cluster parameter).
    """

    def __init__(self, cluster_id=None, cluster=None, scale=None):
        # type: (str, ClusterType, int) -> None
        try:
            self.cluster, client = clusters_controller_singleton.get_cluster(
                id_=cluster_id
            )  # type: ClusterType
        except ClusterException:
            self.cluster, client = clusters_controller_singleton.add_cluster(
                id_=cluster_id, cluster=cluster
            )  # type: ClusterType
        if type(self.cluster) is not LocalCluster:
            if scale is not None:
                # If the kwarg 'scale' has been used in the decorator call we adaptively
                # scale up to that many workers
                if scale > self.cluster.maximum_scale:
                    print(
                        "Scaling cluster {cluster_id} to {scale} exceeds default "
                        "maximum workers ({maximum_scale})".format(
                            cluster_id=cluster_id,
                            scale=scale,
                            maximum_scale=self.cluster.maximum_scale,
                        )
                    )
                    self.cluster.adapt(
                        minimum=0, maximum=scale, wait_count=10, interval="6s"
                    )
                else:
                    self.cluster.adapt(
                        minimum=0,
                        maximum=self.cluster.maximum_scale,
                        wait_count=10,
                        interval="6s",
                    )
                # We immediately start up `scale` workers rather than wait for adapt to
                # kick in the `wait_count`*`interval` should keep them from shutting
                # down too fast (allows 1 minute idle)
                self.cluster.scale(scale)
            else:
                # Otherwise we adaptively scale to the maximum number of workers
                self.cluster.adapt(minimum=0, maximum=self.cluster.maximum_scale)

    def __call__(self, f):
        # type: (Callable) -> Callable
        @wraps(f)
        def wrapped_function(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped_function


class task(object):
    """Decorator gets client from clusters controller and submits
    wrapped function to given cluster.
    Parameters
    ----------
    cluster_id : str
        Gets client of given cluster by id or 'default' if none given.
    """

    def __init__(self, cluster_id=None):
        # type: (str) -> None
        self.cluster_id = cluster_id

    def __call__(self, f):
        # type: (Callable) -> Callable
        @wraps(f)
        def wrapped_f(*args, **kwargs):  # type: (...) -> Future
            cluster, client = self._get_cluster_and_client()
            return self._submit(cluster, client, f, *args, **kwargs)

        return wrapped_f

    def _get_cluster_and_client(self):
        try:
            return clusters_controller_singleton.get_cluster(id_=self.cluster_id)
        except KeyError:
            raise ClusterException(
                "Could not find Cluster or Client. " "Use @on_cluster() decorator."
            )

    def _submit(self, cluster, client, f, *args, **kwargs):
        # type: (ClusterType, Client, Callable, List[...], Dict[...]) -> Future
        # For normal tasks, we maintain the Dask default that functions are pure (by
        # default)
        kwargs.update({"pure": getattr(cluster, "pure", getattr(kwargs, "pure", True))})
        return client.submit(f, *args, **kwargs)


class mpi_task(task):
    def __init__(self, cluster_id=None, default_mpi_tasks=1):
        # type: (str, int) -> None
        # set a default number of MPI tasks (in case we are running on a localcluster)
        self.default_mpi_tasks = default_mpi_tasks
        super(mpi_task, self).__init__(cluster_id=cluster_id)

    def _submit(self, cluster, client, f, *args, **kwargs):
        # Check if it is a forking task
        fork_mpi = getattr(cluster, "fork_mpi", getattr(kwargs, "fork_mpi", False))
        # type: (ClusterType, Client, Callable, List[...], Dict[...]) -> Future
        if fork_mpi:
            kwargs.update(
                {
                    "mpi_launcher": getattr(
                        cluster,
                        "mpi_launcher",
                        getattr(kwargs, "mpi_launcher", MPIEXEC),
                    ),
                    "mpi_tasks": getattr(
                        cluster,
                        "mpi_tasks",
                        getattr(kwargs, "mpi_tasks", self.default_mpi_tasks),
                    ),
                    "nodes": getattr(cluster, "nodes", getattr(kwargs, "nodes", None)),
                    "cpus_per_task": getattr(
                        cluster, "cpus_per_task", getattr(kwargs, "cpus_per_task", None)
                    ),
                    "ntasks_per_node": getattr(
                        cluster,
                        "ntasks_per_node",
                        getattr(kwargs, "ntasks_per_node", None),
                    ),
                }
            )
        # For MPI tasks, since we are forking a process let's assume functions are not
        # pure (by default)
        kwargs.update(
            {"pure": getattr(cluster, "pure", getattr(kwargs, "pure", False))}
        )
        if fork_mpi:
            return super(mpi_task, self)._submit(cluster, client, f, *args, **kwargs)
        else:
            # If we are not forking we need to serialize the task and arguments
            serialized_object = serialize_function_and_args(f, *args, **kwargs)
            # Then we submit our deserializing/executing function as the task
            return super(mpi_task, self)._submit(
                cluster, client, mpi_deserialize_and_execute, serialized_object
            )
