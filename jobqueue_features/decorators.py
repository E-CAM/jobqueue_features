from __future__ import print_function

from typing import Callable, List, Dict, TYPE_CHECKING, Optional, Tuple, Any  # noqa
from functools import wraps

from dask.distributed import LocalCluster, Client, Future  # noqa

from .clusters_controller import clusters_controller_singleton

if TYPE_CHECKING:
    from .clusters_controller import ClusterType  # noqa
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
    cluster : ClusterType
        Work on given cluster
    cluster_id : str
        Gets or create cluster with given id. If no id provided controller
        gets default cluster (cluster_id = 'default').
    jobs : int
        Scale cluster to given number of jobs (default or given by id or passes
        by cluster parameter).
    """

    def __init__(
        self,
        cluster: Optional["ClusterType"] = None,
        cluster_id: Optional[str] = None,
        jobs: Optional[int] = None,
    ) -> None:
        _id = self._get_cluster_id(cluster=cluster, cluster_id=cluster_id)
        try:
            self.cluster, client = clusters_controller_singleton.get_cluster(
                id_=_id
            )  # type: ClusterType
        except ClusterException:
            self.cluster, client = clusters_controller_singleton.add_cluster(
                id_=_id, cluster=cluster
            )  # type: ClusterType
        if not self._is_local_cluster(cluster=self.cluster):
            minimum_jobs = self.cluster.minimum_jobs
            maximum_jobs = self.cluster.maximum_jobs
            if jobs is not None:
                # If the kwarg 'jobs' has been used in the decorator call we adaptively
                # scale up to that many jobs and set appropriate limits on adapt()
                if jobs > minimum_jobs:
                    minimum_jobs = jobs
                if jobs > maximum_jobs:
                    print(
                        "Scaling cluster {cluster_id} to {jobs} jobs exceeds default "
                        "maximum jobs ({maximum_jobs})...but I am doing it "
                        "anyway".format(
                            cluster_id=_id,
                            jobs=jobs,
                            maximum_jobs=maximum_jobs,
                        )
                    )
                    maximum_jobs = jobs

            # Adaptively scale between the requested limits
            self.cluster.adapt(minimum_jobs=minimum_jobs, maximum_jobs=maximum_jobs)

    def __call__(self, f: Callable) -> Callable:
        @wraps(f)
        def wrapped_function(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped_function

    @staticmethod
    def _is_local_cluster(cluster: "ClusterType") -> bool:
        return type(cluster) is LocalCluster

    def _get_cluster_id(
        self,
        cluster: Optional["ClusterType"] = None,
        cluster_id: Optional[str] = None,
    ) -> str:
        if cluster:
            if not self._is_local_cluster(cluster=cluster):
                if cluster_id is not None:
                    assert cluster_id == cluster.name
                    return cluster_id
                else:
                    return cluster.name
            else:
                if cluster_id is None:
                    raise ClusterException(
                        'LocalCluster requires "cluster_id" argument.'
                    )
                return cluster_id
        return cluster_id


class task(object):
    """Decorator gets client from clusters controller and submits
    wrapped function to given cluster.
    Parameters
    ----------
    cluster : ClusterType

    cluster_id : str
        Gets client of given cluster by id or 'default' if none given.
    """

    def __init__(
        self,
        cluster_id: Optional[str] = None,
        cluster: Optional["ClusterType"] = None,
    ) -> None:
        self.cluster_id = cluster_id
        if cluster:
            _id = getattr(cluster, "name", None)
            if not _id:
                raise ClusterException("Cluster has no name attribute set.")
            elif cluster_id and _id != cluster_id:
                raise ClusterException("Cluster 'name' and cluster_id are different.")
            else:
                self.cluster_id = _id

    def __call__(self, f: Callable) -> Callable:
        @wraps(f)
        def wrapped_f(*args, **kwargs) -> Future:
            cluster, client = self._get_cluster_and_client()
            return self._submit(cluster, client, f, *args, **kwargs)

        return wrapped_f

    def _get_cluster_and_client(self) -> Tuple["ClusterType", Client]:
        try:
            return clusters_controller_singleton.get_cluster(id_=self.cluster_id)
        except KeyError:
            raise ClusterException(
                "Could not find Cluster or Client. " "Use @on_cluster() decorator."
            )

    def _submit(
        self, cluster: "ClusterType", client: Client, f: Callable, *args, **kwargs
    ) -> Future:
        # For normal tasks, we maintain the Dask default that functions are pure (by
        # default)
        kwargs.update({"pure": getattr(cluster, "pure", getattr(kwargs, "pure", True))})
        return client.submit(f, *args, **kwargs)


class mpi_task(task):
    def __init__(
        self,
        cluster_id: Optional[str] = None,
        cluster: Optional["ClusterType"] = None,
        default_mpi_tasks: int = 1,
    ) -> None:
        # set a default number of MPI tasks (in case we are running on a localcluster)
        self.default_mpi_tasks = default_mpi_tasks
        super(mpi_task, self).__init__(cluster_id=cluster_id, cluster=cluster)

    def _get_cluster_attribute(
        self,
        cluster: "ClusterType",
        attribute: str,
        default: Any,
        **kwargs,
    ) -> Tuple[Any, Dict[Any, Any]]:
        # A kwarg wins over an attribute, then fall back to default
        return_value = None
        if attribute in kwargs:
            return_value = kwargs.pop(attribute)
        if return_value is None:
            return_value = getattr(cluster, attribute, default)

        return return_value, kwargs

    def _submit(
        self,
        cluster: "ClusterType",
        client: Client,
        f: Callable,
        *args,
        **kwargs,
    ) -> Future:
        # For MPI tasks, let's assume functions are not pure (by default)
        pure, kwargs = self._get_cluster_attribute(cluster, "pure", False, **kwargs)
        # For a LocalCluster, mpi_mode/fork_mpi will not have been set so let's assume
        # MPI forking
        mpi_mode, kwargs = self._get_cluster_attribute(
            cluster, "mpi_mode", None, **kwargs
        )
        if mpi_mode is None:
            fork_mpi = True
        else:
            # Check if it is a forking task
            fork_mpi, kwargs = self._get_cluster_attribute(
                cluster, "fork_mpi", False, **kwargs
            )

        if fork_mpi:
            # Add a set of kwargs that define the job layout to be picked up by
            # our mpi_wrap function
            mpi_launcher, kwargs = self._get_cluster_attribute(
                cluster, "mpi_launcher", MPIEXEC, **kwargs
            )
            mpi_tasks, kwargs = self._get_cluster_attribute(
                cluster, "mpi_tasks", self.default_mpi_tasks, **kwargs
            )
            nodes, kwargs = self._get_cluster_attribute(
                cluster, "nodes", None, **kwargs
            )
            cpus_per_task, kwargs = self._get_cluster_attribute(
                cluster, "cpus_per_task", None, **kwargs
            )
            ntasks_per_node, kwargs = self._get_cluster_attribute(
                cluster, "ntasks_per_node", None, **kwargs
            )

            executable = kwargs.get("executable", None)
            if executable:
                f.__name__ = self.cluster_id + "/" + executable
            return super(mpi_task, self)._submit(
                cluster,
                client,
                f,
                *args,
                pure=pure,
                mpi_launcher=mpi_launcher,
                mpi_tasks=mpi_tasks,
                nodes=nodes,
                cpus_per_task=cpus_per_task,
                ntasks_per_node=ntasks_per_node,
                **kwargs,
            )
        else:
            # If we are not forking we need to serialize the task and arguments
            serialized_object = serialize_function_and_args(f, *args, **kwargs)

            mpi_deserialize_and_execute.__name__ = self.cluster_id + "/" + f.__name__
            # Then we submit our deserializing/executing function as the task
            return super(mpi_task, self)._submit(
                cluster,
                client,
                mpi_deserialize_and_execute,
                serialized_object,
                pure=pure,
            )
