from typing import Tuple, Dict, Callable, TYPE_CHECKING  # noqa
import atexit

from dask.distributed import Client, LocalCluster
from .custom_exceptions import ClusterException

if TYPE_CHECKING:
    from .clusters import ClusterType  # noqa

_DEFAULT = "default"


class ClusterController(object):
    """Controller keeps collection of clusters and clients so it can
    provide these for decorators to submitting tasks."""

    default_cluster: Callable[..., "ClusterType"] = LocalCluster

    def __init__(self):
        self._clusters: Dict[str, "ClusterType"] = {_DEFAULT: None}
        self._clients: Dict[str, Client] = {_DEFAULT: None}
        atexit.register(self._close)

    def get_cluster(self, id_: str = None) -> Tuple["ClusterType", Client]:
        cluster = self._clusters.get(id_ or _DEFAULT)
        if cluster is None:
            raise ClusterException('No cluster "{}" set!'.format(id_))
        client = self._clients.get(id_ or _DEFAULT)
        if client is None:
            raise ClusterException('No client for cluster "{}" set!'.format(id_))
        return cluster, client

    def add_cluster(
        self, id_: str = None, cluster: "ClusterType" = None
    ) -> Tuple["ClusterType", Client]:
        if hasattr(cluster, "name"):
            if id_ is None:
                id_ = cluster.name
            else:
                assert id_ == cluster.name
        return self._make_cluster(id_=id_ or _DEFAULT, cluster=cluster)

    def delete_cluster(self, id_: str) -> None:
        self._close_client(id_=id_)
        self._close_cluster(id_=id_)

    def _make_cluster(
        self, id_: str, cluster: "ClusterType" = None
    ) -> Tuple["ClusterType", Client]:
        if id_ != _DEFAULT and id_ in self._clusters:
            raise ClusterException('Cluster "{}" already exists!'.format(id_))
        self._clusters[id_] = cluster or self._make_default_cluster(name=id_)
        self._make_client(id_=id_)
        return self._clusters[id_], self._clients[id_]

    def _make_default_cluster(self, name: str) -> "ClusterType":
        kwargs = {}
        if self.default_cluster is not LocalCluster:
            kwargs["name"] = name
        else:
            kwargs["processes"] = False
        return self.default_cluster(**kwargs)

    def _make_client(self, id_: str) -> None:
        cluster = self._clusters[id_]
        if hasattr(cluster, "client"):
            client = cluster.client
        else:
            client = Client(cluster)
        self._clients[id_] = client

    def _close(self) -> None:
        self._close_clients()
        self._close_clusters()

    def _close_cluster(self, id_: str) -> None:
        cluster = self._clusters.pop(id_, None)
        if cluster and cluster.status != "closed":
            cluster.close()

    def _close_clusters(self) -> None:
        for id_ in list(self._clusters.keys()):
            self._close_cluster(id_)
        self._clusters = {_DEFAULT: None}

    def _close_client(self, id_: str) -> None:
        client = self._clients.get(id_)
        if client:
            client.close()

    def _close_clients(self) -> None:
        for id_ in list(self._clients.keys()):
            self._close_client(id_)
        self._clients = {_DEFAULT: None}


clusters_controller_singleton = ClusterController()
