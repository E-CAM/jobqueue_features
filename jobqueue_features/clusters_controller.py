from typing import Tuple, Dict, Callable  # noqa
import atexit

from dask.distributed import Client, LocalCluster

from .clusters import ClusterType  # noqa
from .custom_exceptions import ClusterException


class ClusterController(object):
    """Controller keeps collection of clusters and clients so it can
    provide these for decorators to submitting tasks."""
    default_cluster = LocalCluster  # type: Callable[..., ClusterType]

    def __init__(self):
        # type: () -> None
        self._clusters = {'default': None}  # type: Dict[str, ClusterType]
        self._clients = {'default': None}  # type: Dict[str, Client]
        atexit.register(self._close)

    def get_cluster(self, id_=None):
        # type: (str) -> Tuple[ClusterType, Client]
        cluster = self._clusters.get(id_ or 'default')
        if cluster is None:
            raise ClusterException('No cluster "{}" set!'.format(id_))
        client = self._clients.get(id_ or 'default')
        if client is None:
            raise ClusterException('No client for cluster "{}" set!'.format(id_))
        return cluster, client

    def add_cluster(self, id_=None, cluster=None):
        # type: (str, ClusterType) -> Tuple[ClusterType, Client]
        return self._make_cluster(id_=id_ or 'default', cluster=cluster)

    def _make_cluster(self, id_, cluster=None):
        # type: (str, ClusterType) -> Tuple[ClusterType, Client]
        if id_ != 'default' and id_ in self._clusters:
            raise ClusterException('Cluster "{}" already exists!'.format(id_))
        self._clusters[id_] = cluster or self._make_default_cluster(name=id_)
        self._make_client(id_=id_)
        return self._clusters[id_], self._clients[id_]

    def _make_default_cluster(self, name):
        kwargs = {}
        if self.default_cluster is not LocalCluster:
            kwargs['name'] = name
        return self.default_cluster(**kwargs)

    def _make_client(self, id_):
        # type: (str) -> None
        cluster = self._clusters[id_]
        if hasattr(cluster, 'client'):
            client = cluster.client
        else:
            client = Client(cluster)
        self._clients[id_] = client

    def _close(self):
        # type: () -> None
        self._close_clients()
        self._close_clusters()

    def _close_clusters(self):
        # type: () -> None
        for cluster in self._clusters.values():
            if cluster and type(cluster) is not LocalCluster:
                cluster.close()
        self._clusters = {'default': None}

    def _close_clients(self):
        # type: () -> None
        for client in self._clients.values():
            if client:
                client.close()
        self._clients = {'default': None}


clusters_controller_singleton = ClusterController()
