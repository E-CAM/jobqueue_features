from .clusters_controller import clusters_controller_singleton
from .clusters import ClusterType  # noqa


def set_default_cluster(cluster):
    # type: (ClusterType) -> None
    """Function sets default cluster type for clusters controller.
    This should be the right way of setting that."""
    clusters_controller_singleton.default_cluster = cluster
