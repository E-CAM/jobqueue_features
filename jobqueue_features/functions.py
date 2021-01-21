import inspect

from typing import Callable, List, TYPE_CHECKING

from .clusters_controller import clusters_controller_singleton

if TYPE_CHECKING:
    from .clusters import ClusterType  # noqa


def set_default_cluster(cluster: "ClusterType") -> None:
    """Function sets default cluster type for clusters controller.
    This should be the right way of setting that."""
    clusters_controller_singleton.default_cluster = cluster


def get_callable_args(func: Callable, remove_self: bool = True) -> List[str]:
    """Inspects and returns names of arguments. If `remove_self` is true
    then removes `self` argument from list"""
    args_spec = inspect.getfullargspec(func)
    args = args_spec.args + args_spec.kwonlyargs
    if remove_self and "self" in args:
        args.remove("self")
    return args
