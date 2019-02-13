from __future__ import print_function
import time

from dask.distributed import LocalCluster

from jobqueue_features.decorators import on_cluster, task
from jobqueue_features.functions import set_default_cluster

# import logging
# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

set_default_cluster(LocalCluster)  # set LocalCluster as default cluster type


@task()
def square(x):
    return x ** 2


@on_cluster()
def main():
    sq_tasks = list(map(square, range(1, 11)))
    print([t.result() for t in sq_tasks])


if __name__ == "__main__":
    start = time.time()
    main()
    print("Executed in {}".format(time.time() - start))
