from __future__ import print_function
import time

from jobqueue_features.decorators import on_cluster, task
from jobqueue_features.functions import set_default_cluster
from jobqueue_features.clusters import CustomSLURMCluster

# import logging
# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

# set_default_cluster(LocalCluster)  # set LocalCluster as default cluster type
set_default_cluster(CustomSLURMCluster)  # set CustomSLURMCluster as default cluster type


@on_cluster()
@task()
def square(x):
    return x ** 2


@on_cluster(cluster_id='other',
            cluster=CustomSLURMCluster(name='other', walltime='00:04:00'))
@task(cluster_id='other')
def inc(x):
    return x + 1


def main():
    sq_tasks = list(map(square, range(1, 11)))
    inc_tasks = list(map(inc, range(1, 11)))
    print([t.result() for t in sq_tasks])
    print([t.result() for t in inc_tasks])


if __name__ == '__main__':
    start = time.time()
    main()
    print('Executed in {}'.format(time.time() - start))

