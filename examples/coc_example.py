from __future__ import print_function
import time

from jobqueue_features.clusters import CustomSLURMCluster
from jobqueue_features.decorators import task, on_cluster

# import logging
# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

def inc(x):
    time.sleep(1)
    return x + 1


def mul(x):
    time.sleep(1)
    return x * 2


@on_cluster(
    cluster_id='inc-cluster',
    cluster=CustomSLURMCluster(name='inc-cluster', walltime='00:04:00'),
    scale=2
)
@task(cluster_id='inc-cluster')
def increment_task(x):
    return inc(x)


@on_cluster(cluster_id='mul-cluster',
            cluster=CustomSLURMCluster(name='mul-cluster', walltime='00:04:00'),
            scale=2)
@task(cluster_id='mul-cluster')
def multiplication_task(x):
    return mul(x)


#@on_cluster()
def main():
    task1 = list(map(multiplication_task, range(10)))
    task2 = list(map(increment_task, range(10)))
    print([t.result() for t in task1])
    print([t.result() for t in task2])


if __name__ == '__main__':
    start = time.time()
    main()
    print('Executed in {}'.format(time.time() - start))

