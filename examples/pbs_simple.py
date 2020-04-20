import time

from jobqueue_features.clusters import CustomPBSCluster
from jobqueue_features.decorators import on_cluster, task


cluster_1 = CustomPBSCluster(cores=1, memory='2 GB', queue='workq', name='pbs_1')
cluster_2 = CustomPBSCluster(cores=1, memory='2 GB', queue='workq', name='pbs_2')


@on_cluster(cluster=cluster_1, jobs=1)
@task(cluster_id=cluster_1.name)
def square(x):
    time.sleep(1)
    return x ** 2


@on_cluster(cluster=cluster_2, jobs=1)
@task(cluster_id=cluster_2.name)
def cube(x):
    time.sleep(1)
    return x ** 3


def main():
    sq_tasks = list(map(square, range(1, 11)))
    cb_tasks = list(map(cube, range(1, 11)))
    print('sq_tasks:', [t.result() for t in sq_tasks])
    print('cb_tasks:', [t.result() for t in cb_tasks])


if __name__ == "__main__":
    start = time.time()
    main()
    print("Executed in {}".format(time.time() - start))
