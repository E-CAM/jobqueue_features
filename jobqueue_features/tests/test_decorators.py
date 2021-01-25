from __future__ import absolute_import

from unittest import TestCase

import psutil
from dask.distributed import Client, LocalCluster, Future

from jobqueue_features import on_cluster, set_default_cluster, task, ClusterException
from jobqueue_features.clusters import CustomSLURMCluster
from jobqueue_features.clusters_controller import (
    clusters_controller_singleton as controller,
)


class TweakedCustomCluster(CustomSLURMCluster):
    def __init__(self, **kwargs):
        kwargs["interface"] = list(psutil.net_if_addrs().keys())[0]
        kwargs["memory"] = "1GB"
        super(TweakedCustomCluster, self).__init__(**kwargs)


# monkey patch the CustomSLURMCluster so it can be initialized without providing
# the "interface" attribute
CustomSLURMCluster = TweakedCustomCluster


class TestOnClusterDecorator(TestCase):
    """
    Test @on_cluster() decorator against several scenarios of providing clusters or clusters
    IDs to the initializer of decorator for getting/adding clusters from cluster controller.

    cluster=None,         cluster_id=None : default cluster
    cluster=SLURMCluster, cluster_id=None : register cluster by its name
    cluster=LocalCluster, cluster_id=None : error
    cluster=None,         cluster_id=str  : when default cluster is LocalCLuster check for cluster with such id
    cluster=None,         cluster_id=str  : when default cluster is SLURMCluster check for cluster with such id
    cluster=SLURMCluster, cluster_id=str  : assert cluster.name == id
    cluster=LocalCluster, cluster_id=str  : register cluster by given id
    """

    def tearDown(self):
        controller._close()

    def test_default_behavior(self):
        self._test_default_behavior(cluster_type=CustomSLURMCluster)

    def test_default_behavior_local(self):
        self._test_default_behavior(cluster_type=LocalCluster)

    def test_init_only_cluster(self):
        original_cluster = CustomSLURMCluster()

        @on_cluster(cluster=original_cluster)
        def f():
            pass

        cluster, client = controller.get_cluster(original_cluster.name)
        self.assertEqual(original_cluster, cluster)
        self.assertIsInstance(client, Client)
        controller.delete_cluster(cluster.name)

        _id = "test1"
        original_cluster = CustomSLURMCluster(name=_id)

        @on_cluster(cluster=original_cluster)
        def f():
            pass

        cluster, client = controller.get_cluster(_id)
        self.assertEqual(original_cluster, cluster)
        self.assertIsInstance(client, Client)
        controller.delete_cluster(cluster.name)

    def test_init_only_cluster_local(self):
        original_cluster = LocalCluster()
        with self.assertRaises(ClusterException) as ctx:

            @on_cluster(cluster=original_cluster)
            def f():
                pass

        self.assertEqual(
            'LocalCluster requires "cluster_id" argument.', str(ctx.exception)
        )

        _id = original_cluster.name

        @on_cluster(cluster=original_cluster, cluster_id=_id)
        def f():
            pass

        cluster, client = controller.get_cluster(_id)
        self.assertEqual(original_cluster, cluster)
        self.assertIsInstance(client, Client)
        controller.delete_cluster(_id)

    def test_init_only_id(self):
        _id = "test1"
        original_cluster = CustomSLURMCluster(name=_id)
        with self.assertRaises(ClusterException) as ctx:
            controller.add_cluster(cluster=original_cluster)
        self.assertEqual('Cluster "{}" already exists!'.format(_id), str(ctx.exception))

        @on_cluster(cluster_id=_id)
        def f():
            pass

        cluster, client = controller.get_cluster(id_=_id)
        self.assertEqual(original_cluster, cluster)
        self.assertEqual(cluster.name, _id)
        self.assertIsInstance(client, Client)
        controller.delete_cluster(_id)

    def test_init_only_id_local(self):
        _id = "test1"
        original_cluster = LocalCluster(name="test1")
        controller.add_cluster(id_=_id, cluster=original_cluster)

        @on_cluster(cluster_id=_id)
        def f():
            pass

        cluster, client = controller.get_cluster(id_=_id)
        self.assertEqual(original_cluster, cluster)
        self.assertIsInstance(client, Client)
        controller.delete_cluster(_id)

    def test_init_both_cluster_and_id(self):
        _id = "test1"
        original_cluster = CustomSLURMCluster()

        with self.assertRaises(AssertionError):

            @on_cluster(cluster_id=_id, cluster=original_cluster)
            def f():
                pass

        controller.delete_cluster(original_cluster.name)
        original_cluster = CustomSLURMCluster()

        @on_cluster(cluster_id=original_cluster.name, cluster=original_cluster)
        def f():
            pass

        cluster, client = controller.get_cluster(id_=original_cluster.name)
        self.assertEqual(original_cluster, cluster)
        self.assertEqual(cluster.name, original_cluster.name)
        self.assertIsInstance(client, Client)
        controller.delete_cluster(original_cluster.name)

        original_cluster = CustomSLURMCluster(name=_id)

        @on_cluster(cluster_id=_id, cluster=original_cluster)
        def f():
            pass

        cluster, client = controller.get_cluster(id_=_id)
        self.assertEqual(original_cluster, cluster)
        self.assertEqual(cluster.name, _id)
        self.assertIsInstance(client, Client)
        controller.delete_cluster(_id)

    def _test_default_behavior(self, cluster_type):
        set_default_cluster(cluster_type)

        @on_cluster()
        def f():
            pass

        cluster, client = controller.get_cluster()
        self.assertIsInstance(cluster, cluster_type)
        self.assertIsInstance(client, Client)


class TestTaskDecorator(TestCase):
    def tearDown(self):
        controller._close()

    def test_default_behavior(self):
        cluster_type = CustomSLURMCluster
        set_default_cluster(cluster_type)
        _value = 123

        @on_cluster()
        @task()
        def f():
            return _value

        self._basic_tests(cluster_type)

    def test_task_by_id_string(self):
        _id = "test1"
        cluster_type = CustomSLURMCluster
        original_cluster = cluster_type(name=_id)

        @on_cluster(cluster=original_cluster)
        @task(cluster_id=_id)
        def f():
            pass

        self._basic_tests(cluster_type=cluster_type, id_=_id)
        controller.delete_cluster(_id)

    def test_task_by_cluster_name(self):
        _id = "test1"
        cluster_type = CustomSLURMCluster
        original_cluster = cluster_type(name=_id)

        @on_cluster(cluster=original_cluster)
        @task(cluster_id=original_cluster.name)
        def f():
            pass

        self._basic_tests(cluster_type=cluster_type, id_=original_cluster.name)
        controller.delete_cluster(original_cluster.name)

    def test_task_by_cluster(self):
        cluster_type = CustomSLURMCluster
        original_cluster = cluster_type()

        @on_cluster(cluster=original_cluster)
        @task(cluster=original_cluster)
        def f():
            pass

        self._basic_tests(cluster_type=cluster_type, id_=original_cluster.name)
        controller.delete_cluster(original_cluster.name)

    def test_task_by_cluster_local(self):
        _id = "test1"
        cluster_type = LocalCluster
        original_cluster = cluster_type()

        with self.assertRaises(ClusterException) as ctx:

            @on_cluster(cluster=original_cluster, cluster_id=_id)
            @task(cluster=original_cluster)
            def f():
                pass

        self.assertEqual(
            "'cluster_id' argument is required for LocalCluster.", str(ctx.exception)
        )

        @on_cluster(cluster=original_cluster, cluster_id=_id)
        @task(cluster=original_cluster, cluster_id=_id)
        def f():
            pass

        self._basic_tests(cluster_type=cluster_type, id_=_id)
        controller.delete_cluster(_id)

    def test_task_by_cluster_and_cluster_id(self):
        _id = "test1"
        cluster_type = CustomSLURMCluster
        original_cluster = cluster_type(name=_id)

        with self.assertRaises(ClusterException) as ctx:

            @on_cluster(cluster=original_cluster)
            @task(cluster=original_cluster, cluster_id="bad name")
            def f():
                pass

        self.assertEqual(
            "Cluster 'name' and cluster_id are different.", str(ctx.exception)
        )

        @on_cluster(cluster=original_cluster)
        @task(cluster=original_cluster, cluster_id=_id)
        def f():
            pass

        self._basic_tests(cluster_type=cluster_type, id_=original_cluster.name)
        controller.delete_cluster(original_cluster.name)

    def test_task_by_cluster_and_cluster_id_local(self):
        _id = "test1"
        cluster_type = LocalCluster
        original_cluster = cluster_type()

        @on_cluster(cluster=original_cluster, cluster_id=_id)
        @task(cluster=original_cluster, cluster_id="_id")
        def f():
            pass

        self._basic_tests(cluster_type=cluster_type, id_=_id)
        controller.delete_cluster(id_=_id)

    def _basic_tests(self, cluster_type, id_=None):
        cluster, client = controller.get_cluster(id_)
        self.assertIsInstance(cluster, cluster_type)
        self.assertIsInstance(client, Client)
        self.assertEqual(controller.default_cluster.__class__, cluster_type.__class__)


class TestTaskExecutionLocalCluster(TestCase):
    def setUp(self):
        self.cluster_type = LocalCluster
        set_default_cluster(self.cluster_type)

    def tearDown(self):
        controller._close()

    def test_init_blank(self):
        @on_cluster()
        def f():
            pass

        cluster, client = controller.get_cluster()
        self.assertIsInstance(cluster, self.cluster_type)
        self.assertIsInstance(client, Client)

    def test_call(self):
        @on_cluster()
        @task()
        def f():
            return "test"

        r = f()
        self.assertIsInstance(r, Future)
        self.assertEqual(r.result(), "test")

    def test_bad_task(self):
        _id = "bad_ID"
        with self.assertRaises(ClusterException) as ctx:

            @task(cluster_id=_id)
            def bad_function():
                return "test"

            bad_function()
        self.assertEqual('No cluster "{}" set!'.format(_id), str(ctx.exception))
