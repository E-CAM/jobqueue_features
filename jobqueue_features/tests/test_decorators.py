from __future__ import absolute_import
from unittest import TestCase
from dask.distributed import Client, LocalCluster, Future

from jobqueue_features import on_cluster, set_default_cluster, task, ClusterException
from jobqueue_features.clusters_controller import clusters_controller_singleton


class TestDecorators(TestCase):
    def setUp(self):
        self.cluster_type = LocalCluster
        set_default_cluster(self.cluster_type)

        @on_cluster()
        @task()
        def example_function():
            return "test"

        self.function = example_function

    def tearDown(self):
        clusters_controller_singleton._close()

    def test_init(self):
        cluster, client = clusters_controller_singleton.get_cluster()
        self.assertIsInstance(cluster, self.cluster_type)
        self.assertIsInstance(client, Client)

    def test_call(self):
        r = self.function()
        self.assertIsInstance(r, Future)
        self.assertEqual(r.result(), "test")

    def test_bad_task(self):
        with self.assertRaises(ClusterException):

            @task(cluster_id="bad_ID")
            def bad_function():
                return "test"

            bad_function()
