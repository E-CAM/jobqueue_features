from unittest import TestCase

from jobqueue_features import MPIEXEC
from jobqueue_features.clusters import CustomPBSCluster
from jobqueue_features.clusters_controller import (
    clusters_controller_singleton as controller,
)

# import logging
#
# logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)


class TestPBS(TestCase):
    def setUp(self):
        # Kill any existing clusters
        controller._close()
        self.cluster_class = CustomPBSCluster

    def tearDown(self):
        controller._close()

    def test_default_creation(self):
        job_name = "test_pbs"
        cluster = self.cluster_class(memory="2 GB", name=job_name)
        job_script = cluster.job_script()
        self.assertEqual(cluster.name, job_name)
        self.assertEqual(cluster.cores_per_node, 1)
        self.assertEqual(cluster.hyperthreading_factor, 1)
        self.assertEqual(cluster.mpi_mode, False)
        self.assertEqual(cluster.mpi_mode, False)
        self.assertIn(f"#PBS -N {job_name}", job_script)
        self.assertIn("#PBS -l select=1:ncpus=1", job_script)
        self.assertIn("--nthreads 1", job_script)

    def test_cores(self):
        job_name = "test_pbs"
        with self.assertRaises(ValueError):
            self.cluster_class(
                memory="2 GB",
                name=job_name,
                cores=2,
            )
        cluster = self.cluster_class(
            memory="2 GB",
            name=job_name,
            cores=2,
            cores_per_node=2,
        )
        job_script = cluster.job_script()
        self.assertEqual(cluster.cores_per_node, 2)
        self.assertIn(f"#PBS -l select=1:ncpus=2", job_script)
        self.assertIn(f"--nprocs 2", job_script)

    def test_nodes(self):
        job_name = "test_pbs"
        with self.assertRaises(ValueError):
            self.cluster_class(
                memory="2 GB",
                name=job_name,
                cores=2,
                nodes=2,
            )
        with self.assertRaises(ValueError):
            self.cluster_class(
                memory="2 GB",
                name=job_name,
                mpi_mode=True,
            )
        with self.assertRaises(ValueError):
            self.cluster_class(
                memory="2 GB",
                name=job_name,
                mpi_mode=True,
                mpi_launcher=MPIEXEC,
            )
        cores_per_node = 6
        nodes = 2
        ntasks_per_node = 6
        cluster = self.cluster_class(
            memory="2 GB",
            name=job_name,
            mpi_mode=True,
            mpi_launcher=MPIEXEC,
            cores_per_node=cores_per_node,
            nodes=nodes,
            ntasks_per_node=ntasks_per_node,
        )
        job_script = cluster.job_script()
        self.assertIn(
            f"#PBS -l select={nodes}:"
            f"ncpus={cores_per_node}:"
            f"mpiprocs={ntasks_per_node}",
            job_script,
        )

    def test_mpiprocs(self):
        job_name = "test_pbs"
        cores_per_node = 5
        nodes = 3
        ntasks_per_node = 5
        cpus_per_task = 2
        cluster = self.cluster_class(
            memory="2 GB",
            name=job_name,
            mpi_mode=True,
            mpi_launcher=MPIEXEC,
            cores_per_node=cores_per_node,
            nodes=nodes,
            ntasks_per_node=ntasks_per_node,
            cpus_per_task=cpus_per_task,
            hyperthreading_factor=2,
        )
        job_script = cluster.job_script()
        self.assertIn(
            f"#PBS -l select={nodes}:"
            f"ncpus={cores_per_node}:"
            f"mpiprocs={ntasks_per_node}:"
            f"ompthreads={cpus_per_task}",
            job_script,
        )

    def test_gpus(self):
        job_name = "test_pbs"
        cores_per_node = 5
        nodes = 3
        ntasks_per_node = 5
        gpus = 1
        cluster = self.cluster_class(
            memory="2 GB",
            name=job_name,
            mpi_mode=True,
            mpi_launcher=MPIEXEC,
            cores_per_node=cores_per_node,
            nodes=nodes,
            ntasks_per_node=ntasks_per_node,
            ngpus_per_node=1,
        )
        job_script = cluster.job_script()
        self.assertIn(
            f"#PBS -l select={nodes}:"
            f"ncpus={cores_per_node}:"
            f"mpiprocs={ntasks_per_node}:"
            f"ngpus={gpus}",
            job_script,
        )
