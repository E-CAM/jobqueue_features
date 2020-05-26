from unittest import TestCase

from jobqueue_features import MPIEXEC
from jobqueue_features.clusters import CustomPBSCluster
from jobqueue_features.clusters_controller import (
    clusters_controller_singleton as controller,
)


class TestPBS(TestCase):
    def setUp(self):
        # Kill any existing clusters
        controller._close()
        self.cluster_class = CustomPBSCluster

    def test_default_creation(self):
        controller._close()
        job_name = "test_pbs"
        cluster = self.cluster_class(memory="2 GB", name=job_name)
        job_script = cluster.job_script()
        self.assertEqual(cluster.name, job_name)
        self.assertEqual(cluster.cores_per_node, 1)
        self.assertEqual(cluster.hyperthreading_factor, 1)
        self.assertEqual(cluster.mpi_mode, False)
        self.assertEqual(cluster.mpi_mode, False)
        self.assertIn(f"#PBS -N {job_name}", job_script)
        self.assertIn(f"#PBS -l select=1:ncpus=1", job_script)
        self.assertIn(f"--nthreads 1", job_script)
        controller._close()

    def test_cores(self):
        controller._close()
        job_name = "test_pbs"
        with self.assertRaises(ValueError):
            self.cluster_class(
                memory="2 GB", name=job_name, cores=2,
            )
        cluster = self.cluster_class(
            memory="2 GB", name=job_name, cores=2, cores_per_node=2,
        )
        job_script = cluster.job_script()
        self.assertEqual(cluster.cores_per_node, 2)
        self.assertIn(f"#PBS -l select=1:ncpus=2", job_script)
        self.assertIn(f"--nprocs 2", job_script)
        controller._close()

    def test_nodes(self):
        controller._close()
        job_name = "test_pbs"
        with self.assertRaises(ValueError):
            self.cluster_class(
                memory="2 GB", name=job_name, cores=2, nodes=2,
            )
        with self.assertRaises(ValueError):
            self.cluster_class(
                memory="2 GB", name=job_name, mpi_mode=True,
            )
        with self.assertRaises(ValueError):
            self.cluster_class(
                memory="2 GB", name=job_name, mpi_mode=True, mpi_launcher=MPIEXEC,
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
        controller._close()
        ntasks_per_node *= 2
        cluster = self.cluster_class(
            memory="2 GB",
            name=job_name,
            mpi_mode=True,
            mpi_launcher=MPIEXEC,
            cores_per_node=cores_per_node,
            nodes=nodes,
            ntasks_per_node=ntasks_per_node,
            hyperthreading_factor=2,
        )
        job_script = cluster.job_script()
        print(job_script)
        self.assertIn(
            f"#PBS -l select={nodes}:"
            f"ncpus={cores_per_node}:"
            f"mpiprocs={ntasks_per_node}",
            job_script,
        )
        controller._close()
