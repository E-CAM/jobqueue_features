from unittest import TestCase
from dask.distributed import Client

from jobqueue_features.clusters import get_cluster, SLURM
from jobqueue_features.mpi_wrapper import SRUN
from jobqueue_features.cli.mpi_dask_worker import MPI_DASK_WRAPPER_MODULE
from jobqueue_features.clusters_controller import (
    clusters_controller_singleton as controller,
)


class TestClusters(TestCase):
    def setUp(self):
        self.cluster_name = "dask-worker-batch"  # default
        self.kwargs = {
            # 'interface': 'eth0',  # most likely won't have ib0 available so just use
            # a safe default for the tests
            "interface": "",
            "fork_mpi": False,
            "mpi_launcher": SRUN,
            "memory": "1GB",
        }

    def test_custom_cluster(self):
        cluster = get_cluster(scheduler=SLURM, **self.kwargs)
        self.assertEqual(cluster.name, self.cluster_name)
        self.assertIsInstance(cluster.client, Client)
        with self.assertRaises(ValueError):
            get_cluster(SLURM, cores=128, **self.kwargs)
        controller.delete_cluster(cluster.name)

    def test_gpu_cluster(self):
        cluster = get_cluster(queue_type="gpus", **self.kwargs)
        self.assertEqual(cluster.name, "dask-worker-gpus")
        self.assertIn("#SBATCH -p gpus", cluster.job_header)
        self.assertIn("#SBATCH --gres=gpu:4", cluster.job_header)
        self.assertIsInstance(cluster.client, Client)
        controller.delete_cluster(cluster.name)

    def test_knl_cluster(self):
        cluster = get_cluster(queue_type="knl", **self.kwargs)
        self.assertEqual(cluster.name, "dask-worker-knl")
        self.assertIn("#SBATCH -p booster", cluster.job_header)
        self.assertIn("#SBATCH --cpus-per-task=64", cluster.job_header)
        self.assertIsInstance(cluster.client, Client)
        controller.delete_cluster(cluster.name)

    def test_nonexistent_queue_type(self):
        with self.assertRaises(ValueError) as context:
            get_cluster(queue_type="chicken", **self.kwargs)
        self.assertIn(
            "queue_type kwarg value 'chicken' not in available options",
            str(context.exception),
        )

    def test_mpi_job_cluster(self):
        # First do a simple mpi job
        cluster = get_cluster(queue_type="knl", mpi_mode=True, cores=64, **self.kwargs)
        self.assertIn("#SBATCH --cpus-per-task=1", cluster.job_header)
        self.assertIn("#SBATCH --ntasks-per-node=64", cluster.job_header)
        self.assertIn("#SBATCH -n 64", cluster.job_script())
        self.assertIn(MPI_DASK_WRAPPER_MODULE, cluster._command_template)
        self.assertEqual(cluster.worker_cores, 1)
        self.assertEqual(cluster.worker_processes, 1)
        self.assertEqual(cluster.worker_process_threads, 1)
        self.assertIsInstance(cluster.client, Client)
        controller.delete_cluster(cluster.name)
        with self.assertRaises(ValueError):
            cluster = get_cluster(
                queue_type="knl", mpi_mode=True, nodes=64, cores=64, **self.kwargs
            )
            controller.delete_cluster(cluster.name)

    def test_fork_mpi_job_cluster(self):
        # First do a simple mpi job
        kwargs = self.kwargs
        kwargs.update({"fork_mpi": True})
        cluster = get_cluster(queue_type="knl", mpi_mode=True, cores=64, **kwargs)
        self.assertNotIn(MPI_DASK_WRAPPER_MODULE, cluster._command_template)
        controller.delete_cluster(cluster.name)

    def test_mpi_multi_node_job_cluster(self):
        # First do a simple mpi job
        cluster = get_cluster(queue_type="knl", mpi_mode=True, cores=130, **self.kwargs)
        self.assertIn("#SBATCH --cpus-per-task=1", cluster.job_header)
        self.assertIn("#SBATCH --ntasks-per-node=64", cluster.job_header)
        self.assertIn("#SBATCH -n 130", cluster.job_script())
        self.assertEqual(cluster.worker_cores, 1)
        self.assertEqual(cluster.worker_processes, 1)
        self.assertEqual(cluster.worker_process_threads, 1)
        with self.assertRaises(ValueError):
            get_cluster(queue_type="knl", mpi_mode=True, **self.kwargs)
        with self.assertRaises(ValueError):
            get_cluster(
                queue_type="knl",
                cpus_per_task=37,
                cores=2,
                mpi_mode=True,
                **self.kwargs
            )
        with self.assertRaises(ValueError):
            get_cluster(
                queue_type="knl",
                cores=1,
                ntasks_per_node=13,
                mpi_mode=True,
                **self.kwargs
            )
        controller.delete_cluster(cluster.name)

    def test_mpi_complex_job_cluster(self):
        # Now a few more variables
        cluster = get_cluster(
            queue_type="gpus", mpi_mode=True, nodes=2, ntasks_per_node=4, **self.kwargs
        )
        self.assertIn("#SBATCH --cpus-per-task=6", cluster.job_header)
        self.assertIn("#SBATCH --ntasks-per-node=4", cluster.job_header)
        self.assertIn("#SBATCH --nodes=2", cluster.job_header)
        self.assertIn("#SBATCH --gres=gpu:4", cluster.job_header)
        self.assertEqual(cluster.worker_cores, 1)
        self.assertEqual(cluster.worker_processes, 1)
        self.assertEqual(cluster.worker_process_threads, 1)
        self.assertIn("#SBATCH -n 8", cluster.job_script())
        self.assertIn(
            "export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}", cluster.job_script()
        )
        self.assertIn("export OMP_PROC_BIND=spread", cluster.job_script())
        self.assertIn("export OMP_PLACES=threads", cluster.job_script())
        controller.delete_cluster(cluster.name)

    def test_mpi_complex_job_cluster_fail(self):
        # Now a few more variables
        with self.assertRaises(ValueError) as ctx:
            # When we provide ntasks_per_node, cpus_per_tasks is derived (in this case
            # 24/2 = 12). For an MPI job we expect the core count (which is the total
            # number of cores to be used) to be divisible by cpus_per_tasks but that is
            # not true in this case resulting in a ValueError
            get_cluster(
                queue_type="gpus",
                mpi_mode=True,
                cores=2,
                ntasks_per_node=2,
                **self.kwargs
            )
        # If you really want this you ask for it explicitly
        cluster = get_cluster(
            queue_type="gpus",
            mpi_mode=True,
            cores=2,
            ntasks_per_node=2,
            cpus_per_task=1,
            **self.kwargs
        )
        self.assertIn("#SBATCH --cpus-per-task=1", cluster.job_header)
        self.assertIn("#SBATCH --ntasks-per-node=2", cluster.job_header)
        self.assertIn("#SBATCH -n 2", cluster.job_header)
        self.assertNotIn("#SBATCH --nodes", cluster.job_header)
        self.assertIn("#SBATCH --gres=gpu:4", cluster.job_header)
        controller.delete_cluster(cluster.name)
        # For memory pinning stuff that may be done by the scheduler, it is probably
        # better to ask for it like this (even if you don't intend to use OpenMP)
        cluster = get_cluster(
            queue_type="gpus", mpi_mode=True, nodes=1, ntasks_per_node=2, **self.kwargs
        )
        self.assertIn("#SBATCH --cpus-per-task=12", cluster.job_header)
        self.assertIn("#SBATCH --ntasks-per-node=2", cluster.job_header)
        self.assertIn("#SBATCH -n 2", cluster.job_header)
        self.assertIn("#SBATCH --nodes=1", cluster.job_header)
        self.assertIn("#SBATCH --gres=gpu:4", cluster.job_header)
        controller.delete_cluster(cluster.name)

    def test_mpi_explicit_job_cluster(self):
        # Now a few more variables
        cluster = get_cluster(
            queue_type="gpus",
            mpi_mode=True,
            nodes=2,
            cpus_per_task=2,
            ntasks_per_node=12,
            **self.kwargs
        )
        self.assertIn("#SBATCH --cpus-per-task=2", cluster.job_header)
        self.assertIn("#SBATCH --ntasks-per-node=12", cluster.job_header)
        self.assertIn("#SBATCH --nodes=2", cluster.job_header)
        self.assertIn("#SBATCH --gres=gpu:4", cluster.job_header)
        self.assertEqual(cluster.worker_cores, 1)
        self.assertEqual(cluster.worker_processes, 1)
        self.assertEqual(cluster.worker_process_threads, 1)
        self.assertIn("#SBATCH -n 24", cluster.job_script())
        self.assertIn(
            "export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}", cluster.job_script()
        )
        self.assertIn("export OMP_PROC_BIND=spread", cluster.job_script())
        self.assertIn("export OMP_PLACES=threads", cluster.job_script())
        controller.delete_cluster(cluster.name)

    def test_scheduler_fail_job_cluster(self):
        with self.assertRaises(NotImplementedError):
            get_cluster(scheduler="pbs", **self.kwargs)

    def test_non_integer_kwargs(self):
        with self.assertRaises(ValueError):
            get_cluster(SLURM, minimum_cores="a", **self.kwargs)
        with self.assertRaises(ValueError):
            get_cluster(SLURM, cores_per_node=0, **self.kwargs)
        with self.assertRaises(ValueError):
            get_cluster(SLURM, hyperthreading_factor=[], **self.kwargs)

    def test_wrong_hyperthreading_factor(self):
        with self.assertRaises(ValueError):
            get_cluster(
                SLURM,
                minimum_cores=2,
                cores_per_node=1,
                hyperthreading_factor=1,
                **self.kwargs
            )
