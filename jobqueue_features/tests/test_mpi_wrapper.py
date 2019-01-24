from __future__ import print_function
from unittest import TestCase
import os

from distributed import LocalCluster

from jobqueue_features import mpi_wrap, MPIEXEC, SRUN, on_cluster, mpi_task


def which(file_name):
    for path in os.environ["PATH"].split(os.pathsep):
        full_path = os.path.join(path, file_name)
        if os.path.exists(full_path) and os.access(full_path, os.X_OK):
            return full_path
    return None


class TestMPIWrap(TestCase):
    def setUp(self):
        self.number_of_processes = 4
        self.local_cluster = LocalCluster()
        self.script_path = os.path.join('../../examples', 'resources',
                                        'helloworld.py')

        @mpi_task(cluster_id='test', default_mpi_tasks=4)
        def mpi_wrap_task(**kwargs):
            return mpi_wrap(**kwargs)

        @on_cluster(cluster=self.local_cluster, cluster_id='test')
        def test_function(script_path):
            t = mpi_wrap_task(executable='python', exec_args=script_path, mpi_launcher=MPIEXEC,
                              mpi_tasks=self.number_of_processes)
            return t.result()['out']

        self.test_function = test_function

    def test_mpi_wrap(self):
        # Assume here we have mpiexec support
        if which(MPIEXEC) is not None:
            result = self.test_function(self.script_path)
            for n in range(self.number_of_processes):
                text = 'Hello, World! I am process {} of {}'.format(
                    n, self.number_of_processes)
                self.assertIn(text, result)
        else:
            pass

    # Test the MPI wrapper in isolation for srun (which we assume doesn't exist):
    def test_mpi_srun_wrapper(self):
        if which(SRUN) is None:
            with self.assertRaises(OSError) as context:
                mpi_wrap(executable='python', exec_args=self.script_path, mpi_launcher=SRUN,
                         mpi_tasks=self.number_of_processes)
            self.assertTrue('OS error caused by constructed command' in context.exception.message)
        else:
            pass
