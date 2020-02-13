from __future__ import print_function
from unittest import TestCase
import os
import pytest

from jobqueue_features import (
    mpi_wrap,
    MPIEXEC,
    SRUN,
    on_cluster,
    mpi_task,
    which,
    get_task_mpi_comm,
    set_task_mpi_comm,
    serialize_function_and_args,
    deserialize_and_execute,
    mpi_deserialize_and_execute,
    verify_mpi_communicator,
    flush_and_abort,
    CustomSLURMCluster,
)


class TestSLURM(TestCase):
    def setUp(self):
        self.number_of_processes = 4
        self.slurm_cluster = CustomSLURMCluster(
            name="mpiCluster",
            walltime="00:04:00",
            nodes=2,
            cores_per_node=2,
            minimum_cores=2,
            hyperthreading_factor=1,
            ntasks_per_node=2,
            memory="256 MB",
            mpi_mode=True,
            fork_mpi=True,
        )

        self.executable = "python"
        self.script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "resources", "helloworld.py")
        )

        @on_cluster(cluster=self.slurm_cluster, cluster_id="mpiCluster")
        @mpi_task(cluster_id="mpiCluster", default_mpi_tasks=4)
        def mpi_wrap_task(**kwargs):
            return mpi_wrap(**kwargs)

        @on_cluster(cluster=self.slurm_cluster)
        def test_function(script_path, return_wrapped_command=False):
            script_path = os.path.join(
                os.path.dirname(__file__), "resources", "helloworld.py"
            )
            t = mpi_wrap_task(
                executable=self.executable,
                exec_args=script_path,
                mpi_launcher=SRUN,
                mpi_tasks=self.number_of_processes,
                return_wrapped_command=return_wrapped_command,
            )
            result = t.result()

            return result

        self.test_function = test_function

    @pytest.mark.env("slurm")
    def test_mpi_wrap(self):
        #
        # Assume here we have srun support
        if which(SRUN) is not None:
            print("Found {}, running MPI test".format(SRUN))
            result = self.test_function(self.script_path)
            print(result)
            for n in range(self.number_of_processes):
                text = "Hello, World! I am process {} of {}".format(
                    n, self.number_of_processes
                )
                self.assertIn(text.encode(), result["out"])
            result = self.test_function(self.script_path, return_wrapped_command=True)
            expected_result = "{} {} {} {}".format(
                SRUN, self.number_of_processes, self.executable, self.script_path
            )
            self.assertEqual(result, expected_result)
        else:
            pass
