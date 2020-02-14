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
    CustomSLURMCluster,
)


class TestSLURM(TestCase):
    def setUp(self):
        self.number_of_processes = 4
        # Really hard to get srun in CI, so use mpiexec to keep things simple
        self.launcher = MPIEXEC
        self.slurm_cluster = CustomSLURMCluster(
            interface="",
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
            env_extra=[
                "export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1",
                "export OMPI_ALLOW_RUN_AS_ROOT=1",
            ],
            mpi_launcher=self.launcher,
            local_directory="/tmp",
            queue="batch",
        )
        self.executable = "python"
        self.script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "resources", "helloworld.py")
        )

        @on_cluster(cluster=self.slurm_cluster, cluster_id="mpiCluster")
        @mpi_task(cluster_id="mpiCluster")
        def mpi_wrap_task(**kwargs):
            return mpi_wrap(**kwargs)

        def test_function(script_path, return_wrapped_command=False):
            t = mpi_wrap_task(
                executable=self.executable,
                exec_args=script_path,
                # mpi_tasks=self.number_of_processes,
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
            print(
                "Found {} so assuming we have Slurm, running MPI test (with {})".format(
                    SRUN, self.launcher
                )
            )
            launcher = self.launcher
            # First check we can construct the command
            result = self.test_function(self.script_path, return_wrapped_command=True)
            expected_result = "{} -np {} {} {}".format(
                launcher, self.number_of_processes, self.executable, self.script_path
            )
            self.assertEqual(result, expected_result)
            result = self.test_function(self.script_path)
            print(result)
            for n in range(self.number_of_processes):
                text = "Hello, World! I am process {} of {}".format(
                    n, self.number_of_processes
                )
                self.assertIn(text.encode(), result["out"])
        else:
            pass
