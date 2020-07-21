from __future__ import print_function

from unittest import TestCase

import pytest

from jobqueue_features import (
    MPIEXEC,
    OPENMPI,
    CustomPBSCluster,
)
from jobqueue_features.tests.resources.test_base import TestBase

# Use logging if there are hard to see issues in the CI

# import logging
# logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)


class TestCIPBS(TestBase, TestCase):
    cluster_name = "PBS"
    cluster = CustomPBSCluster
    mpi_launcher = MPIEXEC
    default_mpi_launcher = OPENMPI
    queue_name = "workq"
    slave_1_name = "pbs-slave-1"
    slave_2_name = "pbs-slave-2"
    memory = "2 GB"

    def _multi_mpi_wrap_assert(self, test_function, nodes):
        tasks = []
        for x in range(20):
            tasks.append(
                (
                    test_function(self.script_path),
                    # Only check for root in the output
                    "Hello, World! I am process 0 of {}".format(
                        self.number_of_processes_per_node * nodes
                    ),
                )
            )
        c1_count = 0
        c2_count = 0
        for job, text in iter(tasks):
            result = job.result()["out"]
            self.assertIn(text.encode(), result)
            # Count which node the job executed on
            self.assertTrue(
                self.slave_1_name.encode() in result
                or self.slave_2_name.encode() in result
            )
            if self.slave_1_name.encode() in result:
                c1_count += 1
            elif self.slave_2_name.encode() in result:
                c2_count += 1
        self.assertTrue(c1_count + c2_count == 20)

    def _single_mpi_tasks_assert(self, task1, task2, nodes):

        tasks = []
        tasks.append(
            (
                task1("task1"),
                "Running {} tasks of type task1 on nodes".format(
                    self.number_of_processes_per_node * nodes,
                ),
            )
        )
        tasks.append(
            (
                task1("task1, 2nd iteration"),
                "Running {} tasks of type task1, 2nd iteration on nodes".format(
                    self.number_of_processes_per_node * nodes,
                ),
            )
        )
        tasks.append(
            (
                task2("Alan", task_name="Task 2"),
                "Hi Alan, my rank is 0 for task of type Task 2",
            )
        )
        for task, text in iter(tasks):
            self.assertIn(text, task.result())

    def _multi_mpi_tasks_assert(self, task):
        tasks = []
        for x in range(20):
            tasks.append(
                (
                    task("task-{}".format(x)),
                    # We don't know which node the task will run on
                    "Running {} tasks of type task-{} on nodes ".format(
                        self.number_of_processes_per_node, x
                    ),
                )
            )
        c1_count = 0
        c2_count = 0
        for job, text in iter(tasks):
            self.assertIn(text, job.result())
            # Count which node the job executed on
            self.assertTrue(
                self.slave_1_name in job.result()
                or self.slave_2_name in job.result()
            )
            if self.slave_1_name in job.result():
                c1_count += 1
            elif self.slave_2_name in job.result():
                c2_count += 1
        self.assertTrue(c1_count + c2_count == 20)

    @pytest.mark.env("pbs")
    def test_single_mpi_wrap(self):
        self._test_single_mpi_wrap()

    @pytest.mark.env("pbs")
    def test_multi_mpi_wrap(self):
        self._test_multi_mpi_wrap()

    @pytest.mark.env("pbs")
    def test_single_mpi_tasks(self):
        self._test_single_mpi_tasks()

    @pytest.mark.env("pbs")
    def test_multi_mpi_tasks(self):
        self._test_multi_mpi_tasks()
