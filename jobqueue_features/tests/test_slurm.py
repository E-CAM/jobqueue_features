from __future__ import print_function

from unittest import TestCase

import pytest

from jobqueue_features import (
    MPIEXEC,
    SRUN,
    CustomSLURMCluster,
)
from jobqueue_features.tests.resources.test_base import TestBase

# Use logging if there are hard to see issues in the CI

# import logging
# logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)


class TestSLURM(TestBase, TestCase):
    cluster_name = 'SLURM'
    cluster = CustomSLURMCluster
    mpi_launcher = MPIEXEC
    default_mpi_launcher = SRUN
    queue_name = 'batch'
    slave_1_name = 'c1'
    slave_2_name = 'c2'
    memory = '256 MB'

    @pytest.mark.env("slurm")
    def test_single_mpi_wrap(self):
        self._test_single_mpi_wrap()

    @pytest.mark.env("slurm")
    def test_multi_mpi_wrap(self):
        self._test_multi_mpi_wrap()

    @pytest.mark.env("slurm")
    def test_single_mpi_tasks(self):
        self._test_single_mpi_tasks()

    @pytest.mark.env("slurm")
    def test_multi_mpi_tasks(self):
        self._test_multi_mpi_tasks()
