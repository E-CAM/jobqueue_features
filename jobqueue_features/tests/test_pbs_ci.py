from __future__ import print_function

from unittest import TestCase

import pytest

from jobqueue_features import (
    MPIEXEC,
    OPENMPI,
    CustomPBSCluster,
)

from jobqueue_features.tests.test_base import TestBase

# Use logging if there are hard to see issues in the CI

# import logging
# logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)


class TestCIPBS(TestBase, TestCase):
    cluster_name = 'PBS'
    cluster = CustomPBSCluster
    mpi_launcher = MPIEXEC
    default_mpi_launcher = OPENMPI
    queue_name = 'batch'
    slave_1_name = 'c1'
    slave_2_name = 'c2'
    memory = '2 GB'

    @pytest.mark.env("pbs")
    def test_single_mpi_wrap(self):
        super().test_single_mpi_wrap()

    @pytest.mark.env("pbs")
    def test_multi_mpi_wrap(self):
        super().test_multi_mpi_wrap()

    @pytest.mark.env("pbs")
    def test_single_mpi_tasks(self):
        super().test_single_mpi_tasks()

    @pytest.mark.env("pbs")
    def test_multi_mpi_tasks(self):
        super().test_multi_mpi_tasks()
