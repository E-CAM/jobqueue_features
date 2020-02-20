from __future__ import print_function

import shlex
from unittest import TestCase
import os
import subprocess

from distributed import LocalCluster

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
)


class TestMPIWrap(TestCase):
    def setUp(self):
        self.number_of_processes = 4
        self.local_cluster = LocalCluster()
        self.executable = "python"
        self.launcher = MPIEXEC
        # Include some (non-standard) OpenMPI options so that we can run this in CI
        if not self.is_mpich():
            self.launcher_args = "--allow-run-as-root --oversubscribe"
        else:
            self.launcher_args = ''
        self.script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "resources", "helloworld.py")
        )

        @mpi_task(cluster_id="test", default_mpi_tasks=4)
        def mpi_wrap_task(**kwargs):
            return mpi_wrap(**kwargs)

        @on_cluster(cluster=self.local_cluster, cluster_id="test")
        def test_function(script_path, return_wrapped_command=False):
            t = mpi_wrap_task(
                executable=self.executable,
                exec_args=script_path,
                mpi_launcher=self.launcher,
                launcher_args=self.launcher_args,
                mpi_tasks=self.number_of_processes,
                return_wrapped_command=return_wrapped_command,
            )
            result = t.result()

            return result

        self.test_function = test_function

        def mpi_task1(task_name):
            comm = get_task_mpi_comm()
            size = comm.Get_size()
            # Since it is a return  value it will only get printed by root
            return "Running %d tasks of type %s." % (size, task_name)

        self.mpi_task1 = mpi_task1

        def string_task(string, kwarg_string=None):
            return " ".join([s for s in [string, kwarg_string] if s])

        self.string_task = string_task

    def is_mpich(self):
        cmd = 'mpicc -v'
        proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if b'mpich' in proc.stdout.read().lower():
            return True
        return False

    def test_which(self):
        # Check it finds a full path
        self.assertEqual(which(self.script_path), self.script_path)
        # Check it searches the PATH envvar
        os.environ["PATH"] += os.pathsep + os.path.dirname(self.script_path)
        self.assertEqual(which(os.path.basename(self.script_path)), self.script_path)
        # Check it returns None if the executable doesn't exist
        self.assertIsNone(which("not_an_executable"))
        # Check it returns None when a file is not executable
        self.assertIsNone(which(os.path.realpath(__file__)))

    def test_mpi_wrap(self):
        #
        # Assume here we have mpiexec support
        if which(self.launcher) is not None:
            print("Found {}, running MPI test".format(self.launcher))
            result = self.test_function(self.script_path)
            for n in range(self.number_of_processes):
                text = "Hello, World! I am process {} of {}".format(
                    n, self.number_of_processes
                )
                self.assertIn(text.encode(), result["out"])
            result = self.test_function(self.script_path, return_wrapped_command=True)
            _cmd = (
                self.launcher,
                '-np',
                self.number_of_processes,
                self.launcher_args,
                self.executable,
                self.script_path,
            )
            expected_result = ' '.join(filter(len, map(str, _cmd)))
            self.assertEqual(result, expected_result)
        else:
            pass

    # Test the MPI wrapper in isolation for srun (which we assume doesn't exist):
    def test_mpi_srun_wrapper(self):
        if which(SRUN) is None:
            print(
                "Didn't find {}, running OSError test for no available launcher".format(
                    SRUN
                )
            )
            with self.assertRaises(OSError) as context:
                mpi_wrap(
                    executable="python",
                    exec_args=self.script_path,
                    mpi_launcher=SRUN,
                    mpi_tasks=self.number_of_processes,
                )
            self.assertTrue(
                "OS error caused by constructed command" in str(context.exception)
            )
        else:
            pass

    # Test our serialisation method
    def test_serialize_function_and_args(self):
        # First check elements in our dict
        serialized_object = serialize_function_and_args(self.string_task)
        for key in serialized_object.keys():
            self.assertIn(key, ["header", "frames"])
        serialized_object = serialize_function_and_args(self.string_task, "chicken")
        for key in serialized_object.keys():
            self.assertIn(key, ["header", "frames", "args_header", "args_frames"])
        serialized_object = serialize_function_and_args(
            self.string_task, kwarg_string="dog"
        )
        for key in serialized_object.keys():
            self.assertIn(key, ["header", "frames", "kwargs_header", "kwargs_frames"])
        serialized_object = serialize_function_and_args(
            self.string_task, "chicken", kwarg_string="dog"
        )
        for key in serialized_object.keys():
            self.assertIn(
                key,
                [
                    "header",
                    "frames",
                    "args_header",
                    "args_frames",
                    "kwargs_header",
                    "kwargs_frames",
                ],
            )

    def test_deserialize_and_execute(self):
        serialized_object = serialize_function_and_args(
            self.string_task, "chicken", kwarg_string="dog"
        )
        self.assertEqual("chicken dog", deserialize_and_execute(serialized_object))

    def test_flush_and_abort(self):
        with self.assertRaises(SystemExit) as cm:
            flush_and_abort(mpi_abort=False)
        self.assertEqual(cm.exception.code, 1)
        with self.assertRaises(SystemExit) as cm:
            flush_and_abort(error_code=2, mpi_abort=False)
        self.assertEqual(cm.exception.code, 2)

    def test_verify_mpi_communicator(self):
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
        with self.assertRaises(SystemExit) as cm:
            verify_mpi_communicator("Not a communicator", mpi_abort=False)
        self.assertEqual(cm.exception.code, 1)
        self.assertTrue(verify_mpi_communicator(comm, mpi_abort=False))

    def test_mpi_deserialize_and_execute(self):
        trivial = "trivial"
        serialized_object = serialize_function_and_args(self.mpi_task1, trivial)
        # For the deserializer to work we need to first set the task MPI communicator
        with self.assertRaises(AttributeError):
            mpi_deserialize_and_execute(serialized_object)
        set_task_mpi_comm()
        # The test framework is not started with an MPI launcher so we have a single task
        expected_string = "Running 1 tasks of type {}.".format(trivial)
        return_value = mpi_deserialize_and_execute(serialized_object)
        self.assertEqual(expected_string, return_value)
