from __future__ import print_function
from unittest import TestCase
import os

from distributed import LocalCluster

from jobqueue_features import (
    mpi_wrap,
    MPIEXEC,
    SRUN,
    on_cluster,
    mpi_task,
    which,
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
        self.script_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "..",
                "examples",
                "resources",
                "helloworld.py",
            )
        )

        @mpi_task(cluster_id="test", default_mpi_tasks=4)
        def mpi_wrap_task(**kwargs):
            return mpi_wrap(**kwargs)

        @on_cluster(cluster=self.local_cluster, cluster_id="test")
        def test_function(script_path, return_wrapped_command=False):
            t = mpi_wrap_task(
                executable=self.executable,
                exec_args=script_path,
                mpi_launcher=MPIEXEC,
                mpi_tasks=self.number_of_processes,
                return_wrapped_command=return_wrapped_command,
            )
            if return_wrapped_command:
                result = t.result()
            else:
                result = t.result()["out"]

            return result

        self.test_function = test_function

        def mpi_task1(task_name):
            from mpi4py import MPI

            comm = MPI.COMM_WORLD
            size = comm.Get_size()
            # Since it is a return  value it will only get printed by root
            return "Running %d tasks of type %s." % (size, task_name)

        self.mpi_task1 = mpi_task1

        def string_task(string, kwarg_string=None):
            return " ".join([s for s in [string, kwarg_string] if s])

        self.string_task = string_task

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
        if which(MPIEXEC) is not None:
            print("Found {}, running MPI test".format(MPIEXEC))
            result = self.test_function(self.script_path)
            for n in range(self.number_of_processes):
                text = "Hello, World! I am process {} of {}".format(
                    n, self.number_of_processes
                )
                self.assertIn(text.encode(), result)
            result = self.test_function(self.script_path, return_wrapped_command=True)
            expected_result = "{} -np {} {} {}".format(
                MPIEXEC, self.number_of_processes, self.executable, self.script_path
            )
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
        self.assertEqual(["header", "frames"], list(serialized_object.keys()))
        serialized_object = serialize_function_and_args(self.string_task, "chicken")
        self.assertEqual(
            ["header", "frames", "args_header", "args_frames"],
            list(serialized_object.keys()),
        )
        serialized_object = serialize_function_and_args(
            self.string_task, kwarg_string="dog"
        )
        self.assertEqual(
            ["header", "frames", "kwargs_header", "kwargs_frames"],
            list(serialized_object.keys()),
        )
        serialized_object = serialize_function_and_args(
            self.string_task, "chicken", kwarg_string="dog"
        )
        self.assertEqual(
            [
                "header",
                "frames",
                "args_header",
                "args_frames",
                "kwargs_header",
                "kwargs_frames",
            ],
            list(serialized_object.keys()),
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
        # The test framework is not started with an MPI launcher so we have a single task
        expected_string = "Running 1 tasks of type {}.".format(trivial)
        return_value = mpi_deserialize_and_execute(serialized_object)
        self.assertEqual(expected_string, return_value)
