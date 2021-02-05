import os
import time

from jobqueue_features.clusters_controller import (
    clusters_controller_singleton as controller,
)

from jobqueue_features import mpi_wrap, on_cluster, mpi_task, which, get_task_mpi_comm


class TestBase:
    cluster_name = ""
    cluster = None
    mpi_launcher = None
    default_mpi_launcher = None
    queue_name = ""
    slave_1_name = ""
    slave_2_name = ""
    memory = ""

    def _single_mpi_wrap_assert(self, test_function, nodes):
        # First check we can construct the command
        result = test_function(self.script_path, return_wrapped_command=True)
        result = result.result()
        expected_result = "{} -n {} {} {}".format(
            self.mpi_launcher["launcher"],
            self.number_of_processes_per_node * nodes,
            self.executable,
            self.script_path,
        )
        self.assertEqual(result, expected_result)

        # Then check the execution of it
        result = test_function(self.script_path)
        result = result.result()
        for n in range(self.number_of_processes_per_node):
            text = "Hello, World! I am process {} of {}".format(
                n, self.number_of_processes_per_node * nodes
            )
            self.assertIn(text.encode(), result["out"])

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
        self.assertTrue(c1_count > 0)
        self.assertTrue(c2_count > 0)

    def _single_mpi_tasks_assert(self, task1, task2, nodes):

        tasks = []
        tasks.append(
            (
                task1("task1"),
                "Running {} tasks of type task1 on nodes {}.".format(
                    self.number_of_processes_per_node * nodes,
                    [self.slave_1_name, self.slave_2_name],
                ),
            )
        )
        tasks.append(
            (
                task1("task1, 2nd iteration"),
                "Running {} tasks of type task1, 2nd iteration on nodes {}.".format(
                    self.number_of_processes_per_node * nodes,
                    [self.slave_1_name, self.slave_2_name],
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
                self.slave_1_name in job.result() or self.slave_2_name in job.result()
            )
            if self.slave_1_name in job.result():
                c1_count += 1
            elif self.slave_2_name in job.result():
                c2_count += 1
        self.assertTrue(c1_count + c2_count == 20)
        self.assertTrue(c1_count > 0)
        self.assertTrue(c2_count > 0)

    def setUp(self):
        # Kill any existing clusters
        controller._close()
        self.number_of_processes_per_node = 2

        self.run_tests = False
        if which(self.default_mpi_launcher["launcher"]) is not None:
            print(
                "Found {} so assuming we have {}, running MPI test (with {})".format(
                    self.default_mpi_launcher["launcher"],
                    self.cluster_name,
                    self.mpi_launcher,
                )
            )
            self.run_tests = True

        self.common_kwargs = {
            "interface": None,
            "walltime": "00:04:00",
            "cores_per_node": 2,
            "minimum_cores": 2,
            "hyperthreading_factor": 1,
            "ntasks_per_node": 2,
            "memory": self.memory,
            "mpi_mode": True,
            "env_extra": [
                "export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1",
                "export OMPI_ALLOW_RUN_AS_ROOT=1",
            ],
            "mpi_launcher": self.mpi_launcher,
            "local_directory": "/tmp",
            "queue": self.queue_name,
        }

        self.executable = "python"
        self.script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "helloworld.py")
        )

    def tearDown(self):
        # Kill any existing clusters
        controller._close()

    def _test_minimum_jobs(self):
        if self.run_tests:
            controller._close()

            # Create the cluster
            nodes = 1
            fork_cluster = self.cluster(
                name="fork_cluster",
                fork_mpi=True,
                nodes=nodes,
                minimum_jobs=2,
                **self.common_kwargs
            )

            # Create the function that wraps tasks for this cluster
            @on_cluster(cluster=fork_cluster)
            @mpi_task(cluster_id=fork_cluster.name)
            def mpi_wrap_task(**kwargs):
                return mpi_wrap(**kwargs)

            def test_function(script_path, return_wrapped_command=False):
                t = mpi_wrap_task(
                    executable=self.executable,
                    exec_args=script_path,
                    return_wrapped_command=return_wrapped_command,
                )
                return t

            # Wait for a few seconds and the workers will start
            time.sleep(5)

            self.assertEqual(len(fork_cluster.client.scheduler_info()["workers"]), 2)

            controller._close()
        else:
            pass

    def _test_single_mpi_wrap(self):
        if self.run_tests:
            controller._close()

            # Create the cluster
            nodes = 2
            fork_cluster = self.cluster(
                name="fork_cluster", fork_mpi=True, nodes=nodes, **self.common_kwargs
            )

            # Create the function that wraps tasks for this cluster
            @on_cluster(cluster=fork_cluster)
            @mpi_task(cluster_id=fork_cluster.name)
            def mpi_wrap_task(**kwargs):
                return mpi_wrap(**kwargs)

            def test_function(script_path, return_wrapped_command=False):
                t = mpi_wrap_task(
                    executable=self.executable,
                    exec_args=script_path,
                    return_wrapped_command=return_wrapped_command,
                )
                return t

            self._single_mpi_wrap_assert(test_function, nodes)
            controller._close()
        else:
            pass

    def _test_multi_mpi_wrap(self):
        if self.run_tests:
            controller._close()

            # Create the cluster
            nodes = 1
            fork_cluster = self.cluster(
                name="multifork_cluster",
                fork_mpi=True,
                nodes=nodes,
                maximum_jobs=2,
                **self.common_kwargs
            )

            # Create the function that wraps tasks for this cluster
            @on_cluster(cluster=fork_cluster)
            @mpi_task(cluster_id=fork_cluster.name)
            def mpi_wrap_task(**kwargs):
                return mpi_wrap(**kwargs)

            def test_function(script_path, return_wrapped_command=False):
                t = mpi_wrap_task(
                    executable=self.executable,
                    exec_args=script_path,
                    return_wrapped_command=return_wrapped_command,
                )
                return t

            self._multi_mpi_wrap_assert(test_function, nodes)
            controller._close()
        else:
            pass

    def _test_single_mpi_tasks(self):
        # Assume here we have srun support
        if self.run_tests:
            controller._close()
            nodes = 2
            custom_cluster = self.cluster(
                name="mpiCluster", nodes=nodes, **self.common_kwargs
            )

            @on_cluster(cluster=custom_cluster)
            @mpi_task(cluster_id=custom_cluster.name)
            def task1(task_name):
                from mpi4py import MPI

                comm = get_task_mpi_comm()
                size = comm.Get_size()
                name = MPI.Get_processor_name()
                all_nodes = comm.gather(name, root=0)
                if all_nodes:
                    all_nodes = list(set(all_nodes))
                    all_nodes.sort()
                else:
                    all_nodes = []
                # Since it is a return  value it will only get printed by root
                return_string = "Running %d tasks of type %s on nodes %s." % (
                    size,
                    task_name,
                    all_nodes,
                )
                return return_string

            @on_cluster(cluster=custom_cluster)
            @mpi_task(cluster_id=custom_cluster.name)
            def task2(name, task_name="default"):
                comm = get_task_mpi_comm()
                rank = comm.Get_rank()
                # This only appears in the slurm job output
                return_string = "Hi %s, my rank is %d for task of type %s" % (
                    name,
                    rank,
                    task_name,
                )
                return return_string

            self._single_mpi_tasks_assert(task1, task2, nodes)
            controller._close()
        else:
            pass

    def _test_multi_mpi_tasks(self):
        #
        # Assume here we have srun support
        if self.run_tests:
            controller._close()
            # We only have 2 worker nodes so to have multiple jobs we need one worker
            # per node
            custom_cluster = self.cluster(
                name="mpiMultiCluster", nodes=1, maximum_jobs=2, **self.common_kwargs
            )

            @on_cluster(cluster=custom_cluster)
            @mpi_task(cluster_id=custom_cluster.name)
            def task(task_name):
                import time
                from mpi4py import MPI

                comm = get_task_mpi_comm()
                size = comm.Get_size()
                name = MPI.Get_processor_name()
                all_nodes = comm.gather(name, root=0)
                if all_nodes:
                    all_nodes = list(set(all_nodes))
                    all_nodes.sort()
                else:
                    all_nodes = []
                # Since it is a return  value it will only get printed by root
                return_string = "Running %d tasks of type %s on nodes %s." % (
                    size,
                    task_name,
                    all_nodes,
                )

                # Add a sleep to make the task substantial enough to require scaling
                time.sleep(1)
                return return_string

            self._multi_mpi_tasks_assert(task)

            controller._close()
        else:
            pass
