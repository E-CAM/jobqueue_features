from unittest import TestCase

from jobqueue_features.functions import get_callable_args


class TestGetCallableArgs(TestCase):
    def test_function_no_arguments(self):
        def f_test():
            return

        self.assertEqual(get_callable_args(f_test), [])

    def test_function_simple_args(self):
        def f_test(a, b, c):
            return

        self.assertEqual(get_callable_args(f_test), ["a", "b", "c"])

    def test_function_args_and_kwargs(self):
        def f_test(a, b, c=True):
            return

        self.assertEqual(get_callable_args(f_test), ["a", "b", "c"])

    def test_function_star_args(self):
        def f_test(a, *args, b=1):
            return

        self.assertEqual(get_callable_args(f_test), ["a", "b"])

    def test_function_kwargs_only(self):
        def f_test(*args, a=1, b=2):
            return

        self.assertEqual(get_callable_args(f_test), ["a", "b"])
        self.assertEqual(get_callable_args(f_test, remove_self=False), ["a", "b"])

    def test_class_method_simple(self):
        class C:
            def method(self, a, b, c=1):
                return

        self.assertEqual(get_callable_args(C.method), ["a", "b", "c"])

    def test_class_method_complex(self):
        class C:
            def method(self, a, b, *args, c=1, **kwargs):
                return

        self.assertEqual(get_callable_args(C.method), ["a", "b", "c"])
        self.assertEqual(
            get_callable_args(C.method, remove_self=False), ["self", "a", "b", "c"]
        )
