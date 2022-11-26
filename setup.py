from setuptools import setup, find_packages

setup(
    name="jobqueue_features",
    version="0.1dev",
    description="This library provides some useful decorators for using "
    "dask_jobqueue. It also contains predefined cluster "
    "classes for specific JURECA cluster's partitions.",
    url="https://gitlab.e-cam2020.eu/adam/dask-jobqueue-decorators",
    author="Adam Wlodarczyk",
    author_email="adam.wlodarczyk@pwr.edu.pl",
    license="MIT",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "dask>=0.18.1",
        "distributed>=1.22.0",
        "dask_jobqueue>=0.4.1",
        "mpi4py",
        "typing",
        "pytest-cov",
        "pytest-ordering",
    ],
    python_requires=">=3.8",
    zip_safe=False,
)
