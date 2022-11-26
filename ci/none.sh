#!/usr/bin/env bash

function jobqueue_before_install {
  # Install miniconda
  ./ci/conda_setup.sh
  # Default to Python 3.8
  if [ -z ${PYTHON_VERSION+x} ]; then export PYTHON_VERSION=3.8; else echo "Python version is set to '$PYTHON_VERSION'"; fi
  # Use Ubuntu location for CA certs
  PATH="$HOME/miniconda/bin:$PATH" REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt conda install --yes python=${PYTHON_VERSION}
  PATH="$HOME/miniconda/bin:$PATH" conda install --yes -c conda-forge flake8 black pytest pytest-asyncio codespell openmpi
  # also install OpenMPI and mpi4py
  PATH="$HOME/miniconda/bin:$PATH" conda install --yes -c conda-forge openmpi-mpicc mpi4py
  export REQUIREMENTS=$(cat ./requirements.txt | grep -v jobqueue)
  PATH="$HOME/miniconda/bin:$PATH" conda install --yes -c conda-forge $REQUIREMENTS 
}

function jobqueue_install {
  which python
  # Make sure requirements are met
  # PATH="$HOME/miniconda/bin:$PATH" pip install --upgrade -r requirements.txt
  PATH="$HOME/miniconda/bin:$PATH" pip install --upgrade dask_jobqueue dask distributed
  PATH="$HOME/miniconda/bin:$PATH" pip install --no-deps -e .
}

function jobqueue_script {
  # flake8 -j auto jobqueue_features
  echo -e "\e[1mRunning black\e[0m"
  PATH="$HOME/miniconda/bin:$PATH" black --exclude versioneer.py --check .
  echo -e "\e[1mSuccess...running codespell\e[0m"
  PATH="$HOME/miniconda/bin:$PATH" codespell --quiet-level=2
  echo -e "\e[1mSuccess...running pip list\e[0m"
  PATH="$HOME/miniconda/bin:$PATH" pip list
  echo -e "\e[1mRunning pytest...\e[0m"
  PATH="$HOME/miniconda/bin:$PATH" OMPI_MCA_rmaps_base_oversubscribe=1 OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 pytest --verbose -s --cov=jobqueue_features
}

function jobqueue_after_script {
  echo "Done."
}
