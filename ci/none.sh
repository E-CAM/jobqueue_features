#!/usr/bin/env bash

function jobqueue_before_install {
  # Install MPI runtime
  sudo apt-get update
  sudo apt-get install -y -q openmpi-bin libopenmpi-dev
  # Install miniconda
  ./ci/conda_setup.sh
  export PATH="$HOME/miniconda/bin:$PATH"
  conda install --yes -c conda-forge python=$TRAVIS_PYTHON_VERSION dask distributed flake8 black pytest pytest-asyncio
  pip install git+https://github.com/dask/dask-jobqueue@master --upgrade --no-deps
  # Add checkers
  pip install black --upgrade
  pip install codespell --upgrade
}

function jobqueue_install {
  which python
  pip install -r requirements.txt
  pip install --no-deps -e .
}

function jobqueue_script {
  # flake8 -j auto jobqueue_features
  echo -e "\e[1mRunning black\e[0m"
  black --exclude versioneer.py --check .
  echo -e "\e[1mSuccess...running codespell\e[0m"
  codespell --quiet-level=2
  echo -e "\e[1mSuccess...running pytest\e[0m"
  pytest -s --cov=jobqueue_features
}

function jobqueue_after_script {
  echo "Done."
}
