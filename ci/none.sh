#!/usr/bin/env bash

function jobqueue_before_install {
  # Install miniconda
  ./ci/conda_setup.sh
  export PATH="$HOME/miniconda/bin:$PATH"
  conda install --yes -c conda-forge python=$TRAVIS_PYTHON_VERSION dask distributed flake8 black pytest pytest-asyncio
  pip install git+https://github.com/dask/dask-jobqueue@master --upgrade --no-deps
  # install MPI runtime
  sudo apt-get install -y -q openmpi-bin libopenmpi-dev
}

function jobqueue_install {
  which python
  pip install --no-deps -e .
}

function jobqueue_script {
  flake8 -j auto jobqueue_features
  black --exclude versioneer.py --check .
  pytest --verbose --cov=jobqueue_features -s
}

function jobqueue_after_script {
  echo "Done."
}
