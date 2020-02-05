FROM giovtorres/slurm-docker-cluster

RUN curl -o miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH
ARG PYTHON_VERSION
RUN conda install --yes -c conda-forge python=$PYTHON_VERSION flake8 black pytest pytest-asyncio
# Install OpenMPI and mpi4py
RUN conda install --yes -c conda-forge python=$PYTHON_VERSION openmpi mpi4py
# Install latest jobqueue
# RUN pip install git+https://github.com/dask/jobqueue --upgrade --no-deps

ENV LC_ALL en_US.UTF-8

COPY slurm.conf /etc/slurm/slurm.conf
