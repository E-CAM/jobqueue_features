FROM daskdev/dask-jobqueue:slurm

# Some customisation on jobqueue base images
# Remove the previous conda installation and use our own, also installing OpenMPI and mpi4py
RUN rm -r /opt/anaconda
RUN curl -o miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \

# Install required Python as well as OpenMPI, mpi4py and most of the requirements
ARG PYTHON_VERSION
ARG REQUIREMENTS
RUN conda install mamba -n base -c conda-forge
RUN REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt mamba install --yes -c conda-forge python=${PYTHON_VERSION} openmpi-mpicc mpi4py black ${REQUIREMENTS}

COPY slurm.conf /etc/slurm/slurm.conf
