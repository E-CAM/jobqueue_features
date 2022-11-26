FROM daskdev/dask-jobqueue:slurm

# Some customisation on jobqueue base images
# Install required Python as well as OpenMPI and mpi4py
ARG PYTHON_VERSION
ARG REQUIREMENTS
RUN conda install mamba -n base -c conda-forge
RUN REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt mamba install --yes -c conda-forge python=${PYTHON_VERSION} openmpi-mpicc mpi4py black ${REQUIREMENTS}

COPY slurm.conf /etc/slurm/slurm.conf
