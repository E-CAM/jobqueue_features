FROM daskdev/dask-jobqueue:slurm

# Some customisation on jobqueue base images
# Install required Python as well as OpenMPI and mpi4py
ARG PYTHON_VERSION
RUN conda install --yes -c conda-forge python=${PYTHON_VERSION} black openmpi mpi4py

COPY slurm.conf /etc/slurm/slurm.conf
