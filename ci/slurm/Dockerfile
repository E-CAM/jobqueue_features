FROM daskdev/dask-jobqueue:slurm

# Some customisation on jobqueue base images
# Install required Python as well as OpenMPI and mpi4py
ARG PYTHON_VERSION
RUN conda install --yes -c conda-forge python=${PYTHON_VERSION}
RUN conda install --yes -c conda-forge black openmpi-mpicc mpi4py && conda clean -tipy

COPY slurm.conf /etc/slurm/slurm.conf
