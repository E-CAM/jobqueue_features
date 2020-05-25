FROM daskdev/dask-jobqueue:slurm

# Some customisation on jobqueue base images
# Install OpenMPI and mpi4py
RUN conda install --yes -c conda-forge black openmpi mpi4py

COPY slurm.conf /etc/slurm/slurm.conf
