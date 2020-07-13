FROM daskdev/dask-jobqueue:slurm

# Some customisation on jobqueue base images
# Install OpenMPI and mpi4py
RUN conda install --yes -c conda-forge black openmpi mpi4py
RUN git clone https://github.com/E-CAM/jobqueue_features.git

COPY slurm.conf /etc/slurm/slurm.conf
COPY jobqueue_features_slurm.yaml /jobqueue_features/jobqueue_features/jobqueue_features.yaml

