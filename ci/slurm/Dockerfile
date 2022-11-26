FROM daskdev/dask-jobqueue:slurm

# Some customisation on jobqueue base images
# Install required Python as well as OpenMPI and mpi4py
ARG PYTHON_VERSION
ARG REQUIREMENTS
RUN REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt conda install --yes python=${PYTHON_VERSION}
RUN conda install --yes -c conda-forge openmpi-mpicc mpi4py
RUN conda install --yes -c conda-forge black ${REQUIREMENTS} && conda clean -tipy

COPY slurm.conf /etc/slurm/slurm.conf
