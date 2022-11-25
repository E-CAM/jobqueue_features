# base image
FROM daskdev/dask-jobqueue:pbs
LABEL description="jobqueue_features customisation of dask_jobqueue CI images"

# Some customisation on jobqueue base images
ENV PBS_MASTER pbs-master
RUN yum install -y which && yum clean all

# Install required Python as well as MPICH and mpi4py (and black)
ARG PYTHON_VERSION
RUN ls /etc/pki/tls/certs
RUN REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt conda install --yes python=${PYTHON_VERSION}
RUN conda install --yes -c conda-forge black mpich-mpicc mpi4py && conda clean -tipy

# Copy entrypoint and other needed scripts
COPY ./*.sh /
RUN chmod a+x ./*.sh

# default entrypoint launch pbs master
ENTRYPOINT ["bash", "/master-entrypoint.sh"]
