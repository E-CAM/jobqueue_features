# base image
FROM daskdev/dask-jobqueue:pbs
LABEL description="jobqueue_features customisation of dask_jobqueue CI images"

# Some customisation on jobqueue base images
ENV PBS_MASTER pbs-master
RUN yum install -y which && yum clean all

# Install MPICH and mpi4py (and black)
RUN conda install --yes -c conda-forge black mpi4py && conda install --yes -c conda-forge/label/cf202003 mpich  && conda clean -tipy

# Copy entrypoint and other needed scripts
COPY ./*.sh /
RUN chmod a+x ./*.sh

# default entrypoint launch pbs master
ENTRYPOINT ["bash", "/master-entrypoint.sh"]
