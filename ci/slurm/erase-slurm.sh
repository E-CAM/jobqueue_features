#!/bin/bash

docker-compose down
if [ $? -eq 0 ]
then
  echo "Successfully brought down containers"
else
  echo "Could not bring down containers" >&2
fi
docker-compose rm -f
if [ $? -eq 0 ]
then
  echo "Successfully removed containers"
else
  echo "Could not remove containers" >&2
fi
docker volume rm slurm_etc_munge slurm_etc_slurm slurm_slurm_jobdir slurm_var_lib_mysql slurm_var_log_slurm
if [ $? -eq 0 ]
then
  echo "Successfully removed container volumes"
else
  echo "Could not remove container volumes" >&2
fi
echo "SLURM completely removed"
