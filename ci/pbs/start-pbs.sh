#!/bin/bash

docker-compose up -d
while [ `docker exec -u pbsuser pbs-master pbsnodes -a | grep "Mom = pbs-slave" | wc -l` -ne 2 ]
do
    echo "Waiting for PBS slave nodes to become available";
    sleep 2
done
echo "PBS properly configured"
