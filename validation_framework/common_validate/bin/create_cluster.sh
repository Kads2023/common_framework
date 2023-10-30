#!/bin/bash
./dataproc_profile;

group=$1
echo "create cluster called for $group group"

if [ -z "$group" ]
then
  echo "Invalid group name"
  exit 1
fi

retry=1
retry_threshold=3
while [ $retry -le $retry_threshold ]
do
	echo -e "\nAttempt-$retry to start the cluster"
	create_result=$(/cloudcli/cloudprovisioncli/cloud dataproc cluster create \
		/validation_conf/cluster_configs/cluster_config_"${group}".json \
		--appId <> --scope prod)
	echo "$create_result"

	status_result=$(echo "$create_result" | grep "Status: RUNNING")
	if [ -z "$status_result" ]
	then
		echo "Problem in running the server. Retrying after few seconds"
		if [ $retry -eq $retry_threshold ]
		then
			echo "Failed to create the cluster. Please check."
			exit 1
		else
			sleep 3
			retry=$((retry + 1))
		fi
	else
		echo "Server is up and running."
		exit 0
	fi
done