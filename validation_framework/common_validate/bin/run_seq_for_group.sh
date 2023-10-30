#!/bin/bash
./dataproc_profile;


group=$1
echo "create cluster called for $group group"

now="$(date +'%Y-%m-%d_%H:%M:%S')"

file_suffix=$(printf "%s_%s" "$group" "$now")

if [ -z "$group" ]
then
  echo "Invalid group name"
  exit 1
fi

sh create_cluster.sh "group-${group}"

nohup python3 submit_workflow_seq.py --tenant "" --cluster_name "cluster-group-${group}" --job_config "/job_configs/job_config_${group}.json" --app_id "" --scope "" --file "gs://spark-bigquery-with-dependencies_2.11-0.29.0.jar" >> cluster-group-"$file_suffix".log &

echo "tail -f cluster-group-$file_suffix.log"
