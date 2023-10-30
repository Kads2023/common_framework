## Steps to execute the framework in Dataproc

- We need to install the following package to build the wheel file
``` commandline
pip install wheel
```

- Build the whole project as a wheel file
``` commandline
python3 setup.py bdist_wheel
```

- Upload the `init_action_validation.sh` to artifactory  - One time during project initial setup
```commandline
curl --user <user_name>:<passord> 
     --upload-file validation_framework/common/bin/init_action_validation.sh 
     "https://artifactory/spark-bq/init_action_validation.sh"
```

- Upload the built wheel file to artifactory


- Upload the main driver file to GCS


- Submit the dataproc job using cloudcli command
```commandline
nohup /cloudcli/cloudprovisioncli/cloud dataproc job submit --type PY_SPARK 
--file gs://dependency/lib/
spark-bigquery-with-dependencies_2.11-0.29.0.jar 
--main gs://test/lib/compare_data.py 
--appId <> --scope <> --num-workers 64 --worker-type n1-highmem-32 
--init-actions init_action_validation.sh > py_compare.log &
```

- Check the dataproc logs using cloudcli command
```commandline
/cloudcli/cloudprovisioncli/cloud dataproc workflow logs-v2 \
--workflow-id <workflow_id/from/submitted/job/logs> --job-id <job_id/from/submitted/job/logs> \
--appId <> --scope <>
```



To Run sequentially and test
============================

sh /create_cluster.sh 'group-11'

. ./dataproc_profile;
python3 submit_workflow_seq.py --tenant "<>" --cluster_name "validation-cluster-group-11" 
--job_config "/validation_conf/job_configs/job_config_11.json" --app_id "<>" --scope "<>" 
--file "gs://example-validations/spark-bigquery-with-dependencies_2.11-0.29.0.jar"

. ./dataproc_profile;
/cloudcli/cloudprovisioncli/cloud dataproc cluster delete \
validation-cluster-group-11 --appId <> --scope <>

====================================
Useful Generic DataProc Commands
====================================

To List DataProc Jobs
======================
/cloudcli/cloudprovisioncli/cloud dataproc job list --appId <> --scope <>

/cloudcli/cloudprovisioncli/cloud dataproc job list --appId <> --scope <> | grep "PROVISIONING"

/cloudcli/cloudprovisioncli/cloud dataproc job list --appId <> --scope <> | grep "RUNNING"

To List Clusters
======================
/cloudcli/cloudprovisioncli/cloud dataproc cluster list --appId <> --scope <>

To Submit DataProc Job
======================
/cloudcli/cloudprovisioncli/cloud dataproc job submit --type SPARK_SQL --main "SELECT \"Hello World\" as message;select reflect(\"java.lang.Thread\", \"sleep\", bigint(100000));" --appId <> --scope <>

To Cancel DataProc Job
======================
/cloudcli/cloudprovisioncli/cloud dataproc job cancel <> --appId <> --scope <>

To Create Cluster
======================
/cloudcli/cloudprovisioncli/cloud dataproc cluster create /<PATH>/cluster_config.json --appId <> --scope <>

To Submit DataProc Workflow (using an existing cluster)
======================
/cloudcli/cloudprovisioncli/cloud dataproc workflow submit --file gs://spark-snowflake_2.11-2.9.3-spark_2.4.jar,gs://spark-bigquery-with-dependencies_2.11-0.29.0.jar,gs://snowflake-ingest-sdk-0.10.3.jar,gs://snowflake-jdbc-3.13.14.jar --cluster-name <CLUSTER_NAME> --job-config /<PATH>/job_config.json --appId <> --scope <> --disable-workflow-timeout

To Check DataProc Logs
======================
/cloudcli/cloudprovisioncli/cloud dataproc workflow logs-v2 --workflow-id fea29c49-84ea-3dd9-bfba-dd9608b8019f --job-id PY_SPARK-UrhKO-ao2pgz7rgfgss --appId <> --scope <>




