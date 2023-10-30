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