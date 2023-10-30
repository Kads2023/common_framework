#!/bin/bash

# Function to execute the command with retry logic
function run_with_retry() {
  local -r cmd=("$@")
  for ((retry = 1; retry <= 10; retry++)); do
    printf '\nAttempt-%s to execute the command.\n' "$retry"
    if "${cmd[@]}"; then
      return 0
    fi
    sleep 30
  done

  echo "Failed to run command: ${cmd[*]}"
  exit 1
}


echo "Installing validation_framework wheel file"
run_with_retry pip install https://artifactory/spark-bq/validation_framework-268.0-py3-none-any.whl -i https://artifactory/api/pypi/python/simple

echo "installing dependencies started"
pip install "datacompy==0.8.4" -i https://artifactory/api/pypi/python/simple
pip install "google-cloud-bigquery>=2.29.0" -i https://artifactory/api/pypi/python/simple
pip install "google-cloud-storage>=1.42.3" -i https://artifactory/api/pypi/python/simple
pip install "google-cloud-core>=2.1.0" -i https://artifactory/api/pypi/python/simple
pip install "google-api-core>=2.1.0" -i https://artifactory/api/pypi/python/simple
pip install "google-cloud>=0.34.0" -i https://artifactory/api/pypi/python/simple
echo "installing dependencies completed"

python_home=$(python3 -c 'import site; print(site.getsitepackages()[0])')
echo "python home is $python_home"
gcloud_path="$python_home/google/cloud/"

echo "downloading the spark-dependency-file"
curl "https://artifactory/spark-bq/spark.zip" --output spark.zip
unzip spark.zip -d "$gcloud_path"
echo "unzip done"
ls "$gcloud_path/spark/"

echo "init action completed"