#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

BUCKET=/s3v/test

wait_and_monitor_deployment() {
  local app="$1"
  until kubectl rollout status deployment "$app" --timeout=30s; do
    kubectl get deployment "$app"
    kubectl get pods -l app="$app"
    kubectl describe deployment "$app"
  done
}

helm repo add ozone https://apache.github.io/ozone-helm-charts/
helm install ozone ozone/ozone --version 0.2.0 --values itests/test-docker/helm/ozone/values.yaml --wait
# Wait for a while because Ozone's Helm chart does not have readiness probes...
sleep 10
if kubectl exec statefulset/ozone-om -- ozone sh bucket info "$BUCKET" >/dev/null 2>&1; then
  echo "Bucket already exists. Skipping."
else
  echo "Bucket does not exist. Creating..."
  kubectl exec statefulset/ozone-om -- ozone sh bucket create "$BUCKET"
fi

base_dir=$(dirname "$(cd "$(dirname "$0")" || exit; pwd)")

kubectl apply -f "$base_dir/k8s/*"

wait_and_monitor_deployment hive-metastore
wait_and_monitor_deployment hive
wait_and_monitor_deployment beeline

mkdir -p "$base_dir/target"
nohup kubectl port-forward service/hive-metastore 9083 > "$base_dir/target/hive-metastore-thrift.log" 2>&1 &
echo $! > "$base_dir/target/hive-metastore-thrift.pid"
nohup kubectl port-forward service/hive-metastore 9001 > "$base_dir/target/hive-metastore-rest.log" 2>&1 &
echo $! > "$base_dir/target/hive-metastore-rest.pid"
nohup kubectl port-forward service/ozone-s3g-rest 9878 > "$base_dir/target/ozone-s3g.log" &
echo $! > "$base_dir/target/ozone-s3g.pid"
