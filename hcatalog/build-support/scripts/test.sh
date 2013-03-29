#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Run HCatalog tests. This script intends to centralize the different commands
# and options necessary to build and test HCatalog. It should be run before
# committing patches, and from CI.
#
# Set ANT_HOME to specify the version of ant to build with
# (feature of "ant" shell script, not implemented here).

function run_cmd() {
  echo "Running command: ${cmd}"
  ${cmd}
  if [ $? != 0 ]; then
    echo "Failed!"
    exit 1
  fi
}

if [ "${FORREST_HOME}" == "" ]; then
  echo "required environment variable FORREST_HOME not set"
  exit 1
fi

umask 0022
env

cmd='ant clean src-release'
run_cmd

cd build
tar -xzf hcatalog-src-*.tar.gz
cd hcatalog-src-*
echo "Running tests from $(pwd)"

# Build with hadoop23, but do not run tests as they do not pass.
cmd='ant clean package -Dmvn.hadoop.profile=hadoop23'
run_cmd

# Build and run tests with hadoop20. This must happen afterwards so test results
# are available for CI to publish.
cmd='ant -Dtest.junit.output.format=xml clean releaseaudit package test'
if [ "${HUDSON_URL}" == "https://builds.apache.org/" ]; then
  cmd="${cmd} mvn-deploy"
fi
run_cmd
