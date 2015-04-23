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
set -e
. jenkins-common.sh
test -n "$BRANCH" || fail "BRANCH must be specified"
test -n "$API_ENDPOINT" || fail "API_ENDPOINT must be specified"
test -n "$LOG_ENDPOINT" || fail "LOG_ENDPOINT must be specified"
test -n "$API_PASSWORD" || fail "API_PASSWORD must be specified"
if [[ -n "$ISSUE_NUM" ]]
then
  export JIRA_NAME="HIVE-${ISSUE_NUM}"
fi
export ROOT=$PWD
export JIRA_ROOT_URL="https://issues.apache.org"
export BUILD_TAG="${BUILD_TAG##jenkins-}"
if [[ -n "$JIRA_NAME" ]]
then
  echo $JIRA_NAME
fi
set -x
env

if [[ -n "$JIRA_NAME" ]]
then
  process_jira
fi

profile=$BUILD_PROFILE
if [[ -z "$profile" ]]
then
  profile=$DEFAULT_BUILD_PROFILE
fi
if [[ -z "$profile" ]]
then
  fail "Could not find build profile"
fi

test -d hive/build/ || mkdir -p hive/build/
cd hive/build/
rm -rf hive
git clone --depth 1 https://git-wip-us.apache.org/repos/asf/hive.git
cd hive/testutils/ptest2

mvn clean package -DskipTests -Drat.numUnapprovedLicenses=1000 -Dmaven.repo.local=$WORKSPACE/.m2
set +e
optionalArgs=()
if [[ -n "$JIRA_NAME" ]]
then
  optionalArgs=(--patch "${JIRA_ROOT_URL}${PATCH_URL}" --jira "$JIRA_NAME")
fi
java -cp "target/hive-ptest-1.0-classes.jar:target/lib/*" org.apache.hive.ptest.api.client.PTestClient --endpoint "$API_ENDPOINT" \
  --logsEndpoint "$LOG_ENDPOINT" \
  --command testStart \
  --profile $profile \
  --password $API_PASSWORD \
  --outputDir target/ \
  --testHandle "$BUILD_TAG" \
  ${optionalArgs[@]} ${BUILD_OPTS} "$@"
ret=$?
cd target/
if [[ -f test-results.tar.gz ]]
then
  rm -rf $ROOT/hive/build/test-results/
  tar zxf test-results.tar.gz -C $ROOT/hive/build/
fi
exit $ret
