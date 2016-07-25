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
export JIRA_NAME="HIVE-${ISSUE_NUM}"
export JIRA_ROOT_URL="https://issues.apache.org"
export BRANCH=trunk
echo $JIRA_NAME

process_jira

# sanity check the profile
case "$BUILD_PROFILE" in
  trunk-mr2)
   test -n "$TRUNK_URL" || fail "TRUNK_URL must be specified"
   url="$TRUNK_URL&ISSUE_NUM=$ISSUE_NUM"
  ;;
  branch-1-mr1|branch-1-mr2)
   test -n "$BRANCH1_URL" || fail "BRANCH1_URL must be specified"
   url="$BRANCH1_URL&ISSUE_NUM=$ISSUE_NUM"
  ;;
  spark-mr2)
   test -n "$SPARK_URL" || fail "SPARK_URL must be specified"
   url="$SPARK_URL&ISSUE_NUM=$ISSUE_NUM"
  ;;
  encryption-mr2)
   test -n "$ENCRYPTION_URL" || fail "ENCRYPTION_URL must be specified"
   url="$ENCRYPTION_URL&ISSUE_NUM=$ISSUE_NUM"
  ;;
  parquet-mr2)
   test -n "$PARQUET_URL" || fail "PARQUET_URL must be specified"
   url="$PARQUET_URL&ISSUE_NUM=$ISSUE_NUM"
  ;;
  beeline-cli-mr2)
   test -n "$BEELINE_CLI_URL" || fail "BEELINE_CLI_URL must be specified"
   url="$BEELINE_CLI_URL&ISSUE_NUM=$ISSUE_NUM"
  ;;
  java8-mr2)
   test -n "$JAVA8_URL" || fail "JAVA8_URL must be specified"
   url="$JAVA8_URL&ISSUE_NUM=$ISSUE_NUM"
  ;;
  branch-2.1-mr2)
   test -n "$BRANCH21_URL" || fail "BRANCH21_URL must be specified"
   url="$BRANCH21_URL&ISSUE_NUM=$ISSUE_NUM"
  ;;
  *)
  echo "Unknown profile '$BUILD_PROFILE'"
  exit 1
  ;;
esac

# Execute jenkins job for HMS upgrade tests if needed
if patch_contains_hms_upgrade "${JIRA_ROOT_URL}$PATCH_URL"; then
  test -n "$HMS_UPGRADE_URL" || fail "HMS_UPGRADE_URL must be specified"
  echo "Calling HMS upgrade testing job..."
  curl -v -i "${HMS_UPGRADE_URL}&ISSUE_NUM=${ISSUE_NUM}&BRANCH=${BRANCH}"
fi

# Execute jenkins job for specific profile
echo "Calling Precommit $BRANCH Build..."
curl -v -i "$url"
