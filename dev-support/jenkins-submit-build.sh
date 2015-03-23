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
  trunk-mr1|trunk-mr2)
   test -n "$TRUNK_URL" || fail "TRUNK_URL must be specified"
   url="$TRUNK_URL&ISSUE_NUM=$ISSUE_NUM"
  ;;
  spark-mr2)
   test -n "$SPARK_URL" || fail "SPARK_URL must be specified"
   url="$SPARK_URL&ISSUE_NUM=$ISSUE_NUM"
  ;;
  encryption-mr2)
   test -n "$ENCRYPTION_URL" || fail "ENCRYPTION_URL must be specified"
   url="$ENCRYPTION_URL&ISSUE_NUM=$ISSUE_NUM"
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
