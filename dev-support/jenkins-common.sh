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
fail() {
  echo "$@" 1>&2
  exit 1
}

# Parses the JIRA/patch to find relavent information.
# Exports two variables of import:
# * BUILD_PROFILE - the profile which the ptest server understands
# * BUILD_OPTS - additional test options to be sent to ptest cli
# * PATCH_URL - the URL to the patch file
process_jira() {
  test -n "$BRANCH" || fail "BRANCH must be specified"
  test -n "$JIRA_ROOT_URL" || fail "JIRA_ROOT_URL must be specified"
  test -n "$JIRA_NAME" || fail "JIRA_NAME must be specified"
  JIRA_TEXT=$(mktemp)
  trap "rm -f $JIRA_TEXT" EXIT
  curl -s -S --location --retry 3 "${JIRA_ROOT_URL}/jira/browse/${JIRA_NAME}" > $JIRA_TEXT
  if [[ "${CHECK_NO_PRECOMMIT_TESTS}" == "true" ]] && grep -q "NO PRECOMMIT TESTS" $JIRA_TEXT
  then
    fail "Test $JIRA_NAME has tag NO PRECOMMIT TESTS"
  fi
  # ensure the patch is actually in the correct state
  if ! grep -q 'Patch Available' $JIRA_TEXT
  then
    fail "$JIRA_NAME is not \"Patch Available\". Exiting."
  fi
  # pull attachments from JIRA (hack stolen from hadoop since rest api doesn't show attachments)
  export PATCH_URL=$(grep -o '"/jira/secure/attachment/[0-9]*/[^"]*' $JIRA_TEXT | \
    grep -v -e 'htm[l]*$' | sort | tail -1 | \
    grep -o '/jira/secure/attachment/[0-9]*/[^"]*')
  if [[ -z "$PATCH_URL" ]]
  then
    fail "Unable to find attachment for $JIRA_NAME"
  fi
  # ensure attachment has not already been tested
  ATTACHMENT_ID=$(basename $(dirname $PATCH_URL))
  if grep -q "ATTACHMENT ID: $ATTACHMENT_ID" $JIRA_TEXT
  then
    fail "Attachment $ATTACHMENT_ID is already tested for $JIRA_NAME"
  fi
  # validate the patch name, parse branch if needed
  shopt -s nocasematch
  PATCH_NAME=$(basename $PATCH_URL)
  # Test examples:
  # HIVE-123.patch HIVE-123.1.patch HIVE-123.D123.patch HIVE-123.D123.1.patch HIVE-123-tez.patch HIVE-123.1-tez.patch
  # HIVE-XXXX.patch, HIVE-XXXX.XX.patch  HIVE-XXXX.XX-branch.patch HIVE-XXXX-branch.patch
  if [[ $PATCH_NAME =~ ^HIVE-[0-9]+(\.[0-9]+)?(-[a-z0-9-]+)?\.(patch|patch.\txt)$ ]]
  then
    if [[ -n "${BASH_REMATCH[2]}" ]]
    then
      BRANCH=${BASH_REMATCH[2]#*-}
    else
      echo "Assuming branch $BRANCH"
    fi
  # HIVE-XXXX.DXXXX.patch or HIVE-XXXX.DXXXX.XX.patch
  elif [[ $PATCH_NAME =~ ^(HIVE-[0-9]+\.)?D[0-9]+(\.[0-9]+)?\.(patch|patch.\txt)$ ]]
  then
    echo "Assuming branch $BRANCH"
  else
    fail "Patch $PATCH_NAME does not appear to be a patch"
  fi
  shopt -u nocasematch
  # append mr2 if needed
  if [[ $BRANCH =~ (mr1|mr2)$ ]]
  then
    profile=$BRANCH
  else
    profile=${BRANCH}-mr2
  fi
  export BUILD_PROFILE=$profile
  build_opts=""
  if grep -q "CLEAR LIBRARY CACHE" $JIRA_TEXT
  then
    echo "Clearing library cache before starting test"
    build_opts="--clearLibraryCache"
  fi
  export BUILD_OPTS=$build_opts
}
