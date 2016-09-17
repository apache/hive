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

JIRA_ROOT_URL="https://issues.apache.org"

fail() {
  echo "$@" 1>&2
  exit 1
}

alias echoerr='>&2 echo'

# Parses the patch filename, and returns the branch name.
# If empty or branch is not part of the filename, it returns a default branch
# Note: DEFAULT_BRANCH must exists
get_branch_name() {
  local PATCH_NAME="$1"
  local branch="$2"        # $2 is a default branch

  if [[ -n $PATCH_NAME ]]; then
    # Test PATCH_NAME:
    # HIVE-123.patch HIVE-123.1.patch HIVE-123-tez.patch HIVE-123.1-tez.patch
    # HIVE-XXXX.patch, HIVE-XXXX.XX.patch  HIVE-XXXX.XX-branch.patch HIVE-XXXX-branch.patch
    if [[ $PATCH_NAME =~ ^HIVE-[0-9]+(\.[0-9]+)?(-[A-Za-z0-9.-]+)?\.(patch|patch.\txt)$ ]]; then
      if [[ -n "${BASH_REMATCH[2]}" ]]; then
        branch=${BASH_REMATCH[2]#*-}
      fi
    elif [[ $PATCH_NAME =~ ^(HIVE-[0-9]+\.)?D[0-9]+(\.[0-9]+)?\.(patch|patch.\txt)$ ]]; then
      # It will assume the default branch
      continue
    else
      echoerr "Patch '$PATCH_NAME' does not appear to be a patch"
      return 1
    fi
  fi

  echo $branch
  return 0
}

# Gets the attachment identifier of a JIRA attachment file
get_attachment_id() {
  local jira_attachment_url="$1"

  basename $(dirname $jira_attachment_url)
}

# Fetchs JIRA information, and save it in a file specified as a parameter
initialize_jira_info() {
  local jira_issue="$1" 
  local output_file="$2"

  curl -s -S --location --retry 3 "${JIRA_ROOT_URL}/jira/browse/${jira_issue}" > $output_file
}

# Checks if a JIRA is in 'Patch Available' status
is_patch_available() {
  grep -q "Patch Available" $1
}

# Checks if a JIRA has 'NO PRECOMMIT TESTS' enabled
is_check_no_precommit_tests_set() {
  grep -q "NO PRECOMMIT TESTS" $1
}

# Gets the URL for the JIRA patch attached
get_jira_patch_url() {
  grep -o '"/jira/secure/attachment/[0-9]*/[^"]*' $1 | grep -v -e 'htm[l]*$' | sort | tail -1 | \
    grep -o '/jira/secure/attachment/[0-9]*/[^"]*'
}

# Checks if the patch was already tested
is_patch_already_tested() {
  local attachment_id="$1"
  local test_handle="$2"
  local jira_info_file="$3"

  grep -q "ATTACHMENT ID: $attachment_id $test_handle" $jira_info_file
}

# Gets the branch profile attached to the patch
get_branch_profile() {
  local jira_patch_url="$1"
  local branch_name=`get_branch_name $(basename $jira_patch_url)`

  if [ -n "$branch_name" ]; then
    shopt -s nocasematch
    if [[ $branch_name =~ (mr1|mr2)$ ]]; then
      echo $branch_name
    else
      echo ${branch_name}-mr2
    fi
  fi
}

# Checks if a JIRA patch has 'CLEAR LIBRARY CACHE' enabled
is_clear_cache_set() {
  grep -q "CLEAR LIBRARY CACHE" $1
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
  if test -n "$BUILD_TAG"
  then
    build_postfix=" - ${BUILD_TAG%-*}"
  fi
  if grep -q "ATTACHMENT ID: $ATTACHMENT_ID $build_postfix" $JIRA_TEXT
  then
    fail "Attachment $ATTACHMENT_ID is already tested for $JIRA_NAME"
  fi
  # validate the patch name, parse branch if needed
  shopt -s nocasematch

  # Get branch name
  PATCH_NAME=$(basename $PATCH_URL)
  BRANCH=$(get_branch_name $PATCH_NAME $BRANCH)
  echo "Assuming branch $BRANCH"

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

# Checks if a specified URL patch contains HMS upgrade changes
# Returns 0 if there are changes; non-zero value otherwise.
patch_contains_hms_upgrade() {
	curl -s "$1" | grep "^diff.*metastore/scripts/upgrade/" >/dev/null
}

string_to_upper_case() {
  local str="$1"

  echo "$str" | tr '[:lower:]' '[:upper:]'
}

get_jenkins_job_url() {
  local branch="$1"
  local varname=`string_to_upper_case $branch`_URL
  local joburl=`eval echo \\$${varname}`

  echo $joburl
}
