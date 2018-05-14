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
set -x

. jenkins-common.sh

# Build the ptest framework locally
build_ptest_client() {
	[ -z "$PTEST_BUILD_DIR" ] && echoerr "Error: Cannot build ptest: PTEST_BUILD_DIR is not defined."

	test -d $PTEST_BUILD_DIR || mkdir -p $PTEST_BUILD_DIR
	cd $PTEST_BUILD_DIR &&	rm -rf hive

  unset GIT_CLONE_ARGS
  if [ -n "${PTEST_GIT_BRANCH}" ]; then
    GIT_CLONE_ARGS=" -b ${PTEST_GIT_BRANCH}"
  fi
  if [ -z "${PTEST_GIT_REPO}" ]; then
    PTEST_GIT_REPO=https://github.com/apache/hive.git
  fi
  GIT_CLONE_ARGS=${GIT_CLONE_ARGS}" ${PTEST_GIT_REPO} hive"

	git clone --depth 1 ${GIT_CLONE_ARGS}
	cd hive/testutils/ptest2
	mvn clean package -B -DskipTests -Drat.numUnapprovedLicenses=1000 -Dmaven.repo.local=$MVN_REPO_LOCAL
}

# Call the ptest server to run all tests
call_ptest_server() {
	[ -z "$PTEST_BUILD_DIR" ] && echoerr "Error: Cannot build ptest: PTEST_BUILD_DIR is not defined."

	read -s  -p "JIRA password: " JIRA_PASSWORD

	build_ptest_client || return 1

	local PTEST_CLASSPATH="$PTEST_BUILD_DIR/hive/testutils/ptest2/target/hive-ptest-3.0-classes.jar:$PTEST_BUILD_DIR/hive/testutils/ptest2/target/lib/*"

	java -cp "$PTEST_CLASSPATH" org.apache.hive.ptest.api.client.PTestClient --command testStart \
		--outputDir "$PTEST_BUILD_DIR/hive/testutils/ptest2/target" --password "$JIRA_PASSWORD" "$@"
}

# Unpack all test results
unpack_test_results() {
	[ -z "$PTEST_BUILD_DIR" ] && echoerr "Error: Cannot build ptest: PTEST_BUILD_DIR is not defined."

	cd "$PTEST_BUILD_DIR/hive/testutils/ptest2/target"
	if [[ -f test-results.tar.gz ]]; then
	  rm -rf $PTEST_BUILD_DIR/test-results/
	  tar zxf test-results.tar.gz -C $PTEST_BUILD_DIR
	fi
}

# Check required arguments
test -n "$TEST_HANDLE" || fail "TEST_HANDLE must be specified and cannot be empty."
test -n "$PTEST_API_ENDPOINT" || fail "PTEST_API_ENDPOINT must be specified and cannot be empty."
test -n "$PTEST_LOG_ENDPOINT" || fail "PTEST_LOG_ENDPOINT must be specified and cannot be empty."

# WORKSPACE is an environment variable created by Jenkins, and it is the directory where the build is executed.
# If not set, then default to $HOME
MVN_REPO_LOCAL=${WORKSPACE:-$HOME}/.m2/repository

# Directory where to build the ptest framework
PTEST_BUILD_DIR="$PWD/hive/build"

# Default profile in case a patch does not have a profile assigned, or 
# if this script is executed without JIRA_ISSUE defined
DEFAULT_BUILD_PROFILE="master-mr2"

# Optional parameters that might be passed to the ptest client command
optionalArgs=()

if [ -n "$JIRA_ISSUE" ]; then
	JIRA_INFO_FILE=`mktemp`
	trap "rm -f $JIRA_INFO_FILE" EXIT

	initialize_jira_info $JIRA_ISSUE $JIRA_INFO_FILE || fail "Error: Cannot initialize JIRA information."

	if ! is_patch_available $JIRA_INFO_FILE; then
		fail "$JIRA_ISSUE is not 'Patch Available'."
	fi

	if is_check_no_precommit_tests_set $JIRA_INFO_FILE; then
		fail "$JIRA_ISSUE has tag NO PRECOMMIT TESTS"
	fi

	JIRA_PATCH_URL=`get_jira_patch_url $JIRA_INFO_FILE`
	if [ -z "$JIRA_PATCH_URL" ]; then
		fail "Unable to find attachment for $JIRA_ISSUE"
	fi

	attachment_id=`get_attachment_id $JIRA_PATCH_URL`
	if is_patch_already_tested "$attachment_id" "$TEST_HANDLE" "$JIRA_INFO_FILE"; then
		fail "attachment $attachment_id is already tested for $JIRA_ISSUE"
	fi

  # Use the BUILD_PROFILE if it is provided. 
  if [ -z ${BUILD_PROFILE} ]; then
	  BUILD_PROFILE=`get_branch_profile $JIRA_PATCH_URL $JIRA_INFO_FILE`
	  if [ -z "$BUILD_PROFILE" ]; then
	  	BUILD_PROFILE="$DEFAULT_BUILD_PROFILE"
	  fi
  fi

	if is_clear_cache_set $JIRA_INFO_FILE; then
		optionalArgs+=(--clearLibraryCache)
	fi

	optionalArgs+=(--patch "${JIRA_ROOT_URL}${JIRA_PATCH_URL}" --jira "$JIRA_ISSUE")

	echo "ISSUE: $JIRA_ISSUE PROFILE: $BUILD_PROFILE"
else
	# If not JIRA is specified, and no BUILD_PROFILE provided, then use a default profile
  if [ -z ${BUILD_PROFILE} ]; then
  	BUILD_PROFILE="$DEFAULT_BUILD_PROFILE"
  fi

	echo "ISSUE: unspecified PROFILE: $BUILD_PROFILE"
fi

set +e

call_ptest_server --testHandle "$TEST_HANDLE" --endpoint "$PTEST_API_ENDPOINT" --logsEndpoint "$PTEST_LOG_ENDPOINT" \
	--profile "$BUILD_PROFILE" ${optionalArgs[@]} "$@"

ret=$?

unpack_test_results

exit $ret
