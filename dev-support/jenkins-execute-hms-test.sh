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

# This script executes all hive metastore upgrade scripts on an specific
# database server in order to verify that upgrade scripts are working
# properly.

# This script is run on jenkins only, and it creates some LXC containers
# in order to execute the metastore-upgrade-tests for different
# server configurations.

set -e -x
. jenkins-common.sh

test -n "$BRANCH" || fail "BRANCH must be specified"
test -n "$ISSUE_NUM" || fail "ISSUE_NUM must be specified"
test -n "$SSH_HOST" || fail "SSH_HOST must be specified"
test -n "$SSH_KEY" || fail "SSH_KEY must be specified"
test -n "$LOG_ENDPOINT" || fail "LOG_ENDPOINT must be specified"
test -n "$JENKINS_URL" || fail "JENKINS_URL must be specified"
test -n "$API_PASSWORD" || fail "API_PASSWORD must be specified"

test -n "$WORKSPACE" || WORKSPACE="$HOME"

export JIRA_NAME="HIVE-${ISSUE_NUM}"
export ROOT=$PWD
export JIRA_ROOT_URL="https://issues.apache.org"
export BUILD_TAG="${BUILD_TAG##jenkins-}"

process_jira

PUBLISH_VARS=(
	BUILD_STATUS=buildStatus
	BUILD_TAG=buildTag
	LOG_ENDPOINT=logsURL
	JENKINS_URL=jenkinsURL
	PATCH_URL=patchUrl
	JIRA_NAME=jiraName
	JIRA_URL=jiraUrl
	BRANCH=branch
	REPO=repository
	REPO_NAME=repositoryName
	TESTS_EXECUTED=numTestsExecuted
	FAILED_TESTS=failedTests
	MESSAGES=messages
)

BUILD_STATUS=0
PATCH_URL="${JIRA_ROOT_URL}${PATCH_URL}"
JIRA_URL="${JIRA_ROOT_URL}/jira"
REPO_NAME="trunk" # not used, but JIRAService asks for it
REPO="trunk"      # not used, but JIRAService asks for it
TESTS_EXECUTED=0
FAILED_TESTS=()
MESSAGES=()

build_ptest2() {
	local path="$1"
	local curpath="$PWD"

	test -d $path || mkdir -p $path
	rm -rf $path/ptest2
	svn co http://svn.apache.org/repos/asf/hive/$BRANCH/testutils/ptest2/ $path/ptest2
	cd $path/ptest2
	mvn clean package -DskipTests -Drat.numUnapprovedLicenses=1000 -Dmaven.repo.local=$WORKSPACE/.m2

	cd $curpath
}

add_json_object() {
	echo -n "\"$2\" : \"$3\"" >> $1
}

add_json_array() {
	echo -n "\"$2\" : [ $3 ]" >> $1
}

create_publish_file() {
	local json_file=$(mktemp)
	local arr_length=${#PUBLISH_VARS[@]}

	vars_added=0
	echo "{" > $json_file
	for i in ${PUBLISH_VARS[@]}
	do
		var=${i%=*}
		key=${i#*=}
		val=

		# Treat this as an array
		if [[ $var = "FAILED_TESTS" ]]; then
			if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
				val=$(printf "\"%s\","  "${FAILED_TESTS[@]}")
				val=${val%?}
			fi
			add_json_array $json_file $key "$val"
		elif [[ $var = "MESSAGES" ]]; then
			if [[ ${#MESSAGES[@]} -gt 0 ]]; then
				val=$(printf "\"%s\","  "${MESSAGES[@]}")
				val=${val%?}
			fi
			add_json_array $json_file $key "$val"
		else
			val=${!var}
			add_json_object $json_file $key $val
		fi

		vars_added=$((vars_added+1))

		if [[ $vars_added -lt $arr_length ]]; then
			echo "," >> $json_file
		else
			echo >> $json_file
		fi
	done
	echo "}" >> $json_file

	echo $json_file
}

wget "${PATCH_URL}" -O /tmp/${JIRA_NAME}.patch
if cat /tmp/${JIRA_NAME}.patch | grep "^diff.*metastore/scripts/upgrade/" >/dev/null; then
	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $SSH_KEY $SSH_HOST "
		rm -rf metastore/ &&
		svn co http://svn.apache.org/repos/asf/hive/$BRANCH/testutils/metastore metastore &&
		sudo bash -x metastore/execute-test-on-lxc.sh --patch \"${PATCH_URL}\" --branch $BRANCH
	"
	ret=$?
	if [[ $ret = 0 ]]; then
		scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $SSH_KEY ${SSH_HOST}:/tmp/execute-test-on-lxc.sh.log /tmp || exit 1
		TESTS_EXECUTED=$(cat /tmp/execute-test-on-lxc.sh.log  | grep "Executing sql test" | wc -l)

		while read line
		do
			if echo $line | grep 'Test failed' > /dev/null; then
				FAILED_TESTS+=("$line")
			fi

			MESSAGES+=("$line")
		done < /tmp/execute-test-on-lxc.sh.log
	fi

	# Publish results to JIRA only if there were errors during the tests
	BUILD_STATUS=$ret
	if [[ $BUILD_STATUS -gt 0 ]] || [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
		build_ptest2 "hive/build"

		json_file=$(create_publish_file)
		java -cp "hive/build/ptest2/target/hive-ptest-1.0-classes.jar:hive/build/ptest2/target/lib/*" org.apache.hive.ptest.execution.JIRAService \
			--user "hive" \
			--password "$API_PASSWORD" \
			--file $json_file
		ret=$?
	fi

	exit $ret
fi
