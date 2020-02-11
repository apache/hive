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

set -x
. jenkins-common.sh

test -n "$BRANCH" || fail "BRANCH must be specified"
test -n "$ISSUE_NUM" || fail "ISSUE_NUM must be specified"
test -n "$SSH_HOST" || fail "SSH_HOST must be specified"
test -n "$SSH_KEY" || fail "SSH_KEY must be specified"
test -n "$BUILD_TAG" || fail "BUILD_TAG must be specified"

test -n "$WORKSPACE" || WORKSPACE="$HOME"

export JIRA_NAME="HIVE-${ISSUE_NUM}"
export ROOT=$PWD
export JIRA_ROOT_URL="https://issues.apache.org"
export BUILD_TAG="${BUILD_TAG##jenkins-}"

process_jira

# Jenkins may call this script with BRANCH=trunk to refer to SVN.
# We use git here, so we change to master.
[[ "$BRANCH" = "trunk" ]] && BRANCH="master"

PUBLISH_VARS=(
	BUILD_STATUS=buildStatus
	BUILD_TAG=buildTag
	LOGS_URL=logsURL
	JENKINS_URL=jenkinsURL
	PATCH_URL=patchUrl
	JIRA_NAME=jiraName
	JIRA_URL=jiraUrl
	BRANCH=branch
	REPO=repository
	REPO_NAME=repositoryName
	REPO_TYPE=repositoryType
	TESTS_EXECUTED=numTestsExecuted
	FAILED_TESTS=failedTests
	MESSAGES=messages
)

BUILD_STATUS=0
PATCH_URL="${JIRA_ROOT_URL}${PATCH_URL}"
TESTS_EXECUTED=0
FAILED_TESTS=()
MESSAGES=()

PROFILE_PROPERTIES_FILE="/usr/local/hiveptest/etc/public/${BUILD_PROFILE}.properties"
[[ -f $PROFILE_PROPERTIES_FILE ]] || fail "$PROFILE_PROPERTIES_FILE file does not exist."

profile_properties_get() {
	grep "^$1 = " "$PROFILE_PROPERTIES_FILE" | awk '{print $3}'
}

REPO_TYPE=$(profile_properties_get "repositoryType")
REPO_NAME=$(profile_properties_get "repository")
REPO=$(profile_properties_get "repositoryName")
JIRA_URL=$(profile_properties_get "jiraUrl")

# Avoid showing this information on Jira
set +x
JIRA_USER=$(profile_properties_get "jiraUser")
JIRA_PASS=$(profile_properties_get "jiraPassword")
set -x

JENKINS_URL=$(profile_properties_get "jenkinsURL")
LOGS_URL=$(profile_properties_get "logsURL")

build_ptest2() {
	local path="$1"
	local curpath="$PWD"

	test -d $path || mkdir -p $path
	rm -rf $path
	git clone --depth 1 -b $BRANCH https://github.com/apache/hive.git $path/ || return 1
	cd $path/testutils/ptest2
	mvn clean package -B -DskipTests -Drat.numUnapprovedLicenses=1000 -Dmaven.repo.local=$WORKSPACE/.m2 || return 1

	cd $curpath
}

publish_results() {
	local file="$1"

	build_ptest2 "hive/build" || return 1

	# Avoid showing this information on Jira
	set +x
	java -cp "hive/build/testutils/ptest2/target/*:hive/build/testutils/ptest2/target/lib/*" org.apache.hive.ptest.execution.JIRAService \
			--user "$JIRA_USER" \
			--password "$JIRA_PASS" \
			--file "$file"
	set -x
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

if patch_contains_hms_upgrade "$PATCH_URL"; then
	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $SSH_KEY $SSH_HOST "
		rm -rf hive/ &&
		git clone --depth 1 -b $BRANCH https://github.com/apache/hive.git &&
		cd hive/ &&
		curl ${PATCH_URL} | bash -x testutils/ptest2/src/main/resources/smart-apply-patch.sh - &&
		sudo bash -x testutils/metastore/execute-test-on-lxc.sh --patch \"${PATCH_URL}\" --branch $BRANCH
	"
	BUILD_STATUS=$?
	if [[ $BUILD_STATUS = 0 ]]; then
		tmp_test_log="/tmp/execute-test-on-lxc.sh.log"

		scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $SSH_KEY ${SSH_HOST}:"$tmp_test_log" "$tmp_test_log" || exit 1
		TESTS_EXECUTED=$(cat "$tmp_test_log"  | grep "Executing sql test" | wc -l)

		while read line
		do
			if echo $line | grep 'Test failed' > /dev/null; then
				FAILED_TESTS+=("$line")
			elif echo $line | grep 'Executing sql test' >/dev/null; then
				# Remove 'Executing sql test' line from MESSAGES log to avoid a verbose
				# comment on JIRA
				continue
			fi

			MESSAGES+=("$line")
		done < "$tmp_test_log"

		rm "$tmp_test_log"
	fi

	json_file=$(create_publish_file)
	publish_results "$json_file"
	ret=$?

	rm "$json_file"
	exit $ret
fi
