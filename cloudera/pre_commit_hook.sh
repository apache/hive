#!/bin/bash
#
# This script (pre_commit_hook.sh) is executed by hive-gerrit jenkins job
# located at http://unittest.jenkins.cloudera.com/view/gerrit
#
# This script is called from inside the Hive source code directory, and it
# should be used to build and test the current Hive patched code.
#
# hive-gerrit has its own username and home directory in the Jenkins machine

# -e will make the script exit if an error happens on any command executed
set -ex

# Script created by Cloudcat with useful environment information
. /opt/toolchain/toolchain.sh

export JAVA_HOME=$JAVA7_HOME
export PATH=${JAVA_HOME}/bin:${PATH}

# Add any test to be excluded in alphabetical order to keep readability, starting with files, and
# then directories.
declare -a EXCLUDE_TESTS=(
	".*org/apache/hadoop/hive/metastore/.*"
	".*org/apache/hadoop/hive/ql/Test.*"
	".*org/apache/hadoop/hive/ql/exec/.*"
	".*org/apache/hadoop/hive/ql/metadata/.*"
	".*org/apache/hadoop/hive/ql/io/orc/.*"
	".*org/apache/hadoop/hive/ql/parse/.*"
	".*org/apache/hadoop/hive/ql/session/.*"
	".*org/apache/hadoop/hive/ql/security/.*"
	".*org/apache/hadoop/hive/ql/txn/.*"
	".*org/apache/hadoop/hive/ql/udf/.*"
	".*org/apache/hadoop/hive/ql/vector/.*"
	".*org/apache/hive/hcatalog/.*"
	".*org/apache/hive/service/.*"
	".*org/apache/hive/jdbc/.*"
)

function get_excluded_tests() {
	local IFS="|"
	echo -n "${EXCLUDE_TESTS[*]}"
}

function get_regex_excluded_tests() {
	echo -n "%regex[`get_excluded_tests`]"
}

regex_tests=`get_regex_excluded_tests`
mvn clean install -Phadoop-2 -Dtest.excludes.additional="$regex_tests"
cd itests/
mvn clean install -Phadoop-2 -DskipTests
