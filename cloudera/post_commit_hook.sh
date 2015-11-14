#!/bin/bash
#
# This script (post_commit_hook.sh) is executed by CDH*-Hive-Post-Commit jenkins job
# located at http://unittest.jenkins.cloudera.com/view/gerrit
#
# CDH*-Hive-Post-Commit refers to a specific CDH release version, such as:
# CDH5-Hive-Post-Commit, CDH5.5.x-Hive-Post-Commit, etc.
#
# This script is called from inside the Hive source code directory, and it
# should be used to build and test the current Hive code.
#
# hive-gerrit has its own username and home directory in the Jenkins machine

# -e will make the script exit if an error happens on any command executed
set -ex

# Script created by Cloudcat with useful environment information
[ -f /opt/toolchain/toolchain.sh ] && . /opt/toolchain/toolchain.sh

# Use JAVA7_HOME if exists
export JAVA_HOME=${JAVA7_HOME:-$JAVA_HOME}

# If JDK_VERSION exists, then try to get the value from JAVAX_HOME
if [ -n "$JDK_VERSION" ]; then
  # Get JAVAX_HOME value, where X is the JDK version
  java_home=`eval echo \\$JAVA${JDK_VERSION}_HOME`
  if [ -n "$java_home" ]; then
    export JAVA_HOME="$java_home"
  else
    echo "ERROR: USE_JDK_VERSION=$JDK_VERSION, but JAVA${JDK_VERSION}_HOME is not found."
    exit 1
  fi
fi

export PATH=${JAVA_HOME}/bin:${PATH}

# WORKSPACE is an environment variable created by Jenkins, and it is the directory where the build is executed.
# If not set, then default to $HOME
MVN_REPO_LOCAL=${WORKSPACE:-$HOME}/.m2/repository

# Add any test to be excluded in alphabetical order to keep readability, starting with files, and
# then directories.
declare -a EXCLUDE_TESTS=(
  ".*org/apache/hadoop/hive/ql/exec/.*"
  ".*org/apache/hadoop/hive/ql/parse/.*"
  ".*org/apache/hive/hcatalog/mapreduce/.*"
  ".*org/apache/hive/hcatalog/pig/.*"
)

function get_excluded_tests() {
  local IFS="|"
  echo -n "${EXCLUDE_TESTS[*]}"
}

function get_regex_excluded_tests() {
  echo -n "%regex[`get_excluded_tests`]"
}

regex_tests=`get_regex_excluded_tests`
mvn clean install -Phadoop-2 -Dmaven.repo.local="$MVN_REPO_LOCAL" -Dtest.excludes.additional="$regex_tests"
cd itests/
rm -f thirdparty/spark-latest.tar.gz
mvn clean install -Phadoop-2 -Dmaven.repo.local="$MVN_REPO_LOCAL" -DskipTests
