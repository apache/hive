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

cd $(dirname $0)

HIVE_PTEST_DIR=".hive-ptest"

if [ -n "$GIT_BRANCH" ]; then
  HIVE_BRANCH="${GIT_BRANCH#*/}"
else
  HIVE_BRANCH=$(git status | grep 'On branch' | cut -d' ' -f3)
fi

[ -z "$HIVE_BRANCH" ] && echo "Fatal: Cannot find GIT branch name." && exit 1

export PTEST_PROPERTIES_FILE="cdh5-1.1.x.properties"
export JAVA7_BUILD="1"

mkdir -p $HIVE_PTEST_DIR
cd $HIVE_PTEST_DIR

rm -f parent_buildinfo
wget http://unittest.jenkins.cloudera.com/job/CDH5-Unit-Tests-Aggregate/lastBuild/artifact/parent_buildinfo

cat > buildinfo << EOF
Job: $JOB_NAME
Build ID: $BUILD_ID
EOF

rm -f execute-hive-ptest.sh

curl -O http://github.mtv.cloudera.com/raw/CDH/hive-ptest-conf/master/bin/execute-hive-ptest.sh

exec bash ./execute-hive-ptest.sh --branch $HIVE_BRANCH
