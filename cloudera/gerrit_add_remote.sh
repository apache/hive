#!/bin/bash
#
# This script 'gerrit_add_remote.sh' helps Hive developers to add
# the Cloudera gerrit remote repository to their local repository.
#

 set -e

# Change directory to hive root directory
cd `dirname $0`/..

USER_NAME="$1"

SCRIPT_NAME=`basename $0`
function show_help() {
  echo "Usage: $SCRIPT_NAME <ldap-username>"
  echo "i.e. $SCRIPT_NAME ldap-user"
  echo
}

if [ -z "$USER_NAME" ]; then
  show_help && exit 1
fi

#
# Prepare local github repository
#

echo "Preparing local github repository with Cloudera gerrit information ..."
git remote add gerrit ssh://$USER_NAME@gerrit.sjc.cloudera.com:29418/hive
git fetch gerrit
scp -p -P 29418 $USER_NAME@gerrit.sjc.cloudera.com:hooks/commit-msg .git/hooks/
chmod 755 .git/hooks/commit-msg
echo "Done"
