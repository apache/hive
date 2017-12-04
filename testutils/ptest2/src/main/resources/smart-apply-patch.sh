#!/usr/bin/env bash
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

set -e

PATCH_FILE=$1
DRY_RUN=$2
if [ -z "$PATCH_FILE" ]; then
  echo usage: $0 patch-file
  exit 1
fi

# Cleanup handler for temporary files
TOCLEAN=""
cleanup() {
  [ "x$TOCLEAN" != "x" ] && rm $TOCLEAN
  exit $1
}
trap "cleanup 1" HUP INT QUIT TERM

# Allow passing "-" for stdin patches
if [ "$PATCH_FILE" == "-" ]; then
  PATCH_FILE=/tmp/tmp.in.$$
  cat /dev/fd/0 > $PATCH_FILE
  TOCLEAN="$TOCLEAN $PATCH_FILE"
fi

if git apply -p0 -3 --check $PATCH_FILE 2>&1 > /dev/null; then
  PLEVEL=0

  # if the patch applied at P0 there is the possibility that all we are doing
  # is adding new files and they would apply anywhere. So try to guess the
  # correct place to put those files.

  CHANGED_FILES=/tmp/tmp.paths.2.$$
  TOCLEAN="$TOCLEAN $CHANGED_FILES"

  git apply -p0 -3 --stat $PATCH_FILE | head -1 | awk '{print $1}' > $CHANGED_FILES

  if [ ! -s $CHANGED_FILES ]; then
    echo "Error: Patch dryrun couldn't detect changes the patch would make. Exiting."
    cleanup 1
  fi

  #first off check that all of the files do not exist
  FOUND_ANY=0
  for CHECK_FILE in $(cat $CHANGED_FILES)
  do
    if [[ -f $CHECK_FILE ]]; then
      FOUND_ANY=1
    fi
  done

  if [[ "$FOUND_ANY" = "0" ]]; then
    #all of the files are new files so we have to guess where the correct place to put it is.

    # if all of the lines start with a/ or b/, then this is a git patch that
    # was generated without --no-prefix
    if ! grep -qv '^a/\|^b/' $CHANGED_FILES ; then
      echo Looks like this is a git patch. Stripping a/ and b/ prefixes
      echo and incrementing PLEVEL
      PLEVEL=$[$PLEVEL + 1]
      sed -i -e 's,^[ab]/,,' $CHANGED_FILES
    fi

  fi
elif git apply -p1 -3 --check $PATCH_FILE 2>&1 > /dev/null; then
  PLEVEL=1
elif git apply -p2 -3 --check $PATCH_FILE 2>&1 > /dev/null; then
  PLEVEL=2
else
  echo "The patch does not appear to apply with p0, p1, or p2";
  cleanup 1;
fi

# If this is a dry run then exit instead of applying the patch
if [[ -n $DRY_RUN ]]; then
  cleanup 0;
fi

echo Going to apply patch with: git apply -p$PLEVEL
git apply -p$PLEVEL -3 $PATCH_FILE

cleanup $?