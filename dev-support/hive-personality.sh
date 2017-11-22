#!/usr/bin/env bash
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

# Override these to match Apache Hadoop's requirements

personality_plugins "maven,asflicense,author,checkstyle,findbugs,javac,compile,javadoc,whitespace,xml,jira"

## @description  Globals specific to this personality
## @audience     private
## @stability    evolving
function personality_globals
{
  # shellcheck disable=SC2034
  BUILDTOOL=maven
  #shellcheck disable=SC2034
  PATCH_BRANCH_DEFAULT=master
  #shellcheck disable=SC2034
  PATCH_NAMING_RULE="http://cwiki.apache.org/confluence/display/Hive/HowToContribute"
  #shellcheck disable=SC2034
  JIRA_ISSUE_RE='^HIVE-[0-9]+$'
  #shellcheck disable=SC2034
  GITHUB_REPO="apache/hive"
  #shellcheck disable=SC2034
  PYLINT_OPTIONS="--indent-string='  '"
  #shellcheck disable=SC2034
  FINDBUGS_SKIP_MAVEN_SOURCE_CHECK=true
  #shellcheck disable=SC2034
  WHITESPACE_EOL_IGNORE_LIST='.*.q.out'
  #shellcheck disable=SC2034
  WHITESPACE_TABS_IGNORE_LIST='.*.q.out'
}

## @description  Queue up modules for this personality
## @audience     private
## @stability    evolving
## @param        repostatus
## @param        testtype
function personality_modules
{
  declare repostatus=$1
  declare testtype=$2
  declare extra="-DskipTests -Pitests"

  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  # this always makes sure the local repo has a fresh
  # copy of everything per pom rules.
  if [[ ${repostatus} == branch
     && ${testtype} == mvninstall ]] ||
     [[ "${BUILDMODE}" == full ]];then
    personality_enqueue_module . ${extra}
    return
  fi

  case ${testtype} in
    asflicense)
      # this is very fast and provides the full path if we do it from
      # the root of the source
      personality_enqueue_module .
      return
    ;;
    findbugs)
      extra="${extra},findbugs"
    ;;
    javadoc)
      extra="${extra},javadoc"
    ;;
  esac

  # We add the changed modules with the appropriate extras
  for module in "${CHANGED_MODULES[@]}"; do
    if [ "${module}" == "itests" ]; then
      # We do not want to test itests itself
      continue;
    fi
    # Skip findbugs if there is no java source in the module
    if [[ ${testtype} = findbugs ]]; then
      if [[ ! -d "${module}/src/java" && ! -d "${module}/src/main/java" ]]; then
        continue;
      fi
    fi
    personality_enqueue_module "${module}" "${extra}"
  done
}

