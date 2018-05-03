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

set -o pipefail

## @description  Print a message to stderr
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
function yetus_error
{
  echo "$*" 1>&2
}

## @description  Given a filename or dir, return the absolute version of it
## @audience     public
## @stability    stable
## @param        directory
## @replaceable  no
## @return       0 success
## @return       1 failure
## @return       stdout abspath
function yetus_abs
{
  declare obj=$1
  declare dir
  declare fn

  if [[ ! -e ${obj} ]]; then
    return 1
  elif [[ -d ${obj} ]]; then
    dir=${obj}
  else
    dir=$(dirname -- "${obj}")
    fn=$(basename -- "${obj}")
    fn="/${fn}"
  fi

  dir=$(cd -P -- "${dir}" >/dev/null 2>/dev/null && pwd -P)
  if [[ $? = 0 ]]; then
    echo "${dir}${fn}"
    return 0
  fi
  return 1
}


WANTED="$1"
shift
ARGV=("$@")

HIVE_YETUS_VERSION=${HIVE_YETUS_VERSION:-0.5.0}
BIN=$(yetus_abs "${BASH_SOURCE-$0}")
BINDIR=$(dirname "${BIN}")

###
###  if YETUS_HOME is set, then try to use it
###
if [[ -n "${YETUS_HOME}"
   && -x "${YETUS_HOME}/bin/${WANTED}" ]]; then
  exec "${YETUS_HOME}/bin/${WANTED}" "${ARGV[@]}"
fi

#
# this directory is ignored by git and maven
#
HIVE_PATCHPROCESS=${HIVE_PATCHPROCESS:-"${BINDIR}/../patchprocess"}

if [[ ! -d "${HIVE_PATCHPROCESS}" ]]; then
  mkdir -p "${HIVE_PATCHPROCESS}"
fi

mytmpdir=$(yetus_abs "${HIVE_PATCHPROCESS}")
if [[ $? != 0 ]]; then
  yetus_error "yetus-dl: Unable to cwd to ${HIVE_PATCHPROCESS}"
  exit 1
fi
HIVE_PATCHPROCESS=${mytmpdir}

##
## if we've already DL'd it, then short cut
##
if [[ -x "${HIVE_PATCHPROCESS}/yetus-${HIVE_YETUS_VERSION}/bin/${WANTED}" ]]; then
  exec "${HIVE_PATCHPROCESS}/yetus-${HIVE_YETUS_VERSION}/bin/${WANTED}" "${ARGV[@]}"
fi

##
## need to DL, etc
##

BASEURL="https://archive.apache.org/dist/yetus/${HIVE_YETUS_VERSION}/"
TARBALL="yetus-${HIVE_YETUS_VERSION}-bin.tar"

GPGBIN=$(command -v gpg)
CURLBIN=$(command -v curl)

pushd "${HIVE_PATCHPROCESS}" >/dev/null
if [[ $? != 0 ]]; then
  yetus_error "ERROR: yetus-dl: Cannot pushd to ${HIVE_PATCHPROCESS}"
  exit 1
fi

if [[ -n "${CURLBIN}" ]]; then
  "${CURLBIN}" -f -s -L -O "${BASEURL}/${TARBALL}.gz"
  if [[ $? != 0 ]]; then
    yetus_error "ERROR: yetus-dl: unable to download ${BASEURL}/${TARBALL}.gz"
    exit 1
  fi
else
  yetus_error "ERROR: yetus-dl requires curl."
  exit 1
fi

if [[ -n "${GPGBIN}" ]]; then
  mkdir -p .gpg
  if [[ $? != 0 ]]; then
    yetus_error "ERROR: yetus-dl: Unable to create ${HIVE_PATCHPROCESS}/.gpg"
    exit 1
  fi
  chmod -R 700 .gpg
  if [[ $? != 0 ]]; then
    yetus_error "ERROR: yetus-dl: Unable to chmod ${HIVE_PATCHPROCESS}/.gpg"
    exit 1
  fi
  "${CURLBIN}" -s -L -o KEYS_YETUS https://dist.apache.org/repos/dist/release/yetus/KEYS
  if [[ $? != 0 ]]; then
    yetus_error "ERROR: yetus-dl: unable to fetch https://dist.apache.org/repos/dist/release/yetus/KEYS"
    exit 1
  fi
  "${CURLBIN}" -s -L -O "${BASEURL}/${TARBALL}.gz.asc"
  if [[ $? != 0 ]]; then
    yetus_error "ERROR: yetus-dl: unable to fetch ${BASEURL}/${TARBALL}.gz.asc"
    exit 1
  fi
  "${GPGBIN}" --homedir "${HIVE_PATCHPROCESS}/.gpg" --import "${HIVE_PATCHPROCESS}/KEYS_YETUS" >/dev/null 2>&1
  if [[ $? != 0 ]]; then
    yetus_error "ERROR: yetus-dl: gpg unable to import ${HIVE_PATCHPROCESS}/KEYS_YETUS"
    exit 1
  fi
  "${GPGBIN}" --homedir "${HIVE_PATCHPROCESS}/.gpg" --verify "${TARBALL}.gz.asc" >/dev/null 2>&1
   if [[ $? != 0 ]]; then
     yetus_error "ERROR: yetus-dl: gpg verify of tarball in ${HIVE_PATCHPROCESS} failed"
     exit 1
   fi
fi

gunzip -c "${TARBALL}.gz" | tar xpf -
if [[ $? != 0 ]]; then
  yetus_error "ERROR: ${TARBALL}.gz is corrupt. Investigate and then remove ${HIVE_PATCHPROCESS} to try again."
  exit 1
fi

if [[ -x "${HIVE_PATCHPROCESS}/yetus-${HIVE_YETUS_VERSION}/bin/${WANTED}" ]]; then
  popd >/dev/null
  exec "${HIVE_PATCHPROCESS}/yetus-${HIVE_YETUS_VERSION}/bin/${WANTED}" "${ARGV[@]}"
fi

##
## give up
##
yetus_error "ERROR: ${WANTED} is not part of Apache Yetus ${HIVE_YETUS_VERSION}"
exit 1
