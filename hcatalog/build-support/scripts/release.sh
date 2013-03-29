#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# HCatalog uses maven for resolving dependencies and publishing artifacts.
# Maven requires project version numbers be hard-coded in pom.xml files,
# rather than being typical variables. Tooling in provided by maven to ease
# managing these version numbers, however, as our build is ant-based
# we cannot use that tooling. This release script is a workaround to
# update pom.xml version numbers to properly build release artifacts.

if [ "${HCAT_RELEASE_VERSION}" == "" ]; then
  echo "Required environment variable HCAT_RELEASE_VERSION not set."
  exit -1
fi

snapshot_version=$(awk -F= '/hcatalog.version=/ { print $2 }' build.properties)

find . -name pom.xml -exec sed -i '' "s/${snapshot_version}/${HCAT_RELEASE_VERSION}/" {} \;
sed -i '' "s/${snapshot_version}/${HCAT_RELEASE_VERSION}/" build.properties

# useful to pass in "-Dtestcase=Foo" to bypass tests when troubleshooting builds
export ANT_ARGS="${ANT_ARGS}"

./build-support/scripts/test.sh
