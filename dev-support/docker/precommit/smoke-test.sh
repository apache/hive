#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Checks that a precommit image provides what the Jenkinsfile and the tests rely on.
#
# Usage: dev-support/docker/precommit/smoke-test.sh <image> [<expected java feature version>]

set -euo pipefail

image=${1:?usage: $0 <image> [<java version>]}
javaVersion=${2:-21}

docker run --rm -i -e JAVA_FEATURE="$javaVersion" "$image" bash -s <<'EOF'
set -euo pipefail
check() {
  local what=$1
  shift
  if out=$("$@" 2>&1); then
    echo "ok   $what"
  else
    echo "FAIL $what: $out" >&2
    exit 1
  fi
}

check "runs as jenkins (uid 1000)" test "$(id -u):$(id -un)" = 1000:jenkins
check "USER is set" test "${USER:-}" = jenkins
check "home is writable" touch "$HOME/.probe"
check "~/.m2 is writable" touch "$HOME/.m2/.probe"
check "java $JAVA_FEATURE" sh -c 'java -version 2>&1 | grep -q "version \"$JAVA_FEATURE"'
check "javac" javac -version
check "default charset is UTF-8" sh -c 'java -XshowSettings:properties -version 2>&1 | grep -q "native.encoding = UTF-8"'
check "maven" mvn -B -v
for tool in bash docker git python python3 rsync tini xmlstarlet ps; do
  check "$tool on PATH" command -v $tool
done
for tool in diff find grep sed sort tar; do
  check "GNU $tool" sh -c "$tool --version | grep -q GNU"
done
check "openssl at /usr/bin/openssl" sh -c 'cd "$(mktemp -d)" && /usr/bin/openssl req -x509 -newkey rsa:2048 -keyout k.pem -out c.pem -days 1 -nodes -subj /CN=test'
check "glibc" getconf GNU_LIBC_VERSION
for lib in libcom_err.so.2 libk5crypto.so.3 libkeyutils.so.1 libkrb5support.so.0; do
  check "$lib for the Kudu mini cluster" sh -c "ldconfig -p | grep -q $lib"
done
check "hadoop launcher" /opt/hadoop/bin/hadoop version
check "python TRANSFORM-style script" sh -c 'test "$(printf "a\tb\n" | python -c "import sys; [print(l.rstrip().upper()) for l in sys.stdin]")" = "$(printf "A\tB")"'

# The Prechecks stage relies on "xmlstarlet edit" reformatting badly indented poms.
cd "$(mktemp -d)"
printf '<?xml version="1.0" encoding="UTF-8"?>\n<project>\n  <a>1</a>\n</project>\n' > good.xml
printf '<?xml version="1.0" encoding="UTF-8"?>\n<project>\n <a>1</a>\n</project>\n' > bad.xml
cp good.xml good.orig
cp bad.xml bad.orig
xmlstarlet edit -L good.xml bad.xml
check "xmlstarlet keeps formatted xml" cmp good.xml good.orig
check "xmlstarlet reformats bad xml" sh -c '! cmp -s bad.xml bad.orig'

check "git" sh -c 'git init -q repo && git -C repo status --short'
echo "All checks passed"
EOF
