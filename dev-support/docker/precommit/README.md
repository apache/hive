<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
# Hive precommit image

The [Jenkinsfile](../../../Jenkinsfile) runs every precommit stage in this image, with a
`docker:dind` sidecar for the tests that start databases in containers.

The image is kept small on purpose. It holds:

* Debian (slim), with the GNU shell tools that scripts and tests expect. It is glibc based on
  purpose: tests run prebuilt native code that is linked against glibc, such as the Kudu
  mini cluster.
* a JDK (Eclipse Temurin), without jmods and man pages
* Maven
* the client part of Hadoop, used only by `bin/schematool` in the `init@<db>` stages
  (see [precommit-init-metastore.sh](../../precommit-init-metastore.sh))
* the docker CLI, git, openssl, python3, rsync, tini and xmlstarlet

Everything else (Tez, the JDBC drivers, the Hive build dependencies) is fetched by Maven
during the build. Before adding a package, check whether Maven can provide it instead.

## How the image is published

The [precommit-image](../../../.github/workflows/precommit-image.yml) workflow builds the
image and smoke tests it on every pull request that changes this directory. When such a
change lands on master, the workflow publishes the image for `linux/amd64` and `linux/arm64`
as `apache/hive-ci-runner` (the default `latest` tag). That is the `precommitImage` of the
Jenkinsfile, and the precommit job pulls it again on every run. The tag is overwritten on
every publish: once the change is merged, every precommit run, on every branch, uses the new
image.

## Changing the image, step by step

All commands run from the root of the Hive source tree.

### 1. Make the change

Edit the [Dockerfile](Dockerfile). Keep the image small:

* Prefer letting Maven fetch a dependency over baking it into the image.
* Pin versions through the existing build arguments (`JAVA_VERSION`, `MAVEN_VERSION`,
  `HADOOP_VERSION`, `DEBIAN_VERSION`) rather than hard-coding them.
* Install packages with `--no-install-recommends`, and add each new package to the comment
  above `apt-get install`, saying which stage or test needs it.
* Do anything heavy in the `jdk` or `downloads` stage and copy only the result into the
  final stage.

If the Jenkinsfile or a test relies on the new content, add a check for it to
[smoke-test.sh](smoke-test.sh).

### 2. Build the image

```bash
docker build -t hive-precommit:local dev-support/docker/precommit
docker images hive-precommit:local
```

Compare the size with the image before your change, and explain any growth in the pull
request. On an arm64 machine (e.g. Apple silicon) this builds an arm64 image. The CI runs
amd64: add `--platform linux/amd64` to try that one, which is emulated and slower.

On Linux, files written into a mounted source tree belong to uid 1000, the `jenkins` user of
the image. If your uid is different, build with `--build-arg UID=$(id -u)`.

### 3. Run the smoke test

```bash
dev-support/docker/precommit/smoke-test.sh hive-precommit:local
```

Every line must say `ok`. The pull request workflow runs the same script.

### 4. Start a docker daemon for the tests

The Jenkins pod runs a `docker:dind` sidecar in the same network namespace. Do the same
locally:

```bash
docker run -d --name hive-dind --privileged -e DOCKER_TLS_CERTDIR= \
  docker:20.10-dind --host=tcp://0.0.0.0:2375
```

The commands below run the image with
`--network container:hive-dind -e DOCKER_HOST=tcp://localhost:2375`. That way the containers
the tests start are reachable on `localhost`, as in the Jenkins pod.

### 5. Build Hive in the image

This is what the `Compile` stage does. Keep the local Maven repository between runs:

```bash
mkdir -p ~/.m2/hive-precommit-repository
docker run --rm -it \
  -v "$PWD:/home/jenkins/hive" -w /home/jenkins/hive \
  -v ~/.m2/hive-precommit-repository:/home/jenkins/.m2/repository \
  hive-precommit:local \
  mvn -B install -DskipTests -Pitests,dist
```

The `dist` profile is needed for the next step.

### 6. Initialize the metastore

This is what the `init@derby` and `init@postgres` stages do:

```bash
docker run --rm \
  -v "$PWD:/home/jenkins/hive" -w /home/jenkins/hive \
  hive-precommit:local \
  dev-support/precommit-init-metastore.sh derby

docker run --rm \
  --network container:hive-dind -e DOCKER_HOST=tcp://localhost:2375 \
  -v "$PWD:/home/jenkins/hive" -w /home/jenkins/hive \
  -v ~/.m2/hive-precommit-repository:/home/jenkins/.m2/repository \
  hive-precommit:local \
  dev-support/precommit-init-metastore.sh postgres
```

Both must end with `Metastore schema initialized on <db>`. The logs, including the log of
the postgres container, are written to `target/init-metastore/<db>`.

### 7. Run some tests

Run tests that exercise what you changed. A good baseline is a couple of qtests:
`newline.q` runs python `TRANSFORM` scripts, and `jdbc_project_pushdown.q` starts postgres
in the docker daemon. They belong to different drivers, so they take two runs:

```bash
for t in TestMiniLlapCliDriver:newline.q TestMiniLlapLocalCliDriver:jdbc_project_pushdown.q; do
  docker run --rm \
    --network container:hive-dind -e DOCKER_HOST=tcp://localhost:2375 \
    -v "$PWD:/home/jenkins/hive" -w /home/jenkins/hive \
    -v ~/.m2/hive-precommit-repository:/home/jenkins/.m2/repository \
    hive-precommit:local \
    mvn -B test -Pitests -pl itests/qtest -Dtest=${t%%:*} -Dqfile=${t#*:} || break
done
```

When you are done, remove the docker daemon with `docker rm -f -v hive-dind`.

### 8. Open a pull request

Open the pull request as usual. The precommit-image workflow builds the image and runs the
smoke test on it. The precommit job of the pull request itself still runs in the published
`apache/hive-ci-runner`, not in your change.

### 9. Run the precommit job with the candidate image

For anything beyond a trivial change, run the full precommit job on the new image before
merging. This needs a committer.

1. Publish the candidate under a tag of its own. Either:
   * push the change to a branch of `apache/hive` and run the precommit-image workflow on that
     branch from the Actions tab, with `tag` set to e.g. `HIVE-12345`; or
   * build it for amd64 and push it to a registry you control:

     ```bash
     docker buildx build --platform linux/amd64 --push \
       -t <your-namespace>/hive-precommit:HIVE-12345 dev-support/docker/precommit
     ```

2. On the precommit job of the pull request in Jenkins, choose *Build with Parameters* and set
   `IMAGE` to the candidate, e.g. `apache/hive-ci-runner:HIVE-12345`. The parameter
   is only offered after the job has run at least once with this Jenkinsfile.
3. Link the green run in the pull request.

### 10. Merge and verify

Once merged, the precommit-image workflow on master publishes
`apache/hive-ci-runner`. Then:

1. Check that the workflow run succeeded.
2. Check the published image:

   ```bash
   docker buildx imagetools inspect apache/hive-ci-runner
   ```

3. Watch the next precommit runs.

To roll back, revert the commit. The workflow then publishes the previous Dockerfile as
`latest` again. The rebuilt image picks up the Debian packages current at that time,
so it is equivalent to the old one but not identical.
