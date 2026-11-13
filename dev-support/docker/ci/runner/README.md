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

The [Jenkinsfile](../../../../Jenkinsfile) runs every precommit stage in this image, with a
`docker:dind` sidecar for the tests that start databases in containers.

The image is kept small on purpose. It holds:

* Debian (slim), with the GNU shell tools that scripts and tests expect. It is glibc based on
  purpose: tests run prebuilt native code that is linked against glibc, such as the Kudu
  mini cluster.
* a JDK (Eclipse Temurin), without jmods and man pages
* Maven
* the docker CLI, git, openssl, python3, rsync, tini and xmlstarlet

Everything else (Tez, the JDBC drivers, the Hive build dependencies) is fetched by Maven
during the build. Before adding a package, check whether Maven can provide it instead.

## How the image is published

The [precommit-image](../../../../.github/workflows/precommit-image.yml) workflow builds the
image on every pull request that changes the Dockerfile. When such a change lands on master,
the workflow publishes the image for `linux/amd64` and `linux/arm64` as
`apache/hive-ci-runner:<version>`, where `<version>` is the `org.opencontainers.image.version`
label of the Dockerfile.

A published tag is never overwritten: the workflow fails if the Dockerfile changed but its
version is a tag that already exists. A change that does not touch the Dockerfile (e.g. of the
workflow) needs no new version: the workflow builds the image but does not publish it. The
Jenkinsfile pins the tag in `precommitImage`, so a new image is only used once a commit bumps
that pin.

## Changing the image

1. Edit the [Dockerfile](Dockerfile), and bump its `org.opencontainers.image.version`. Keep the
   image small:
   * Prefer letting Maven fetch a dependency over baking it into the image.
   * Pin versions through the existing build arguments (`JAVA_VERSION`, `MAVEN_VERSION`,
     `DEBIAN_VERSION`) rather than hard-coding them.
   * Install packages with `--no-install-recommends`, and add each new package to the comment
     above `apt-get install`, saying which stage or test needs it.
   * Do anything heavy in the `jdk` or `downloads` stage and copy only the result into the
     final stage.
2. Build the image, and compare its size with the image before your change. Explain any growth
   in the pull request.

   ```bash
   docker build -t hive-ci-runner:local dev-support/docker/ci/runner
   docker images hive-ci-runner:local
   ```
3. Try the image in the precommit job. Push it, for amd64, to a Docker Hub namespace you
   control:

   ```bash
   docker buildx build --platform linux/amd64 --push \
     -t <your-namespace>/hive-ci-runner:HIVE-12345 dev-support/docker/ci/runner
   ```

   On the precommit job of your pull request in Jenkins, choose *Build with Parameters* and set
   `IMAGE` to `<your-namespace>/hive-ci-runner:HIVE-12345`. The parameter is only offered after
   the job has run at least once with this Jenkinsfile. Link the run in the pull request.
4. Once the pull request is merged, the workflow publishes `apache/hive-ci-runner:<version>`.
   Open a follow-up pull request that bumps `precommitImage` in the Jenkinsfile to the new tag.
   Its own precommit run is the first one in the published image.

To roll back, revert the commit that bumped `precommitImage`. The previous tag is still
published, unchanged.
