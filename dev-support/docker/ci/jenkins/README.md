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
# Hive CI Jenkins

This directory contains all the necessary resources to build the Jenkins image that is used in http://ci.hive.apache.org/
and is hosted under https://hub.docker.com/r/apache/hive-ci-jenkins/. 

The Jenkins container runs in a Kubernetes cluster powered by Google Cloud.

## Build

### Local
To build a new image with the latest Jenkins version and the respective plugins run.
```
docker build . -t apache/hive-ci-jenkins:lts-jdk25
```
The Dockerfile does not contain fixed versions for Jenkins and the plugins used which means that on every build
everything will be upgraded based on the latest available.

### CI 

There is a GitHub action that can be used to build and publish the latest Jenkins image on https://hub.docker.com/r/apache/hive-ci-jenkins/.

## Run & test
To start the container and test that everything is working run.
```
docker run -p 35000:8080 apache/hive-ci-jenkins:lts-jdk25
```
The Jenkins UI can be accessed via http://0.0.0.0:35000/.

## Upgrade guide

The bulk of stateful content that is maintained by Jenkins is located under the "/var/jenkins_home" directory and the
latter is mounted to a persistent volume (see `kubectl describe pod/JENKINS_POD`).

For the upgrade, we need to ensure that the current state and configurations in the persistent volume are not in conflict
with the new Jenkins version before pushing the new image to production.

1. Obtain a backup of the jenkins_home directory from the persistent volume.
```
kubectl exec JENKINS_POD -- tar cf - --exclude=junitResult.xml --exclude=*log* --exclude=archive --exclude=workflow --exclude=*git/objects* --exclude=.kube /var/jenkins_home > jenkins_home_backup.tar
```
Currently, the jenkins_home directory is ~280GB which makes a complete local backup and testing impractical.
The majority of disk space is occupied by the "jobs" directory and in particular by archives that are kept for each build, test results, and log files for each run.
These files are kept for archiving and diagnosability purposes when users wants to consult the results of each build.
However, they are not indispensable for the correct functionality of the Jenkins instance so for the sake of our experiments we can exclude them from the backup.
The command took ~5 minutes to run and created an archive of 2.8GB. The exclusions are referring to voluminous files that are nonessential for testing the upgrade.

2. Extract the jenkins_home directory from the archive.
```
tar -xf jenkins_home_backup.tar
```

3. Prevent the local instance from running builds.

The backup contains the production credentials (GitHub, Slack, JIRA, etc.) so any build that runs locally may post
commit statuses, comments, or notifications on behalf of the production instance. Remove the `kubernetes` cloud, which
cannot work outside the cluster and only floods the logs with failed provisioning attempts, and disable the executors
of the built-in node.
```
sed -i \
  -e '/<clouds>/,/<\/clouds>/d' \
  -e 's/<numExecutors>[0-9]*<\/numExecutors>/<numExecutors>0<\/numExecutors>/' \
  var/jenkins_home/config.xml
```

4. Disable security for testing purposes.
```
sed -i \
  -e '/<securityRealm /,/<\/securityRealm>/d' \
  -e 's/<useSecurity>true<\/useSecurity>/<useSecurity>false<\/useSecurity>/' \
  var/jenkins_home/config.xml
```

5. Launch the container using the new (upgraded) Jenkins image mounting "/var/jenkins_home" to the downloaded backup.

```
docker run  --rm -p 35000:8080 -v $(pwd)/var/jenkins_home:/var/jenkins_home apache/hive-ci-jenkins:lts-jdk25
```

6. Check the container logs and ensure that Jenkins has started and runs smoothly without alarming messages or fatal failures.
Pay attention to plugins that failed to load (`Failed Loading plugin`) and check `var/jenkins_home/copy_reference_file.log`
for plugins that were not upgraded to the version shipped with the image (`SKIPPED`).

Expected warnings in this setup: `kubernetes cloud not found` / `failed to do initial reap check` (the cloud was removed),
and errors resuming in-progress pipeline builds (their logs are excluded from the backup).
