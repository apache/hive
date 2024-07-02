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
docker build . -t apache/hive-ci-jenkins:lts-jdk21
```
The Dockerfile does not contain fixed versions for Jenkins and the plugins used which means that on every build
everything will be upgraded based on the latest available.

### CI 

There is a GitHub action that can be used to build and publish the latest Jenkins image on https://hub.docker.com/r/apache/hive-ci-jenkins/.

## Run & test
To start the container and test that everything is working run.
```
docker run -p 35000:8080 apache/hive-ci-jenkins:lts-jdk21
```
The Jenkins UI can be accessed via http://0.0.0.0:35000/.

## Upgrade guide

The bulk of stateful content that is maintained by Jenkins is located under the "/var/jenkins_home" directory and the
latter is mounted to a persistent volume (see `kubectl describe pod/JENKINS_POD`).

For the upgrade, we need to ensure that the current state and configurations in the persistent volume are not in conflict
with the new Jenkins version before pushing the new image to production.

1. Obtain a backup of the jenkins_home directory from the persistent volume.
```
kubectl exec JENKINS_POD -- tar cf - --exclude=junitResult.xml --exclude=*log* --exclude=archive --exclude=workflow --exclude=*git/objects* /var/jenkins_home > jenkins_home_backup.tar
```
Currently, the jenkins_home directory is ~280GB which makes a complete local backup and testing impractical.
The majority of disk space is occupied by the "jobs" directory and in particular by archives that are kept for each build, test results, and log files for each run.
These files are kept for archiving and diagnosability purposes when users wants to consult the results of each build.
However, they are not indispensable for the correct functionality of the Jenkins instance so for the sake of our experiments we can exclude them from the backup.
The command took ~5 minutes to run and created an archive of 1.2GB. The exclusions are referring to voluminous files that are nonessential for testing the upgrade.

2. Extract the jenkins_home directory from the archive.
```
tar -xf jenkins_home_backup.tar
```

3. (Optional) Modify the main Jenkins configuration file to change the GitHub application that is used for authentication.
```
mv var/jenkins_home/config.xml var/jenkins_home/config.xml.bstar
xmlstarlet edit --update '//securityRealm' --value "
    <githubWebUri>https://github.com</githubWebUri>
    <githubApiUri>https://api.github.com</githubApiUri>
    <clientID>YOUR_CLIENT_ID</clientID>
    <clientSecret>YOUR_CLIENT_SECRET</clientSecret>
    <oauthScopes>read:org,user:email</oauthScopes>
" var/jenkins_home/config.xml.bstar | xmlstarlet unesc > var/jenkins_home/config.xml
```
This steps requires creating a custom authorization APP (https://github.com/settings/apps/new) otherwise the login menu
will direct you to `http://ci.hive.apache.org/` and not to your local Jenkins instance (`http://0.0.0.0:35000`).
If you skip this step you will have to find another way to login to your local Jenkins instance, although it is not strictly required.
If there are no alarming logs when you start the container then doing a user login may not be necessary.

4. Launch the container using the new (upgraded) Jenkins image mounting "/var/jenkins_home" to the downloaded backup.

```
docker run  --rm -p 35000:8080 -v $(pwd)/var/jenkins_home:/var/jenkins_home apache/hive-ci-jenkins:lts-jdk21
```

5. Check the container logs and ensure that Jenkins has started and runs smoothly without alarming messages or fatal failures.
