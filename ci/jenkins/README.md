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

## Run locally
To start the container and test that everything is working run.
```
docker run -p 35000:8080 apache/hive-ci-jenkins:lts-jdk21
```
The Jenkins UI can be accessed via http://0.0.0.0:35000/.
