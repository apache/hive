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
# Apache Hive docs site

This directory contains the code for the Apache Hive web site,
[hive.apache.org](https://hive.apache.org/). The easiest way to build
the site is to use docker to use a standard environment.

## Run the docker container with the preview of the site.

1. `docker build -t hive-site .`
2. `CONTAINER=$(docker run -d -p 4000:4000 hive-site)`

## Browsing

Look at the site by navigating to
[http://0.0.0.0:4000/](http://0.0.0.0:4000/) .

## Pushing to site

Commit and push the changes to the main branch. The site is automatically deployed
from the site directory.

## Shutting down the docker container

1. `docker stop $CONTAINER`