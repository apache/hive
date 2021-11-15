# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Hive site builder
#

FROM ubuntu:18.04
MAINTAINER Hive team <dev@hive.apache.org>

RUN ln -fs /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
RUN apt-get update
RUN apt-get install -y \
  g++ \
  gcc \
  git \
  libssl-dev \
  libz-dev \
  make \
  ruby-dev \
  rubygems \
  tzdata
RUN gem install \
  bundler \
  liquid \
  listen \
  rouge
RUN gem install jekyll -v 3.8.6
RUN gem install github-pages

RUN useradd -ms /bin/bash hive
COPY . /home/hive/site
RUN chown -R hive:hive /home/hive
USER hive
WORKDIR /home/hive/site

EXPOSE 4000
CMD bundle exec jekyll serve -H 0.0.0.0

