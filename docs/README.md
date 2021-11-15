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