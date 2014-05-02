#
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
#

Overview
This directory contains a set of scripts that make running WebHCat e2e tests easier.  These scripts
help ensure that all the necessary artifacts for e2e tests are deployed to the cluster and speed up
code-compile-test loop in Hive/WebHCat.


Assumptions
It is assumed that you have a properly set up Hadoop2 cluster running.

High level workflow
1. Build Hive (e.g. mvn clean package -Phadoop-2,dist -DskipTests)
2. Define variables in env.sh.  This should be the only file you must change.
3. Run restart_hive_redeploy_artifacts.sh, which will 
    a. Stop Hive Metastore, WebHCat server
    b. Delete dirs in HDFS which may be there from previous runs.  Currently this is used with a
      cluster that is only used for WebHCat e2e tests so make sure to see what this will delete if
      the cluster is used for something else.
    c. Copy hive-site.xml and webhcat-site.xml under HIVE_HOME with minimal config needed to start
      the services and run e2e tests.
    d. Start Hive Metastore and WebHCat servers
    e. Copy various artifacts to HDFS as explained in e2e/templeton/README.txt.
4. Now you can run the test command as explained in e2e/templeton/README.txt.


If you would like to make this better (in no particular order):
1. env.sh is sourced from all other scripts but only work if the 'other' script is called from
   deployers/.
2. send 'derby.log' somewhere in /tmp/
3. some tests (e.g. Sqoop) require an RDMBS set up with resources pre-created.  See if this can
   be automated.
  (At least truncating the table between runs).
4. Make the same work on Windows (w/o making a copy of each .sh file if at all possible)
5. Configure a working (even pseudo-dist) Hadoop-2 cluster takes some knowledge.  It may be to
   script taking of Hadoop binary tar file, exploding it and copying a few pre-canned config files
   to it (mapred-site, yarn-site, etc) to make sure the can easily set up a test env.
6. Make this set of scripts work with Hadoop-1 (should not take much effort, if any).
