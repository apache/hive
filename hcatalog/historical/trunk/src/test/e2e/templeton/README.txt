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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

End to end tests
---------------
End to end tests in templeton runs tests against an existing templeton server.
It runs hcat, mapreduce, streaming, hive and pig tests.

!!!! NOTE !!!!
--------------
USE SVN TO CHECKOUT CODE FOR RUNNING TESTS AS THE TEST
  HARNESS IS EXTERNED FROM PIG. GIT WILL NOT IMPORT IT

Test cases
----------
The tests are defined in src/test/e2e/templeton/tests/*.conf

Test framework
--------------
The test framework is derived from the one used in pig, there is more documentation here on the framework -
https://cwiki.apache.org/confluence/display/PIG/HowToTest


Setup
-----
1. Templeton needs to be installed and setup to be able to run hcat, maprduce, hive and pig commands. 

2. Install perl and following perl modules  (cpan -i <MODULE_NAME>)
* IPC::Run
* JSON
* Data::Dump
* Number::Compare
* Text::Glob
* Data::Compare
* File::Find::Rule
* HTTP::Daemon
* Parallel::ForkManager

Tips:
* Using perlbrew (http://perlbrew.pl) should make installing perl modules easier. 
* Use 'yes | cpan -i <MODULE_NAME>' to avoid answering the 100's of questions cpan asks.



3. Copy contents of src/test/e2e/templeton/inpdir to hdfs

4. You will need to two jars in the same HDFS directory as the contents of inpdir.  piggybank.jar, which can
be obtained from Pig.  The second is the hadoop-examples.jar, which can be obtained from your Hadoop distribution.
This should be called hexamples.jar when it is uploaded to HDFS.


Running the tests
-----------------
Use the following command to run tests -

ant test -Dinpdir.hdfs=<location of inpdir on hdfs>  -Dtest.user.name=<user the tests should run as> \
 -Dsecure.mode=<yes/no>   -Dharness.webhdfs.url=<webhdfs url upto port num>  -Dharness.templeton.url=<templeton url upto port num> 

If you want to run specific test group you can specify the group, for example:  -Dtests.to.run='-t TestHive'

If you want to run specific test in a group group you can specify the test, for example:  -Dtests.to.run='-t TestHive_1'


Running the hcat authorization tests
------------------------------------
Hcat authorization tests run commands as different users to test if authorization is done right.

ant test-hcat-authorization -Dkeytab.dir=<keytab files dir> 
  -Dsecure.mode=<yes/no>  -Dtest.group.name=<common group> -Dinpdir.hdfs=<location of inpdir on hdfs>  
  -Dtest.user.name=<user 1 belonging to common group> -Dtest.group.user.name=<user 2 belonging to common group>  
  -Dtest.other.user.name=<user 3 who does not belong to common group> 
  -Dharness.webhdfs.url=<webhdfs url upto port num>  -Dharness.templeton.url=<templeton url upto port num> 

The <keytab files dir> is expected to have keytab filenames of the form - user_name.*keytab .


Notes
-----



Enable webhdfs by adding the following to your hadoop hdfs-site.xml :

<property>
  <name>dfs.webhdfs.enabled</name>
  <value>true</value>
</property>
<property>
  <name>dfs.http.address</name>
  <value>127.0.0.1:8085</value>
  <final>true</final>
</property>

You can build a server that will measure test coverage by using templeton:
ant clean; ant e2e
This assumes you've got webhdfs at the address above, the inpdir info in /user/templeton, and templeton running on the default port.  You can change any of those properties in the build file.

It's best to set HADOOP_HOME_WARN_SUPPRESS=true everywhere you can.
