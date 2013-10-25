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

It's a good idea to look at current versions of
http://hive.apache.org/docs/hcat_r0.5.0/rest_server_install.html and
http://hive.apache.org/docs/hcat_r0.5.0/configuration.html before proceeding.


(Note that by default, webhcat-default.xml templeton.hive.properties sets
hive.metastore.uris=thrift://localhost:9933, thus WebHCat will expect
an external metastore to be running.
to start hive metastore: ./bin/hive --service metastore -p 9933)

launch templeton server: ./hcatalog/sbin/webhcat_server.sh start

to control which DB the metastore uses put something like
<property>
  <name>javax.jdo.option.ConnectionURL</name>
  <value>jdbc:derby:;databaseName=/Users/ekoifman/dev/data/tmp/metastore_db_e2e;create=true</value>
  <description>Controls which DB engine metastore will use for persistence. In particular,
  where Derby will create it's data files.</description>
</property>

in hive-site.xml
)


!!!! NOTE !!!!
--------------
USE SVN TO CHECKOUT CODE FOR RUNNING TESTS AS THE TEST
  HARNESS IS EXTERNED FROM PIG. GIT WILL NOT IMPORT IT
  (if you are using GIT, check out http://svn.apache.org/repos/asf/hive/trunk (or whichever branch)
  (http://hive.apache.org/version_control.html) and symlink
  hcatalog/src/test/e2e/harness/ to corresponding harness/ in SVN tree)

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
* JSON::Path
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
(e.g. ./bin/hadoop fs -put ~/dev/hive/hcatalog/src/test/e2e/templeton/inpdir/ webhcate2e)

4. You will need to copy three jars in the same HDFS directory as the contents of inpdir.  piggybank.jar, which can
be obtained from Pig and the other two are obtained from your Hadoop distribution.
For Hadoop 1.x you would need to upload hadoop-examples.jar twice to HDFS one as hclient.jar and other as hexamples.jar.
For Hadoop 2.x you would need to upload hadoop-mapreduce-client-jobclient.jar to HDFS as hclient.jar and hadoop-mapreduce-examples.jar to HDFS as hexamples.jar. 
Also see http://hive.apache.org/docs/hcat_r0.5.0/rest_server_install.html#Hadoop+Distributed+Cache for notes on
additional JAR files to copy to HDFS.

5. Make sure TEMPLETON_HOME evnironment variable is set

6. hadoop/conf/core-site.xml should have items described in
http://hive.apache.org/docs/hcat_r0.5.0/rest_server_install.html#Permissions

Running the tests
-----------------
Use the following command to run tests -

ant test -Dinpdir.hdfs=<location of inpdir on hdfs>  -Dtest.user.name=<user the tests should run as> \
 -Dtest.other.user.name=<another user who can launch job> \
 -Dsecure.mode=<yes/no>   -Dharness.webhdfs.url=<webhdfs url upto port num>  -Dharness.templeton.url=<templeton url upto port num> 

If you want to run specific test group you can specify the group, for example:  -Dtests.to.run='-t TestHive'

If you want to run specific test in a group group you can specify the test, for example:  -Dtests.to.run='-t TestHive_1'
For example, tests/ddl.conf has several groups such as 'name' => 'REST_DDL_TABLE_BASIC'; use REST_DDL_TABLE_BASIC as the name

If you are running with Hadoop 2, please use the flag: -Dhadoopversion=23


Running the hcat authorization tests
------------------------------------
Hcat authorization tests run commands as different users to test if authorization is done right.

ant test-hcat-authorization -Dkeytab.dir=<keytab files dir> 
  -Dsecure.mode=<yes/no>  -Dtest.group.name=<common group> -Dinpdir.hdfs=<location of inpdir on hdfs>  
  -Dtest.user.name=<user 1 belonging to common group> -Dtest.group.user.name=<user 2 belonging to common group>  
  -Dtest.other.user.name=<user 3 who does not belong to common group> 
  -Dharness.webhdfs.url=<webhdfs url upto port num>  -Dharness.templeton.url=<templeton url upto port num> 

The <keytab files dir> is expected to have keytab filenames of the form - user_name.*keytab .

Running WebHCat doas tests
--------------------------
ant clean test-doas -Dinpdir.hdfs=/user/ekoifman/webhcate2e -Dsecure.mode=no   
    -Dharness.webhdfs.url=http://localhost:8085  -Dharness.templeton.url=http://localhost:50111 
    -Dtests.to.run='-t doAsTests' -Dtest.user.name=hue -Ddoas.user=joe
    
The canonical example, is WebHCat server is running as user 'hcat', end user 'joe' is using Hue,
which generates a request to WebHCat.  If Hue specifies doAs=joe, then the commands that WebHCat
submits to Hadoop will be run as user 'joe'.

In order for this test suite to work, webhcat-site.xml should have webhcat.proxyuser.hue.groups
and webhcat.proxyuser.hue.hosts defined, i.e. 'hue' should be allowed to impersonate 'joe'.
[Of course, 'hcat' proxyuser should be configured in core-site.xml for the command to succeed.]

Furthermore, metastore side file based security should be enabled. 
(See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Authorization#LanguageManualAuthorization-MetastoreServerSecurity for more info) 

To do this 3 properties in hive-site.xml should be configured:
1) hive.security.metastore.authorization.manager set to 
    org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider
2) hive.security.metastore.authenticator.manager set to 
    org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator
3) hive.metastore.pre.event.listeners set to
    org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener
4) hive.metastore.execute.setugi set to true

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
Also useful to add to conf/hadoop-env.sh
export HADOOP_OPTS="-Djava.security.krb5.realm=OX.AC.UK -Djava.security.krb5.kdc=kdc0.ox.ac.uk:kdc1.ox.ac.uk"
to prevent warning about SCDynamicStore which may throw some tests off
(http://stackoverflow.com/questions/7134723/hadoop-on-osx-unable-to-load-realm-info-from-scdynamicstore)


Performance
-----------
It's a good idea to set fork.factor.conf.file={number of .conf files} and fork.factor.group to something > 1
(see build.xml) to make these tests run faster.  If doing this, make sure the Hadoop Cluster has 
enough map slots (10?) (mapred.tasktracker.map.tasks.maximum), otherwise test parallelism won't help.

Adding Tests
------------
ToDo: add some guidelines
