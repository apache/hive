#!/usr/local/bin/perl -w

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

use strict;

my $TESTDIR = '/tmp/templetontest/';
my $TEST_INP_DIR = '/tmp/test_inpdir/'; #dir on hadoop
my $TEST_USER = 'hortonth';
my $WEBHDFS_URL = 'http://localhost:50070';
my $TEMPLETON_URL = 'http://localhost:8080';
my $CATALINA_HOME = $ENV{'CATALINA_HOME'};

#use env variables if they have been set
if(defined  $ENV{'TESTDIR'}){
    $TESTDIR = $ENV{'TESTDIR'};
}
if(defined  $ENV{'TEST_INP_DIR'}){
    $TEST_INP_DIR = $ENV{'TEST_INP_DIR'};
}
if(defined  $ENV{'TEST_USER'}){
    $TEST_USER = $ENV{'TEST_USER'};
}
if(defined  $ENV{'WEBHDFS_URL'}){
    $WEBHDFS_URL = $ENV{'WEBHDFS_URL'};
}
if(defined  $ENV{'TEMPLETON_URL'}){
    $TEMPLETON_URL = $ENV{'TEMPLETON_URL'};
}

if(! defined $ENV{'HCAT_PREFIX'}){
    $ENV{'HCAT_PREFIX'}='/usr/';
}

if(! defined $ENV{'HADOOP_PREFIX'}){
    $ENV{'HADOOP_PREFIX'}='/usr/';
}

my $host = `hostname` ;
chomp $host;

if(! defined $ENV{'ZOOKEEPER_HOST'}){
    $ENV{'ZOOKEEPER_HOST'} =  $host . ':2181';
}

if(! defined $ENV{'METASTORE_HOST'}){
    $ENV{'METASTORE_HOST'} =  $host . ':9933';
}

print STDERR "##################################################################\n";
print STDERR "Using the following settings for environment variables\n" .
    " (Set them to override the default values) \n" .
    "WEBHDFS_URL : $WEBHDFS_URL \n" .
    "TEMPLETON_URL : $TEMPLETON_URL \n" .
    'CATALINA_HOME :' . $ENV{'CATALINA_HOME'} . "\n" .
    'HADOOP_PREFIX :' . $ENV{'HADOOP_PREFIX'} . "\n" .
    'HCAT_PREFIX :' . $ENV{'HCAT_PREFIX'} . "\n" .
    'ZOOKEEPER_HOST :' . $ENV{'ZOOKEEPER_HOST'} . "\n" .
    'METASTORE_HOST :' . $ENV{'METASTORE_HOST'} . "\n" 
;
print STDERR "##################################################################\n";

system("rm -rf $TESTDIR/");

#restart tomcat with updated env variables
my $templeton_src = "$TESTDIR/templeton_src";
$ENV{'TEMPLETON_HOME'} = "$templeton_src/templeton";

system ("$CATALINA_HOME/bin/shutdown.sh") == 0 or die "tomcat shutdown failed" ;
sleep 3;



#get templeton git repo, build and install
system("mkdir -p $templeton_src") == 0 or die "could not create dir $templeton_src: $!";
chdir  "$templeton_src"  or die "could not change directory to  $templeton_src  : $!";
system ('git clone  git@github.com:hortonworks/templeton.git')  == 0 or die "could not clone templeton git repo";
chdir 'templeton' or die 'could not change dir : $!';


#put a templeton-site.xml in $TEMPLETON_HOME with zookeeper hostname
writeTempletonSiteXml();
system ('ant install-war') == 0 or die "templeton build failed";

#tomcat should have shutdown by now, try starting it
system ("$CATALINA_HOME/bin/startup.sh") == 0 or die 'tomcat startup failed';
sleep 3;

my $tdir = "$templeton_src/templeton/src/test/e2e/templeton";
chdir $tdir or die "could not change dir $tdir : $!";

#copy input files
system("hadoop fs -rmr $TEST_INP_DIR");
system("hadoop fs -copyFromLocal $tdir/inpdir $TEST_INP_DIR") == 0 or die "failed to copy input dir : $!";
system("hadoop fs -chmod -R 777 $TEST_INP_DIR")  == 0 or die "failed to set input dir permissions : $!";

#start tests
my $cmd = "ant test -Dinpdir.hdfs=$TEST_INP_DIR  -Dtest.user.name=$TEST_USER" .
    " -Dharness.webhdfs.url=$WEBHDFS_URL -Dharness.templeton.url=$TEMPLETON_URL ";

system($cmd) == 0 or die "templeton tests failed";

#############################
sub writeTempletonSiteXml {
    my $conf = $ENV{'TEMPLETON_HOME'} . "/templeton-site.xml";
    open ( CFH,  ">$conf" ) or die $!;


    print CFH '<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>templeton.zookeeper.hosts</name>
    <value>'  . 
 $ENV{'ZOOKEEPER_HOST'} .
'</value>
    <description>ZooKeeper servers, as comma separated host:port pairs</description>
  </property>

  <property>
    <name>templeton.hive.properties</name>
    <value>hive.metastore.local=false,hive.metastore.uris=thrift://' .
    $ENV{'METASTORE_HOST'} .
    ',hive.metastore.sasl.enabled=false,hive.metastore.execute.setugi=true</value>
    <description>Properties to set when running hive.</description>
  </property>

  <property>
    <name>templeton.hive.archive</name>
    <value>hdfs:///user/templeton/hcatalog-0.3.0.tar.gz</value>
    <description>The path to the Hive archive.</description>
  </property>

  <property>
    <name>templeton.hive.path</name>
    <value>hcatalog-0.3.0.tar.gz/hcatalog-0.3.0/bin/hive</value>
    <description>The path to the Hive executable.</description>
  </property>

  <property>
    <name>templeton.pig.archive</name>
    <value>hdfs:///user/templeton/pig-0.9.2.tar.gz</value>
    <description>The path to the Pig archive.</description>
  </property>

  <property>
    <name>templeton.pig.path</name>
    <value>pig-0.9.2.tar.gz/pig-0.9.2/bin/pig</value>
    <description>The path to the Pig executable.</description>
  </property>

</configuration>
';
    close CFH or die $!;

;





}
