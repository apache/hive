#!/usr/bin/env bash

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

# This script assumes that it is being run from the top level directory of the
# HCatalog distribution tarball

host="unknown"
dir="unknown"
hadoop_home="unknown"
tarball="unknown"
dbroot="unknown"
portnum="9933"
passwd="hive"
warehouseDir="/user/hive/warehouse"
sasl="false"
keytabpath="unknown"
kerberosprincipal="unknown"
forrest="unknown"

function usage() {
    echo "Usage: $0 -D dbroot -d directory -f forrest -h hadoop_home "
    echo "  -m host -t tarball"
    echo "  [-p portnum] [-P password] [-w warehouse_directory]"
    echo "  [-s true|false -k keytabpath -K kerberos_principal]"
    echo 
    echo "    dbroot is the root directory for the mysql drivers"
    echo "    directory is the directory where it will be installed"
    echo "    hadoop_home is the directory of your Hadoop installation."
    echo "    host is the machine to install the HCatalog server on"
    echo "    tarball is the result of running ant src-release in hcat"
    echo "    portnum is the port for the thrift server to use, " \
        "default $portnum"
    echo "    password is the password for the metastore db, default $passwd"
    echo "    warehouse_directory is the HDFS directory to use for " \
        "internal hive tables, default $warehouseDir"
    echo "    -s true will enable security, -s false turn it off, " \
        "default $sasl"
    echo "    keytabpath is path to Kerberos keytab file, required with " \
        "-s true"
    echo "    kerberos_principal service principal for thrift server, " \
        "required with -s true"
    echo "    All paths must be absolute"
}

while [ "${1}x" != "x" ] ; do
    if [ $1 == "-D" ] ; then
        shift
        dbroot=$1
        shift
    elif [ $1 == "-d" ] ; then
        shift
        dir=$1
        shift
    elif [ $1 == "-f" ] ; then
        shift
        forrest=$1
        shift
    elif [ $1 == "-h" ] ; then
        shift
        hadoop_home=$1
        shift
    elif [ $1 == "-K" ] ; then
        shift
        kerberosprincipal=$1
        kerberosprincipal=${kerberosprincipal/@/\\@}
        shift
    elif [ $1 == "-k" ] ; then
        shift
        keytabpath=$1
        shift
    elif [ $1 == "-m" ] ; then
        shift
        host=$1
        shift
    elif [ $1 == "-p" ] ; then
        shift
        portnum=$1
        shift
    elif [ $1 == "-P" ] ; then
        shift
        passwd=$1
        shift
    elif [ $1 == "-s" ] ; then
        shift
        sasl=$1
        shift
    elif [ $1 == "-t" ] ; then
        shift
        tarball=$1
        shift
    elif [ $1 == "-w" ] ; then
        shift
        warehouseDir=$1
        shift
    else
        echo "Unknown option $1"
        shift
    fi

done

for var in $forrest $dbroot $host $dir $hadoop_home $tarball ; do
    if [ $var == "unknown" ] ; then
        usage
        exit 1
    fi
done

# Make sure root and dbroot are absolute paths

for var in $forrest $dbroot $dir $hadoop_home ; do
    if [ ${var:0:1} != "/" ] ; then
        usage
        exit 1
    fi
done

# Take the src distribution and build an installable tarball
# Copy the tarball over
rm -rf /tmp/${USER}_hcat_scratch
mkdir /tmp/${USER}_hcat_scratch
cd /tmp/${USER}_hcat_scratch
cp $tarball .
tar zxf *
dirname=`ls -1 | grep -v gz`
cd $dirname
ant -Dforrest.home=$forrest tar
tarfiledir=`pwd`
tarfilebase=`ls build/hcatalog-*.tar.gz`
tarfile="$tarfiledir/$tarfilebase"

tfile=/tmp/${USER}_hcat_test_tarball.tgz
scp $tarfile $host:$tfile

# Write a quick perl script to modify the hive-site.xml file
pfile=/tmp/${USER}_hcat_test_hive_site_modify.pl
cat > $pfile <<!
#!/usr/bin/env perl

while (<>) {
    s!DBHOSTNAME!$host!;
    s!SVRHOST!$host!;
    s!PASSWORD!$passwd!;
    s!WAREHOUSE_DIR!$warehouseDir!;
    s!SASL_ENABLED!$sasl!;
    s!KEYTAB_PATH!$keytabpath!;
    s!KERBEROS_PRINCIPAL!$kerberosprincipal!;
    s!PORT!$portnum!;
    print;
}
!
 

# Run the install script
file=/tmp/${USER}_hcat_test_install.sh
cat > $file <<!
#!/usr/bin/env bash
rm -rf /tmp/${USER}_hcat_scratch
mkdir /tmp/${USER}_hcat_scratch
cd /tmp/${USER}_hcat_scratch
cp $tfile .
tar zxf ${USER}_hcat_test_tarball.tgz
cd hcatalog-*
share/hcatalog/scripts/hcat_server_install.sh -r $dir -d $dbroot \
    -h $hadoop_home -p $portnum

chmod +x $pfile
cp $dir/etc/hcatalog/hive-site.xml /tmp/${USER}_hcat_test_hive_site.tmp
$pfile < /tmp/${USER}_hcat_test_hive_site.tmp > $dir/etc/hcatalog/hive-site.xml 
!

scp $file $host:$file
scp $pfile $host:$pfile
ssh $host chmod +x $file
ssh $host $file
if [ $? != "0" ] ; then
    echo "Failed to install hcat"
    exit 1
fi

# Stop the current server
file=/tmp/${USER}_hcat_test_install_stop_server.sh
cat > $file <<!
#!/usr/bin/env bash
export HADOOP_HOME=$hadoop_home
$dir/share/hcatalog/scripts/hcat_server_stop.sh
!
scp $file $host:$file
ssh $host chmod +x $file
ssh $host $file

# Start the server
ssh $host $dir/share/hcatalog/scripts/hcat_server_start.sh
if [ $? != "0" ] ; then
    echo "Failed to start hcat"
    exit 1
fi
