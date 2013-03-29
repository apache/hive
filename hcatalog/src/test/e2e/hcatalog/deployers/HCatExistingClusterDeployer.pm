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

package HCatExistingClusterDeployer;

use IPC::Run qw(run);
use TestDeployer;
use Util;

use strict;
use English;

our @ISA = "TestDeployer";

###########################################################################
# Class: HiveExistingClusterDeployer
# Deploy the Pig harness to a cluster and database that already exists.

##############################################################################
# Sub: new
# Constructor
#
# Paramaters:
# None
#
# Returns:
# None.
sub new
{
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {};

    bless($self, $class);

    return $self;
}

##############################################################################
# Sub: checkPrerequisites
# Check any prerequisites before a deployment is begun.  For example if a 
# particular deployment required the use of a database system it could
# check here that the db was installed and accessible.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub checkPrerequisites
{
    my ($self, $cfg, $log) = @_;

    if (! defined $ENV{'HADOOP_HOME'} || $ENV{'HADOOP_HOME'} eq "") {
        print $log "You must set the environment variable HADOOP_HOME";
        die "HADOOP_HOME not defined";
    }
    if (! defined $ENV{'HCAT_HOME'} || $ENV{'HCAT_HOME'} eq "") {
        print $log "You must set the environment variable HCAT_HOME";
        die "HCAT_HOME not defined";
    }
    if (! defined $ENV{'HIVE_HOME'} || $ENV{'HIVE_HOME'} eq "") {
        print $log "You must set the environment variable HIVEOP_HOME";
        die "HIVE_HOME not defined";
    }

    # Run a quick and easy Hadoop command to make sure we can
    Util::runHadoopCmd($cfg, $log, "fs -ls /");

}

##############################################################################
# Sub: deploy
# Deploy any required packages
# This is a no-op in this case because we're assuming both the cluster and the
# database already exist
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub deploy
{
}

##############################################################################
# Sub: start
# Start any software modules that are needed.
# This is a no-op in this case because we're assuming both the cluster and the
# database already exist
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub start
{
}

##############################################################################
# Sub: generateData
# Generate any data needed for this test run.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub generateData
{
    my ($self, $cfg, $log) = @_;
    my @tables = (
        {
            'name' => "studenttab10k",
            'filetype' => "studenttab",
            'rows' => 10000,
            'hdfs' => "studenttab10k",
        }, {
            'name' => "votertab10k",
            'filetype' => "votertab",
            'rows' => 10000,
            'hdfs' => "votertab10k",
        }, {
            'name' => "studentparttab30k",
            'filetype' => "studentparttab",
            'rows' => 10000,
            'hdfs' => "studentparttab30k",
            'partitions' => ['20110924', '20110925', '20110926']
        },{
            'name' => "studentnull10k",
            'filetype' => "studentnull",
            'rows' => 10000,
            'hdfs' => "studentnull10k",
        },{
            'name' => "all100k",
            'filetype' => "allscalars",
            'rows' => 100000,
            'hdfs' => "all100k",
        },{
            'name' => "all100kjson",
            'filetype' => "json",
            'rows' => 100000,
            'hdfs' => "all100kjson",
        },{
            'name' => "all100krc",
            'filetype' => "studenttab",
            'rows' => 100000,
            'hdfs' => "all100krc",
            'format' => "rc",
        }, {
            'name' => "studentcomplextab10k",
            'filetype' => "studentcomplextab",
            'rows' => 10000,
            'hdfs' => "studentcomplextab10k",
        }
    );

    
#   if (defined($cfg->{'load_hive_only'}) && $cfg->{'load_hive_only'} == 1) {
#       return $self->hiveMetaOnly($cfg, $log, \@tables);
#   }

    # Create the HDFS directories
    my $mkdirCmd = "fs -mkdir";
    if ($ENV{'HCAT_HADOOPVERSION'} eq "23") {
        $mkdirCmd = "fs -mkdir -p"
    }
    Util::runHadoopCmd($cfg, $log, "$mkdirCmd $cfg->{'hcat_data_dir'}");

    foreach my $table (@tables) {
        print "Generating data for $table->{'name'}\n";
        # Generate the data
        my @cmd;
        if (defined($table->{'format'})) {
            @cmd = ($cfg->{'gentool'}, $table->{'filetype'}, $table->{'rows'},
                $table->{'name'}, $cfg->{'hcat_data_dir'}, $table->{'format'});
        } else {
            @cmd = ($cfg->{'gentool'}, $table->{'filetype'}, $table->{'rows'},
                $table->{'name'}, $cfg->{'hcat_data_dir'});
        }
        $self->runCmd($log, \@cmd);

        # Copy the data to HDFS
        my $hadoop = "$mkdirCmd $cfg->{'hcat_data_dir'}/$table->{'hdfs'}";
        Util::runHadoopCmd($cfg, $log, $hadoop);

        if (defined($table->{'partitions'})) {
            foreach my $part (@{$table->{'partitions'}}) {
                my $hadoop = "$mkdirCmd
                    $cfg->{'hcat_data_dir'}/$table->{'hdfs'}/$table->{'name'}.$part";
                Util::runHadoopCmd($cfg, $log, $hadoop);
                my $hadoop = "fs -copyFromLocal $table->{'name'}.$part " .
                    "$cfg->{'hcat_data_dir'}/$table->{'hdfs'}/$table->{'name'}.$part/$table->{'name'}.$part";
                Util::runHadoopCmd($cfg, $log, $hadoop);
            }
        } else {
            my $hadoop = "fs -copyFromLocal $table->{'name'} ".
                "$cfg->{'hcat_data_dir'}/$table->{'hdfs'}/$table->{'name'}";
            Util::runHadoopCmd($cfg, $log, $hadoop);
        }

        print "Loading data into Hive for $table->{'name'}\n";
        Util::runHCatCmdFromFile($cfg, $log,
            "./" . $table->{'name'} .  ".hcat.sql");

        print "Loading data into MySQL for $table->{'name'}\n";
        Util::runDbCmd($cfg, $log, $table->{'name'} . ".mysql.sql");
    }

}

###########################################################################
# Sub: hiveMetaOnly                                                        
# Load metadata into Hive, but don't load Mysql or HDFS, as we assume      
# these have already been loaded.                                          
#                                                                          
# Paramaters:                                                              
# cfg - hash from config file, including deployment config                 
# log - log file handle                                                    
#                                                                          
# Returns:                                                                 
# None                                                                     
#                                                                          
sub hiveMetaOnly
{
    my ($self, $cfg, $log, $tables) = @_;
    foreach my $table (@{$tables}) {
        print "Generating data for $table->{'name'}\n";
        # Generate the data
        my @cmd = ($cfg->{'gentool'}, $table->{'filetype'}, $table->{'rows'},
            $table->{'name'}, $cfg->{'hcat_data_dir'});
        $self->runCmd($log, \@cmd);

        print "Loading data into Hive for $table->{'name'}\n";
        Util::runHCatCmdFromFile($cfg, $log, "./" . $table->{'name'} .
             ".hive.sql");
    }
}

##############################################################################
# Sub: confirmDeployment
# Run checks to confirm that the deployment was successful.  When this is 
# done the testing environment should be ready to run.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# Nothing
# This method should die with an appropriate error message if there is 
# an issue.
#
sub confirmDeployment
{
}

##############################################################################
# Sub: deleteData
# Remove any data created that will not be removed by undeploying.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub deleteData
{
}

##############################################################################
# Sub: stop
# Stop any servers or systems that are no longer needed once testing is
# completed.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub stop
{
}

##############################################################################
# Sub: undeploy
# Remove any packages that were installed as part of the deployment.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub undeploy
{
}

##############################################################################
# Sub: confirmUndeployment
# Run checks to confirm that the undeployment was successful.  When this is 
# done anything that must be turned off or removed should be turned off or
# removed.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# Nothing
# This method should die with an appropriate error message if there is 
# an issue.
#
sub confirmUndeployment
{
    die "$0 INFO : confirmUndeployment is a virtual function!";
}

sub runCmd($$$)
{
    my ($self, $log, $cmd) = @_;

    print $log "Going to run [" . join(" ", @$cmd) . "]\n";

    run($cmd, \undef, $log, $log) or
        die "Failed running " . join(" ", @$cmd) . "\n";
}

1;
