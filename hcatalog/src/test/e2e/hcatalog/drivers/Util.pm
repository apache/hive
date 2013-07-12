#!/usr/bin/env perl

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

###########################################################################
# Class: Util
#
# A collection of  helper subroutines.
#


package Util;

use IPC::Run qw(run);
use strict;
use English;

sub prepareHCat
{
    my ($self, $testCmd, $log) = @_;
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $hcatCmd = replaceParameters( $testCmd->{'hcat_prep'}, $outfile, $testCmd, $log);

    my @hivefiles = ();
    my @outfiles = ();
    # Write the hive script to a file.
    $hivefiles[0] = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" .
        $testCmd->{'num'} . ".0.sql";
    $outfiles[0] = $testCmd->{'thisResultsPath'} . "/" . $testCmd->{'group'} .
        "_" .  $testCmd->{'num'} . ".0.out";

    open(FH, "> $hivefiles[0]") or
        die "Unable to open file $hivefiles[0] to write SQL script, $ERRNO\n";
    print FH $testCmd->{'hcat_prep'} . "\n";
    close(FH);

    Util::runHCatCmdFromFile($testCmd, $log, $hivefiles[0]);
}

sub getHadoopCmd
{
    my ( $properties ) = @_;

    my $subName        = (caller(0))[3];
    my @baseCmd;

    die "$0.$subName: null properties" if (! $properties );

    my $cmd;

    $cmd = $properties->{'hadoopbin'};
    if ( ! -x "$cmd" ) {
      print STDERR "\n$0::$subName WARNING: Can't find hadoop command: $cmd\n";
      $cmd = `which hadoop`;
      chomp $cmd;
      print STDERR "$0::$subName WARNING: Instead using command: $cmd\n";
    }
    if ( ! -x "$cmd" ) {
      die "\n$0::$subName FATAL: Hadoop command does not exist: $cmd\n";
    }
    push (@baseCmd, $cmd);

    push (@baseCmd, '--config', $properties->{'hadoopconfdir'})
        if defined($properties->{'hadoopconfdir'});

    return @baseCmd;
}

##############################################################################
#  Sub: runHiveCmdFromFile
#
#  Run the provided file using the Hive command line.
#
#  cfg - The configuration file for the test
#  log - reference to the log file, should be an open file pointer
#  sql - name of file containing SQL to run.  Optional, if present -f $sql
#    will be appended to the command.
#  outfile - open file pointer (or variable reference) to write stdout to for
#    this test.  Optional, will be written to $log if this value is not
#    provided.
#  outfile - open file pointer (or variable reference) to write stderr to for
#    this test.  Optional, will be written to $log if this value is not
#    provided.
#  noFailOnFail - if true, do not fail when the Hive command returns non-zero
#    value.
#  Returns:
#  Nothing
sub runHiveCmdFromFile($$;$$$$)
{
    my ($cfg, $log, $sql, $outfile, $errfile, $noFailOnFail) = @_;

    if (!defined($ENV{'HADOOP_HOME'})) {
        die "Cannot run hive when HADOOP_HOME environment variable is not set.";
    }

    $outfile = $log if (!defined($outfile));
    $errfile = $log if (!defined($errfile));

    my @cmd = ($cfg->{'hivebin'});
  
    $ENV{'HIVE_CONF_DIR'} = $cfg->{'hiveconf'};
    $ENV{'HIVE_AUX_JARS_PATH'} = $cfg->{'hcatshare'};
 
    if (defined($cfg->{'hivecmdargs'})) {
        push(@cmd, @{$cfg->{'hivecmdargs'}});
    }

    if (defined($cfg->{'hiveops'})) {
        $ENV{'HIVE_OPTS'} = join(" ", @{$cfg->{'hiveops'}});
    }

    $ENV{'HIVE_HOME'} = $cfg->{'hivehome'};

    my $envStr;
    for my $k (keys(%ENV)) {
        $envStr .= $k . "=" . $ENV{$k} . " " if ($k =~ /HADOOP/ || $k =~ /HIVE/);
    }
    $envStr .= " ";

    if (defined($sql)) {
        push(@cmd, "-f", $sql);
    }
    print $log "Going to run hive command [" . join(" ", @cmd) .
        "] with environment set to [$envStr]\n";
    my $runrc = run(\@cmd, \undef, $outfile, $errfile);
    my $rc = $? >> 8;

    return $runrc if $runrc; # success

    if (defined($noFailOnFail) && $noFailOnFail) {
        return $rc;
    } else {
        die "Failed running hive command [" . join(" ", @cmd) . "]\n";
    }
}

#############################################################################
#  Sub: runHiveCmdFromFile
#
#  Run the provided file using the Hive command line.
#
#  cfg - The configuration file for the test
#  log - reference to the log file, should be an open file pointer
#  sql - name of file containing SQL to run.  Optional, if present -f $sql
#    will be appended to the command.
#  outfile - open file pointer (or variable reference) to write stdout to for
#    this test.  Optional, will be written to $log if this value is not
#    provided.
#  outfile - open file pointer (or variable reference) to write stderr to for
#    this test.  Optional, will be written to $log if this value is not
#    provided.
#  noFailOnFail - if true, do not fail when the Hive command returns non-zero
#    value.
#  Returns:
#  Nothing
sub runHCatCmdFromFile($$;$$$$)
{
    my ($cfg, $log, $sql, $outfile, $errfile, $noFailOnFail) = @_;

    if (!defined($ENV{'HADOOP_HOME'})) {
        die "Cannot run hcat when HADOOP_HOME environment variable is not set.";
    }

    $outfile = $log if (!defined($outfile));
    $errfile = $log if (!defined($errfile));

    # unset HADOOP_CLASSPATH
#   $ENV{'HADOOP_CLASSPATH'} = "";
    $ENV{'HADOOP_CLASSPATH'} = $cfg->{'hbaseconf'};
    $ENV{'HCAT_CLASSPATH'} = Util::getHBaseLibs($cfg, $log);

    my @cmd;
    if (defined($sql)) {
        @cmd = ("$cfg->{'hcatbin'}", "-f", $sql);
    } else {
        @cmd = ("$cfg->{'hcatbin'}");
    }

    my $envStr;
    for my $k (keys(%ENV)) {
        $envStr .= $k . "=" . $ENV{$k} . " " if ($k =~ /HADOOP/ || $k =~ /HIVE/ ||
                $k =~ /HCAT/);
    }
    $envStr .= " ";
    print $log "Going to run hcat command [" . join(" ", @cmd) .
        "] with environment set to [$envStr]\n";
    my $runrc = run(\@cmd, \undef, $outfile, $errfile);
    my $rc = $? >> 8;

    return $runrc if $runrc; # success

    if (defined($noFailOnFail) && $noFailOnFail) {
        return $rc;
    } else {
        die "Failed running hcat command [" . join(" ", @cmd) . "]\n";
    }
}

##############################################################################
#  Sub: runDbCmd
#
#  Run the provided mysql command
#
#  Returns:
#  Nothing
sub runDbCmd($$$;$)
{
    my ($cfg, $log, $sqlfile, $outfile) = @_;

    $outfile = $log if (!defined($outfile));

    open(SQL, "< $sqlfile") or die "Unable to open $sqlfile for reading, $!\n";

    my @cmd = ('mysql', '-u', $cfg->{'dbuser'}, '-D', $cfg->{'dbdb'},
        '-h', $cfg->{'dbhost'}, "--password=$cfg->{'dbpasswd'}",
        "--skip-column-names","--local-infile");

    print $log "Going to run [" . join(" ", @cmd) . "] passing in [$sqlfile]\n";

    run(\@cmd, \*SQL, $outfile, $log) or
        die "Failed running " . join(" ", @cmd) . "\n";
    close(SQL);
}

#  Sub: runHadoopCmd
#
#  Run the provided hadoop command
#
#  Returns:
#  Nothing
sub runHadoopCmd($$$)
{
    my ($cfg, $log, $c) = @_;

    my @cmd = ("$ENV{'HADOOP_HOME'}/bin/hadoop");
    push(@cmd, split(' ', $c));

    print $log "Going to run [" . join(" ", @cmd) . "]\n";

    run(\@cmd, \undef, $log, $log) or
        die "Failed running " . join(" ", @cmd) . "\n";
}


sub show_call_stack {
  my ( $path, $line, $subr );
  my $max_depth = 30;
  my $i = 1;
    print("--- Begin stack trace ---");
    while ( (my @call_details = (caller($i++))) && ($i<$max_depth) ) {
      print("$call_details[1] line $call_details[2] in function $
+call_details[3]");
    print("--- End stack trace ---");
  }
}


sub getPigCmd
{
    my ( $cfg, $log ) = @_;

    my @cmd = ("$cfg->{'pigbin'}");

    
    # sets the queue, for exampel "grideng"
    if(defined($cfg->{'queue'})) {
        push( @cmd,'-Dmapred.job.queue.name='.$cfg->{'queue'});
    }
    
    my $cp = Util::getHCatLibs($cfg, $log) .  Util::getHiveLibsForPig($cfg, $log) .
        Util::getHBaseLibs($cfg, $log);
    push(@cmd, ('-Dpig.additional.jars='. $cp));
    $cp .= ':' . $cfg->{'hiveconf'};
    $cp .= ':' . $cfg->{'hbaseconf'};
    $ENV{'PIG_CLASSPATH'} = $cp;
    
    # sets the permissions on the jobtracker for the logs
    push( @cmd,'-Dmapreduce.job.acl-view-job=*');


      # Set local mode PIG option
    if ( defined($cfg->{'exectype'}) && $cfg->{'exectype'}=~ "local" ) {
        push(@cmd, ('-x', 'local'));
    }

    return @cmd;
}


sub setLocale
{
   my $locale= shift;
#   $locale = "en_US.UTF-8" if ( !$locale );
$locale = "ja_JP.utf8" if ( !$locale );
   $ENV['LC_CTYPE']="$locale";
   $ENV['LC_NUMERIC']="$locale";
   $ENV['LC_TIME']="$locale";
   $ENV['LC_COLLATE']="$locale";
   $ENV['LC_MONETARY']="$locale";
   $ENV['LC_MESSAGES']="$locale";
   $ENV['LC_PAPER']="$locale";
   $ENV['LC_NAME']="$locale";
   $ENV['LC_ADDRESS']="$locale";
   $ENV['LC_TELEPHONE']="$locale";
   $ENV['LC_MEASUREMENT']="$locale";
   $ENV['LC_IDENTIFICATION']="$locale";
}

sub getLocaleCmd 
{
  my $locale= shift;
  $locale = "en_US.UTF-8" if ( !$locale );

  return     "export LC_CTYPE=\"$locale\";"
          ."export LC_NUMERIC=\"$locale\";"
          ."export LC_TIME=\"$locale\";"
          ."export LC_COLLATE=\"$locale\";"
          ."export LC_MONETARY=\"$locale\";"
          ."export LC_MESSAGES=\"$locale\";"
          ."export LC_PAPER=\"$locale\";"
          ."export LC_NAME=\"$locale\";"
          ."export LC_ADDRESS=\"$locale\";"
          ."export LC_TELEPHONE=\"$locale\";"
          ."export LC_MEASUREMENT=\"$locale\";"
          ."export LC_IDENTIFICATION=\"$locale\"";
}

sub replaceParameters
{

    my ($cmd, $outfile, $testCmd, $log) = @_;

    # $self
# $cmd =~ s/:LATESTOUTPUTPATH:/$self->{'latestoutputpath'}/g;

    # $outfile
    $cmd =~ s/:OUTPATH:/$outfile/g;

    # $ENV
    $cmd =~ s/:PIGHARNESS:/$ENV{HARNESS_ROOT}/g;

    # $testCmd
    $cmd =~ s/:INPATH:/$testCmd->{'inpathbase'}/g;
    $cmd =~ s/:OUTPATH:/$outfile/g;
    $cmd =~ s/:OUTPATHPARENT:/$testCmd->{'outpath'}/g;
    $cmd =~ s/:FUNCPATH:/$testCmd->{'funcjarPath'}/g;
    $cmd =~ s/:PIGPATH:/$testCmd->{'pighome'}/g;
    $cmd =~ s/:RUNID:/$testCmd->{'UID'}/g;
    $cmd =~ s/:USRHOMEPATH:/$testCmd->{'userhomePath'}/g;
    $cmd =~ s/:MAPREDJARS:/$testCmd->{'mapredjars'}/g;
    $cmd =~ s/:SCRIPTHOMEPATH:/$testCmd->{'scriptPath'}/g;
    $cmd =~ s/:DBUSER:/$testCmd->{'dbuser'}/g;
    $cmd =~ s/:DBNAME:/$testCmd->{'dbdb'}/g;
#    $cmd =~ s/:LOCALINPATH:/$testCmd->{'localinpathbase'}/g;
#    $cmd =~ s/:LOCALOUTPATH:/$testCmd->{'localoutpathbase'}/g;
#    $cmd =~ s/:LOCALTESTPATH:/$testCmd->{'localpathbase'}/g;
    $cmd =~ s/:BMPATH:/$testCmd->{'benchmarkPath'}/g;
    $cmd =~ s/:TMP:/$testCmd->{'tmpPath'}/g;
    $cmd =~ s/:HDFSTMP:/tmp\/$testCmd->{'runid'}/g;
    $cmd =~ s/:HCAT_JAR:/$testCmd->{'libjars'}/g;

    if ( $testCmd->{'hadoopSecurity'} eq "secure" ) { 
      $cmd =~ s/:REMOTECLUSTER:/$testCmd->{'remoteSecureCluster'}/g;
    } else {
      $cmd =~ s/:REMOTECLUSTER:/$testCmd->{'remoteNotSecureCluster'}/g;
    }

    return $cmd;
}

sub getHiveLibs($$)
{
    my ($cfg, $log) = @_;

    my $cp;
    opendir(LIB, $cfg->{'hivelib'}) or die "Cannot open $cfg->{'hivelib'}, $!\n";
    my @jars = readdir(LIB);
    foreach (@jars) {
        /\.jar$/ && do {
            $cp .= $cfg->{'hivelib'} . '/' . $_ . ':';
        };
    }
    closedir(LIB);
    return $cp;
}

# Pig needs a limited set of the Hive libs, since they include some of the same jars
# and we get version mismatches if it picks up all the libraries.
sub getHiveLibsForPig($$)
{
    my ($cfg, $log) = @_;

    my $cp;
    opendir(LIB, $cfg->{'hivelib'}) or die "Cannot open $cfg->{'hivelib'}, $!\n";
    my @jars = readdir(LIB);
    foreach (@jars) {
        /hive-.*\.jar$/ && do {
            $cp .= $cfg->{'hivelib'} . '/' . $_ . ':';
        };
        /libfb303-.*\.jar/ && do {
            $cp .= $cfg->{'hivelib'} . '/' . $_ . ':';
        };
        /libthrift-.*\.jar/ && do {
            $cp .= $cfg->{'hivelib'} . '/' . $_ . ':';
        };
        /datanucleus-.*\.jar$/ && do {
            $cp .= $cfg->{'hivelib'} . '/' . $_ . ':';
        };
        /jdo-api-.*\.jar$/ && do {
            $cp .= $cfg->{'hivelib'} . '/' . $_ . ':';
        };
        /bonecp-.*\.jar$/ && do {
            $cp .= $cfg->{'hivelib'} . '/' . $_ . ':';
        };
        /commons-pool-.*\.jar$/ && do {
            $cp .= $cfg->{'hivelib'} . '/' . $_ . ':';
        };
#       /hbase-.*\.jar$/ && do {
#           $cp .= $cfg->{'hivelib'} . '/' . $_ . ':';
#       };
#       /zookeeper-.*\.jar$/ && do {
#           $cp .= $cfg->{'hivelib'} . '/' . $_ . ':';
#       };
    }
    closedir(LIB);
    return $cp;
}

sub getHBaseLibs($$)
{
    my ($cfg, $log) = @_;

    my $cp;
    opendir(LIB, $cfg->{'hbaselibdir'}) or die "Cannot open $cfg->{'hbaselibdir'}, $!\n";
    my @jars = readdir(LIB);
    foreach (@jars) {
        /hbase-.*\.jar$/ && do {
            $cp .= $cfg->{'hbaselibdir'} . '/' . $_ . ':';
        };
    }
    closedir(LIB);
    opendir(LIB, $cfg->{'zklibdir'}) or die "Cannot open $cfg->{'zklibdir'}, $!\n";
    my @jars = readdir(LIB);
    foreach (@jars) {
        /zookeeper.*\.jar$/ && do {
            $cp .= $cfg->{'zklibdir'} . '/' . $_ . ':';
        };
    }
    closedir(LIB);
    return $cp;
}
 

sub getHCatLibs($$)
{
    my ($cfg, $log) = @_;

    my $cp;
    opendir(LIB, $cfg->{'hcatshare'}) or die "Cannot open $cfg->{'hcatshare'}, $!\n";
    my @jars = readdir(LIB);
    foreach (@jars) {
        (/hcatalog-core-[0-9].*\.jar$/ || /hcatalog-pig-adapter-[0-9].*\.jar$/) && do {
            $cp .= $cfg->{'hcatshare'} . '/' . $_ . ':';
        };
    }
    closedir(LIB);
    opendir(LIB, $cfg->{'hcatlib'}) or die "Cannot open $cfg->{'hcatlib'}, $!\n";
    my @jars = readdir(LIB);
    foreach (@jars) {
        /hbase-storage-handler.*\.jar$/ && do {
            $cp .= $cfg->{'hcatlib'} . '/' . $_ . ':';
        };
    }
    closedir(LIB);

    # Get jars required non-hcat jars that are not distributed with Hadoop or Hive
    opendir(LIB, $cfg->{'hcatcoredevlib'}) or die "Cannot open $cfg->{'hcatcoredevlib'}, $!\n";
    my @jars = readdir(LIB);
    foreach (@jars) {
        /guava.*\.jar$/ && do {
            $cp .= $cfg->{'hcatcoredevlib'} . '/' . $_ . ':';
        };
    }
    closedir(LIB);


    return $cp;
}
        

1;
