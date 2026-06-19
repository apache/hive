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

package TestDriverHCat;

###############################################################################
# Test driver for pig nightly tests.
# 
#

use TestDriver;
use IPC::Run; # don't do qw(run), it screws up TestDriver which also has a run method
use Digest::MD5 qw(md5_hex);
use Util;
use File::Path;
use Cwd;

use strict;
use English;

our $className= "TestDriver";
our @ISA = "$className";
our $ROOT = (defined $ENV{'HARNESS_ROOT'} ? $ENV{'HARNESS_ROOT'} : die "ERROR: You must set environment variable HARNESS_ROOT\n");
our $toolpath = "$ROOT/libexec/PigTest";

my $passedStr  = 'passed';
my $failedStr  = 'failed';
my $abortedStr = 'aborted';
my $skippedStr = 'skipped';
my $dependStr  = 'failed_dependency';

sub new
{
    # Call our parent
    my ($proto) = @_;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new;

    bless($self, $class);
    return $self;
}



sub globalSetup
{
    my ($self, $globalHash, $log) = @_;

    # Setup the output path
    my $me = `whoami`;
    chomp $me;
    my $jobId = $globalHash->{'job-id'};
    my $timeId = time;
    $globalHash->{'runid'} = $me . "-" . $timeId . "-" . $jobId;

    # if "-ignore false" was provided on the command line,
    # it means do run tests even when marked as 'ignore'
    if(defined($globalHash->{'ignore'}) && $globalHash->{'ignore'} eq 'false')
    {
        $self->{'ignore'} = 'false';
    }

    $globalHash->{'outpath'} = $globalHash->{'outpathbase'} . "/" . $globalHash->{'runid'} . "/";
    $globalHash->{'localpath'} = $globalHash->{'localpathbase'} . "/" . $globalHash->{'runid'} . "/";
    $globalHash->{'tmpPath'} = $globalHash->{'tmpPath'} . "/" . $globalHash->{'runid'} . "/";
}

sub globalSetupConditional
{
    my ($self, $globalHash, $log) = @_;

    # add libexec location to the path
    if (defined($ENV{'PATH'})) {
        $ENV{'PATH'} = $globalHash->{'scriptPath'} . ":" . $ENV{'PATH'};
    }
    else {
        $ENV{'PATH'} = $globalHash->{'scriptPath'};
    }
}

sub globalCleanup
{
}


sub runTest
{
    my ($self, $testCmd, $log) = @_;
    my $subName  = (caller(0))[3];

    # Handle the various methods of running used in 
    # the original TestDrivers

    if ( $testCmd->{'hcat'} ) {
       return $self->runHCatCmdLine( $testCmd, $log, 1);
    } else {
       die "$subName FATAL Did not find a testCmd that I know how to handle";
    }
}


sub runHCatCmdLine
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $hcatCmd = Util::replaceParameters( $testCmd->{'hcat'}, $outfile, $testCmd, $log);
    my $outdir  = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my ($stdoutfile, $stderrfile);

    mkpath( [ $outdir ] , 0, 0755) if ( ! -e outdir );
    if ( ! -e $outdir ){
       print $log "$0.$subName FATAL could not mkdir $outdir\n";
       die "$0.$subName FATAL could not mkdir $outdir\n";
    }

    open($stdoutfile, "> $outdir/stdout");
    open($stderrfile, "> $outdir/stderr");

    my @hcatfiles = ();
    my @outfiles = ();
    # Write the hive script to a file.
    $hcatfiles[0] = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" .
        $testCmd->{'num'} . ".0.sql";
    $outfiles[0] = $testCmd->{'thisResultsPath'} . "/" . $testCmd->{'group'} .
        "_" .  $testCmd->{'num'} . ".0.out";

    # Append -p to dfs -mkdir to work with Hadoop23.
    if ($ENV{'HCAT_HADOOPVERSION'} eq "23") {
        if ($hcatCmd =~ /\bdfs -mkdir/) {
            $hcatCmd=~s/\bdfs\s+-mkdir\s+(-p\s+)?/dfs -mkdir -p /g;
        }   
    }

    open(FH, "> $hcatfiles[0]") or
        die "Unable to open file $hcatfiles[0] to write SQL script, $ERRNO\n";
    print FH $hcatCmd . "\n";
    close(FH);

    Util::runHCatCmdFromFile($testCmd, $log, $hcatfiles[0], $stdoutfile, $stderrfile, 1);
    $result{'rc'} = $? >> 8;
    $result{'stdout'} = `cat $outdir/stdout`;
    $result{'stderr'} = `cat $outdir/stderr`;
    $result{'stderr_file'} = "$outdir/stderr";
    return \%result;
}

sub compareScript
{
    my ($self, $testResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];


    # IMPORTANT NOTES:
    #
    # If you are using a regex to compare stdout or stderr
    # and if the pattern that you are trying to match spans two line
    # explicitly use '\n' (without the single quotes) in the regex
    #
    # If any verification directives are added here 
    # do remember also to add them to the hasCommandLineVerifications subroutine.
    #
    # If the test conf file misspells the directive, you won't be told...
    # 

    my $result = 1;  # until proven wrong...


    # Return Code
    if (defined $testCmd->{'rc'}) {                                                                             
      print $log "$0::$subName INFO Checking return code " .
                 "against expected <$testCmd->{'rc'}>\n";
      if ( (! defined $testResult->{'rc'}) || ($testResult->{'rc'} != $testCmd->{'rc'})) {                                                         
        print $log "$0::$subName INFO Check failed: rc = <$testCmd->{'rc'}> expected, test returned rc = <$testResult->{'rc'}>\n";
        $result = 0;
      }
    }

    # Standard Out
    if (defined $testCmd->{'expected_out'}) {
      print $log "$0::$subName INFO Checking test stdout' " .
              "as exact match against expected <$testCmd->{'expected_out'}>\n";
      if ($testResult->{'stdout'} ne $testCmd->{'expected_out'}) {
        print $log "$0::$subName INFO Check failed: exact match of <$testCmd->{'expected_out'}> expected in stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_out'}) {
      print $log "$0::$subName INFO Checking test stdout " .
              "as NOT exact match against expected <$testCmd->{'expected_out'}>\n";
      if ($testResult->{'stdout'} eq $testCmd->{'not_expected_out'}) {
        print $log "$0::$subName INFO Check failed: NON-match of <$testCmd->{'expected_out'}> expected to stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'expected_out_regex'}) {
      print $log "$0::$subName INFO Checking test stdout " .
              "for regular expression <$testCmd->{'expected_out_regex'}>\n";
      if ($testResult->{'stdout'} !~ $testCmd->{'expected_out_regex'}) {
        print $log "$0::$subName INFO Check failed: regex match of <$testCmd->{'expected_out_regex'}> expected in stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_out_regex'}) {
      print $log "$0::$subName INFO Checking test stdout " .
              "for NON-match of regular expression <$testCmd->{'not_expected_out_regex'}>\n";
      if ($testResult->{'stdout'} =~ $testCmd->{'not_expected_out_regex'}) {
        print $log "$0::$subName INFO Check failed: regex NON-match of <$testCmd->{'not_expected_out_regex'}> expected in stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    # Standard Error
    if (defined $testCmd->{'expected_err'}) {
      print $log "$0::$subName INFO Checking test stderr " .
              "as exact match against expected <$testCmd->{'expected_err'}>\n";
      if ($testResult->{'stderr'} ne $testCmd->{'expected_err'}) {
        print $log "$0::$subName INFO Check failed: exact match of <$testCmd->{'expected_err'}> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_err'}) {
      print $log "$0::$subName INFO Checking test stderr " .
              "as NOT an exact match against expected <$testCmd->{'expected_err'}>\n";
      if ($testResult->{'stderr'} eq $testCmd->{'not_expected_err'}) {
        print $log "$0::$subName INFO Check failed: NON-match of <$testCmd->{'expected_err'}> expected to stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'expected_err_regex'}) {
      print $log "$0::$subName INFO Checking test stderr " .
              "for regular expression <$testCmd->{'expected_err_regex'}>\n";
      if ($testResult->{'stderr'} !~ m/$testCmd->{'expected_err_regex'}/ms) {
        print $log "$0::$subName INFO Check failed: regex match of <$testCmd->{'expected_err_regex'}> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_err_regex'}) {
      print $log "$0::$subName INFO Checking test stderr " .
              "for NON-match of regular expression <$testCmd->{'not_expected_err_regex'}>\n";
      if ($testResult->{'stderr'} =~ $testCmd->{'not_expected_err_regex'}) {
        print $log "$0::$subName INFO Check failed: regex NON-match of <$testCmd->{'not_expected_err_regex'}> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

  return $result;
}

sub compare
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;

    # Return Code
    return $self->compareScript ( $testResult, $log, $testCmd);
}

sub generateBenchmark
{
}

1;
