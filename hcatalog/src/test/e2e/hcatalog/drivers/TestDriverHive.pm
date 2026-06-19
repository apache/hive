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

package TestDriverHive;

###############################################################################
# Test driver for hive nightly tests.
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
our $toolpath = "$ROOT/libexec/HCatTest";

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

    $globalHash->{'localpath'} = $globalHash->{'localpathbase'} . "/" . $globalHash->{'runid'} . "/";
    $globalHash->{'tmpPath'} = $globalHash->{'tmpPath'} . "/" . $globalHash->{'runid'} . "/";

    IPC::Run::run(['mkdir', '-p', $globalHash->{'localpath'}], \undef, $log, $log) or 
        die "Cannot create localpath directory " . $globalHash->{'localpath'} .
        " " . "$ERRNO\n";

    IPC::Run::run(['mkdir', '-p', $globalHash->{'benchmarkPath'}], \undef, $log, $log) or 
        die "Cannot create benchmark directory " .  $globalHash->{'benchmarkPath'} .
        " " . "$ERRNO\n";

    $globalHash->{'thisResultsPath'} = $globalHash->{'localpath'} . "/"
        . $globalHash->{'resultsPath'};
    IPC::Run::run(['mkdir', '-p', $globalHash->{'thisResultsPath'}], \undef, $log, $log) or 
        die "Cannot create results directory " .  $globalHash->{'thisResultsPath'} .
        " " . "$ERRNO\n";
}

sub globalCleanup
{
    my ($self, $globalHash, $log) = @_;
}


sub runTest
{
    my ($self, $testCmd, $log) = @_;

    my %result;

    my @hivefiles = ();
    my @outfiles = ();
    # Write the hive script to a file.
    $hivefiles[0] = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" .
        $testCmd->{'num'} . ".0.sql";
    $outfiles[0] = $testCmd->{'thisResultsPath'} . "/" . $testCmd->{'group'} .
        "_" .  $testCmd->{'num'} . ".0.out";

    open(FH, "> $hivefiles[0]") or
        die "Unable to open file $hivefiles[0] to write SQL script, $ERRNO\n";
    print FH $testCmd->{'sql'} . "\n";
    close(FH);

    # If the results are written to a table run the command and then 
    # run a another Hive command to dump the results of the table.
    if (defined($testCmd->{'result_table'})) {
        Util::runHiveCmdFromFile($testCmd, $log, $hivefiles[0]);
        $result{'rc'} = $? >> 8;

        my @results = ();
        if (ref($testCmd->{'result_table'}) ne 'ARRAY') {
            $results[0] = $testCmd->{'result_table'};
        } else {
            @results = @{$testCmd->{'result_table'}};
        }
        for (my $i = 0; $i < @results; $i++) {
            $hivefiles[$i] = $testCmd->{'localpath'} .
                $testCmd->{'group'} . "_" .  $testCmd->{'num'} .
                ".dumptable.$i.sql";
            $outfiles[$i] = $testCmd->{'thisResultsPath'} . "/" .
                $testCmd->{'group'} .  "_" .  $testCmd->{'num'} . ".$i.out";
            open(FH, "> $hivefiles[$i]") or
                die "Unable to open file $hivefiles[$i] to write SQL " .
                    "script, $ERRNO\n";
            print FH "select * from " . $results[$i] .  ";\n";
            close(FH);
        }
    }

    my @originalOutputs = ();
    my @outputs = ();
    $result{'originalOutput'} = \@originalOutputs;
    $result{'output'} = \@outputs;

    for (my $i = 0; $i < @hivefiles; $i++) {
        my $outfp;
        open($outfp, "> $outfiles[$i]") or
            die "Unable to open output file $outfiles[$i], $!\n";

        Util::runHiveCmdFromFile($testCmd, $log, $hivefiles[$i], $outfp);

        # Don't overwrite rc if we set it above
        $result{'rc'} = $? >> 8 unless defined $result{'rc'};
        close($outfp);

        $originalOutputs[$i] = $outfiles[$i];
        $outputs[$i] = 
            $self->postProcessSingleOutputFile($outfiles[$i], $testCmd, $log);
    }

    # Compare doesn't get the testCmd hash, so I need to stuff the necessary
    # info about sorting into the result.
    if (defined $testCmd->{'sortArgs'} && $testCmd->{'sortArgs'}) {
        $result{'sortArgs'} = $testCmd->{'sortArgs'};
    }

    return \%result;
}



sub generateBenchmark
{
    my ($self, $testCmd, $log) = @_;

    my %result;

    # Write the SQL to a file.
    my @verifies = ();
    if (defined $testCmd->{'verify_sql'}) {
        if (ref($testCmd->{'verify_sql'}) eq "ARRAY") {
            @verifies = @{$testCmd->{'verify_sql'}};
        } else {
            $verifies[0] = $testCmd->{'verify_sql'};
        }
    } else {
        $verifies[0] = $testCmd->{'sql'};
    }

    my @rcs = ();
    $result{'rc'} = \@rcs;
    my @outputs = ();
    $result{'output'} = \@outputs;
    for (my $i = 0; $i < @verifies; $i++) {
        my $sqlfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" .
            $testCmd->{'num'} . ".benchmark.$i.sql";
        my $outfile = $testCmd->{'benchmarkPath'} . "/" .
            $testCmd->{'group'} .  "_" .  $testCmd->{'num'} .
            ".benchmark.$i.out";

        open(FH, "> $sqlfile") or
            die "Unable to open file $sqlfile to write SQL script, $ERRNO\n";
        print FH $verifies[$i];
        close(FH);

        my $outfp;
        open($outfp, "> $outfile") or
            die "Unable to open output file $outfile, $!\n";

        Util::runDbCmd($testCmd, $log, $sqlfile, $outfp);
        $rcs[$i] =  $? >> 8;
        close($outfp);

        $outputs[$i] = 
            $self->postProcessSingleOutputFile($outfile, $testCmd, $log, 1);
    }

    return \%result;
}

sub compare
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;

    # Make sure we have the same number of results from runTest and
    # generateBenchmark
    if (scalar(@{$testResult->{'output'}}) != 
            scalar(@{$benchmarkResult->{'output'}})) {
        die "runTest returned " .  scalar(@{$testResult->{'output'}}) .
            " results, but generateBenchmark returned " .
            scalar(@{$benchmarkResult->{'output'}}) . "\n";
    }

    my $totalFailures = 0;
    for (my $i = 0; $i < @{$testResult->{'output'}}; $i++) {
        # cksum the the two files to see if they are the same
        my ($testChksm, $benchmarkChksm);
        IPC::Run::run((['cat', @{$testResult->{'output'}}[$i]], '|',
            ['cksum']), \$testChksm, $log) or
            die "$0: error: cannot run cksum on test results\n";
        IPC::Run::run((['cat', @{$benchmarkResult->{'output'}}[$i]], '|',
            ['cksum']), \$benchmarkChksm, $log) or
            die "$0: error: cannot run cksum on benchmark\n";

        chomp $testChksm;
        chomp $benchmarkChksm;
        print $log
            "test cksum: $testChksm\nbenchmark cksum: $benchmarkChksm\n";

        if ($testChksm ne $benchmarkChksm) {
            print $log "Test output $i checksum does not match benchmark " .
                "checksum\n";
            print $log "Test $i checksum = <$testChksm>\n";
            print $log "Expected $i checksum = <$benchmarkChksm>\n";
            print $log "RESULTS DIFFER: vimdiff " . cwd .
                "/" . @{$testResult->{'output'}}[$i] . " " . cwd .
                "/" . @{$benchmarkResult->{'output'}}[$i] . "\n";
            $totalFailures++;
        }

        # Now, check if the sort order is specified
        if (defined($testResult->{'sortArgs'})) {
            my @sortChk = ('sort', '-cs');
            push(@sortChk, @{$testResult->{'sortArgs'}});
            push(@sortChk, @{$testResult->{'originalOutput'}}[$i]);
            print $log "Going to run sort check command: " .
                join(" ", @sortChk) . "\n";
            IPC::Run::run(\@sortChk, \undef, $log, $log);
            my $sortrc = $?;
            if ($sortrc) {
                print $log "Sort check failed\n";
                $totalFailures++;
            }
        }
    }

    return $totalFailures == 0;
}

sub postProcessSingleOutputFile
{
    my ($self, $outfile, $testCmd, $log, $isBenchmark) = @_;

    # If requested, process the data to smooth over floating point
    # differences.
    if (defined $testCmd->{'floatpostprocess'} &&
            defined $testCmd->{'delimiter'}) {
        # Move the file to a temp file and run through the pre-processor.
        my $tmpfile = "$outfile.tmp";
        link($outfile, $tmpfile) or
            die "Unable to create temporary file $tmpfile, $!\n";
        unlink($outfile) or
            die "Unable to unlink file $outfile, $!\n";
        open(IFH, "< $tmpfile") or
            die "Unable to open file $tmpfile, $!\n";
        open(OFH, "> $outfile") or
            die "Unable to open file $outfile, $!\n";
        my @cmd = ("$toolpath/floatpostprocessor.pl",
            $testCmd->{'delimiter'});
        print $log "Going to run [" . join(" ", @cmd) . "]\n";
        IPC::Run::run(\@cmd, \*IFH, \*OFH, $log) or 
            die "Failed to run float postprocessor, $!\n"; 
        close(IFH);
        close(OFH);
        unlink($tmpfile);
    }

    if ($isBenchmark && defined $testCmd->{'nullpostprocess'}) {
        # Move the file to a temp file and run through the pre-processor.
        my $tmpfile = "$outfile.tmp";
        link($outfile, $tmpfile) or
            die "Unable to create temporary file $tmpfile, $!\n";
        unlink($outfile) or
            die "Unable to unlink file $outfile, $!\n";
        open(IFH, "< $tmpfile") or
            die "Unable to open file $tmpfile, $!\n";
        open(OFH, "> $outfile") or
            die "Unable to open file $outfile, $!\n";
        my @cmd = ("sed", "s/NULL//g");
        print $log "Going to run [" . join(" ", @cmd) . "]\n";
        IPC::Run::run(\@cmd, \*IFH, \*OFH, $log) or 
            die "Failed to run float postprocessor, $!\n"; 
        close(IFH);
        close(OFH);
        unlink($tmpfile);
    }

    # Sort the results for the benchmark compare.
    my $sortfile = "$outfile.sorted";
    my @cmd = ("sort", $outfile);
    print $log "Going to run [" . join(" ", @cmd) . "]\n";
    IPC::Run::run(\@cmd, '>', "$sortfile");

    return $sortfile;
}



##############################################################################
# Count the number of stores in a Pig Latin script, so we know how many files
# we need to compare.
#
sub countStores($$)
{
    my ($self, $testCmd) = @_;

    # Special work around for queries with more than one store that are not
    # actually multiqueries.
    if (defined $testCmd->{'notmq'}) {
        return 1;
    }

    my $count;

    # hope they don't have more than store per line
    # also note that this won't work if you comment out a store
    my @q = split(/\n/, $testCmd->{'pig'});
        for (my $i = 0; $i < @q; $i++) {
            $count += $q[$i] =~ /store\s+[a-zA-Z][a-zA-Z0-9_]*\s+into/i;
    }

    return $count;
}

1;
