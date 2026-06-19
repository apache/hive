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

package TestDriverHadoop;

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

sub globalSetupConditional() {
    my ($self, $globalHash, $log) = @_;

    # add libexec location to the path
    if (defined($ENV{'PATH'})) {
        $ENV{'PATH'} = $globalHash->{'scriptPath'} . ":" . $ENV{'PATH'};
    }
    else {
        $ENV{'PATH'} = $globalHash->{'scriptPath'};
    }

    Util::runHadoopCmd($globalHash, $log, "fs -mkdir $globalHash->{'outpath'}");

    IPC::Run::run(['mkdir', '-p', $globalHash->{'localpath'}], \undef, $log, $log) or
        die "Cannot create localpath directory " . $globalHash->{'localpath'} .
        " " . "$ERRNO\n";

    IPC::Run::run(['mkdir', '-p', $globalHash->{'benchmarkPath'}], \undef, $log, $log) or
        die "Cannot create benchmark directory " .  $globalHash->{'benchmarkPath'} .
        " " . "$ERRNO\n";

    # Create the temporary directory
    IPC::Run::run(['mkdir', '-p', $globalHash->{'tmpPath'}], \undef, $log, $log) or
        die "Cannot create temporary directory " . $globalHash->{'tmpPath'} .
        " " . "$ERRNO\n";

    Util::runHadoopCmd($globalHash, $log, "fs -mkdir tmp/$globalHash->{'runid'}");
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

    if ( $testCmd->{'hcat_prep'} ) {
        Util::prepareHCat($self, $testCmd, $log);
    }

    if ( $testCmd->{'hadoop'} ) {
       my $result;
       if (defined($testCmd->{'result_table'})) {
           $result = $self->runHadoop( $testCmd, $log );
           my @results = ();
           my @outputs = ();
           if (ref($testCmd->{'result_table'}) ne 'ARRAY') {
               $results[0] = $testCmd->{'result_table'};
           } else {
               @results = @{$testCmd->{'result_table'}};
           }

           my $id = 0; # regular ouput count
           for (my $i = 0; $i < @results; $i++) {
               if ($results[$i] ne '?') {
	           my %modifiedTestCmd = %{$testCmd};
	           $pigfiles[$i] = $testCmd->{'localpath'} .
	               $testCmd->{'group'} . "_" .  $testCmd->{'num'} .
	               ".dumptable.$i.pig";
	           $outfiles[$i] = $testCmd->{'thisResultsPath'} . "/" .
	               $testCmd->{'group'} .  "_" .  $testCmd->{'num'} . ".$i.out";
                   $tableName = $results[$i];
	           $modifiedTestCmd{'num'} = $testCmd->{'num'} . "_" . $i . "_benchmark";
                   $modifiedTestCmd{'pig'} = "a = load '$tableName' using org.apache.hive.hcatalog.pig.HCatLoader(); store a into ':OUTPATH:';";
                   my $r = $self->runPig(\%modifiedTestCmd, $log, 1);
	           $outputs[$i] = $r->{'output'};
               } else {
                   $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out/$id";
                   my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

                   # Copy result file out of hadoop
                   my @baseCmd = Util::getPigCmd($testCmd, $log);
                   my $testOut = $self->postProcessSingleOutputFile($outfile, $localdir, $testCmd, $log);
                   $outputs[$i] = $testOut;
                   $id++;
               }
           }
           $result->{'outputs'}=\@outputs;
           if ($self->countStores($testCmd)==1) {
               $result->{'output'}=$outputs[0];
           }
       }
       else {
           $result = $self->runHadoop( $testCmd, $log );
       }
       return $result;
    } else {
       die "$subName FATAL Did not find a testCmd that I know how to handle";
    }
}

sub dumpPigTable
{
    my ($self, $testCmd, $table, $log, $id) = @_;
    my $subName  = (caller(0))[3];

    my %result;

    # Write the pig script to a file.
    my $pigfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . $id . ".dump.pig";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'}  . $id . "dump.out";

    open(FH, "> $pigfile") or die "Unable to open file $pigfile to write pig script, $ERRNO\n";
    print FH "a = load '$table' using org.apache.hive.hcatalog.pig.HCatLoader(); store a into '$outfile';\n";
    close(FH);


    # Build the command
    my @baseCmd = Util::getPigCmd($testCmd, $log);
    my @cmd = @baseCmd;

    push(@cmd, $pigfile);


    # Run the command
    print $log "$0::$className::$subName INFO: Going to run pig command: @cmd\n";

    IPC::Run::run(\@cmd, \undef, $log, $log) or die "Failed running $pigfile\n";
    $result{'rc'} = $? >> 8;


    # Get results from the command locally
    my $localoutfile;
    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . $id . ".dump.out";
       
    $outfile = $self->postProcessSingleOutputFile($outfile, $localdir, $testCmd, $log);
    return $outfile;
}

sub postProcessSingleOutputFile
{
    my ($self, $outfile, $localdir, $testCmd, $log) = @_;
    my $subName  = (caller(0))[3];

    Util::runHadoopCmd($globalHash, $log, "fs -copyToLocal $outfile $localdir");
 
    # Sort the result if necessary.  Keep the original output in one large file.
    # Use system not IPC run so that the '*' gets interpolated by the shell.
    
    # Build command to:
    # 1. Combine part files
    my $fppCmd = "cat $localdir/map* $localdir/part* 2>/dev/null";
    
    # 2. Standardize float precision
    if (defined $testCmd->{'floatpostprocess'} &&
            defined $testCmd->{'delimiter'}) {
        $fppCmd .= " | $toolpath/floatpostprocessor.pl '" .
            $testCmd->{'delimiter'} . "'";
    }
    
    $fppCmd .= " > $localdir/out_original";
    
    # run command
    print $log "$fppCmd\n";
    system($fppCmd);

    # Sort the results for the benchmark compare.
    my @sortCmd = ('sort', "$localdir/out_original");
    print $log join(" ", @sortCmd) . "\n";
    IPC::Run::run(\@sortCmd, '>', "$localdir/out_sorted");

    return "$localdir/out_sorted";
}

sub runHadoop
{
    my ($self, $testCmd, $log) = @_;
    my $subName  = (caller(0))[3];

    my %result;

    # Write the hadoop command to a file.
    my $hadoopfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".hadoop";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    # Get all of the additional jars we'll need.
    my $additionalJars = Util::getHBaseLibs($testCmd, $log); #hbase before hive for precedence over bundled hbase
    $additionalJars .= Util::getHCatLibs($testCmd, $log);
    $additionalJars .= Util::getHiveLibs($testCmd, $log);
    $testCmd->{'libjars'} = $additionalJars;
    $testCmd->{'libjars'} =~ s/:/,/g;
    my $hadoopcmd = Util::replaceParameters( $testCmd->{'hadoop'}, $outfile, $testCmd, $log );

    # adjust for the leading and trailing new line often seen in the conf file's command directives
    $hadoopcmd =~ s/^\s*(.*?)\s*$/\1/s;

    open(FH, "> $hadoopfile") or die "Unable to open file $hadoopfile to write hadoop command file, $ERRNO\n";
    print FH $hadoopcmd . "\n";
    close(FH);


    # Build the command
    my @cmd = Util::getHadoopCmd($testCmd);

    # Add command line arguments if they're provided
    if (defined($testCmd->{'hadoop_cmdline_args'})) {
        push(@cmd, @{$testCmd->{'hadoop_cmdline_args'}});
    }

    # Add the test command elements
    push(@cmd, split(/ +/,$hadoopcmd));

    # Set HADOOP_CLASSPATH environment variable if provided
    my $cp = $testCmd->{'hcatalog.jar'}; 
    $cp =~ s/,/:/g;
    # Add in the hcat config file
    $cp .= ":" . $testCmd->{'hiveconf'};
    $cp .= ":" . $additionalJars;
    $ENV{'HADOOP_CLASSPATH'} = $cp;

    if (defined($testCmd->{'hbaseconf'})) {
        $ENV{'HADOOP_CLASSPATH'} = "$ENV{'HADOOP_CLASSPATH'}:$testCmd->{'hbaseconf'}";
    }

    # Add su user if provided
    if (defined($testCmd->{'run_as'})) {
      my $cmd = '"' . join (" ", @cmd) . '"';
      @cmd = ("echo", $cmd, "|", "su", $testCmd->{'run_as'});
    }

    my $script = $hadoopfile . ".sh";
    open(FH, ">$script") or die "Unable to open file $script to write script, $ERRNO\n";
    print FH join (" ", @cmd) . "\n";
    close(FH);

    my @result=`chmod +x $script`;

    # Run the command
    print $log "$0::$className::$subName INFO: Going to run hadoop command in shell script: $script\n";
    print $log "$0::$className::$subName INFO: Going to run hadoop command: " . join(" ", @cmd) . "\n";
    print $log "With HADOOP_CLASSPATH set to " . $ENV{'HADOOP_CLASSPATH'} . " and HADOOP_OPTS set to " . $ENV{'HADOOP_OPTS'} . "\n";

    my @runhadoop = ("$script");
    IPC::Run::run(\@runhadoop, \undef, $log, $log) or
        die "Failed running $script\n";

    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . $id . ".dump.out";
    my @baseCmd = Util::getPigCmd($testCmd, $log);
    if ($self->countStores($testCmd)==1) {
        @outputs = ();
        $outputs[0] = $self->postProcessSingleOutputFile($outfile, $localdir, $testCmd, $log);
        $result{'outputs'} = \@outputs;
    }

    return \%result;
} # end sub runHadoop


sub compare
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];

    my $result;
    
    if (defined($testResult->{'outputs'})) {
        my $res = 0;
        my @outputs = $testResult->{'outputs'};
        my $count = @outputs;
        for (my $id = 0; $id < $count; $id++) {
            my $testOutput = ($testResult->{'outputs'})->[$id];
            my $benchmarkOutput = ($benchmarkResult->{'outputs'})->[$id];
            $res += $self->compareSingleOutput($testResult, $testOutput,
                                               $benchmarkOutput, $log);
            $result = ($res == ($count)) ? 1 : 0;
        }
    } else {
        $result = $self->compareSingleOutput($testResult, $testResult->{'output'},
                $benchmarkResult->{'output'}, $log);
    }

    return $result;
}

sub generateBenchmark
{
    my ($self, $testCmd, $log) = @_;

    my %result;

    my @SQLQuery = @{$testCmd->{'sql'}};
    my @SQLQuery = ();
        if (ref($testCmd->{'sql'}) ne 'ARRAY') {
            $SQLQuery[0] = $testCmd->{'sql'};
        } else {
            @SQLQuery = @{$testCmd->{'sql'}};
        }
 
    my @outfiles = ();
    for (my $id = 0; $id < ($#SQLQuery + 1); $id++) {
        my $sql = $SQLQuery[$id];
        my $outfile = $self->generateSingleSQLBenchmark($testCmd, $sql, ($id+1), $log); 
        push(@outfiles, $outfile);
    }
    $result{'outputs'} = \@outfiles;

    return \%result;
}

sub generateSingleSQLBenchmark
{
    my ($self, $testCmd, $sql, $id, $log) = @_;

    my $qmd5 = substr(md5_hex($testCmd->{'pig'}), 0, 5);
    my $sqlfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".benchmark.$id.sql";
    my $outfile = $testCmd->{'benchmarkPath'} . "/" . $testCmd->{'group'} . "_" . $testCmd->{'num'};

    $outfile .= defined($id) ? ".$id" . ".out" :  ".out";
    
    my $outfp;
    open($outfp, "> $outfile") or
        die "Unable to open output file $outfile, $!\n";

    open(FH, "> $sqlfile") or
        die "Unable to open file $sqlfile to write SQL script, $ERRNO\n";
    print FH $sql;
    close(FH);

    Util::runDbCmd($testCmd, $log, $sqlfile, $outfp);
    
    $rcs[$i] =  $? >> 8;
    close($outfp);

    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    $outfile =
        $self->postProcessSingleSQLOutputFile($outfile, $testCmd, $log);

    return $outfile;
}

sub postProcessSingleSQLOutputFile
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

sub runPig
{
    my ($self, $testCmd, $log, $copyResults) = @_;
    my $subName  = (caller(0))[3];

    my %result;

    # Write the pig script to a file.
    my $pigfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".pig";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $pigcmd = Util::replaceParameters( $testCmd->{'pig'}, $outfile, $testCmd, $log );

    open(FH, "> $pigfile") or die "Unable to open file $pigfile to write pig script, $ERRNO\n";
    print FH $pigcmd . "\n";
    close(FH);


    # Build the command
    #my @baseCmd = $self->getPigCmd($testCmd, $log);
    my @baseCmd = Util::getPigCmd($testCmd, $log);
    my @cmd = @baseCmd;

    # Add option -l giving location for secondary logs
    my $locallog = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".log";
    push(@cmd, "-logfile");
    push(@cmd, $locallog);

    # Add pig parameters if they're provided
    if (defined($testCmd->{'pig_params'})) {
        # Processing :PARAMPATH: in parameters
        foreach my $param (@{$testCmd->{'pig_params'}}) {
            $param =~ s/:PARAMPATH:/$testCmd->{'paramPath'}/g;
        }
        push(@cmd, @{$testCmd->{'pig_params'}});
    }

    push(@cmd, $pigfile);


    # Run the command
    print $log "Setting PIG_CLASSPATH to $ENV{'PIG_CLASSPATH'}\n";
    print $log "$0::$className::$subName INFO: Going to run pig command: @cmd\n";

    IPC::Run::run(\@cmd, \undef, $log, $log) or
        die "Failed running $pigfile\n";
    $result{'rc'} = $? >> 8;


    # Get results from the command locally
    my $localoutfile;
    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $stores = $self->countStores($testCmd);
       
    # single query
    if ($stores == 1) {
        if ($copyResults) {
            $result{'output'} = $self->postProcessSingleOutputFile($outfile, $localdir, $testCmd, $log);
            $result{'originalOutput'} = "$localdir/out_original"; # populated by postProcessSingleOutputFile
        } else {
            $result{'output'} = "NO_COPY";
        }
    }
    # multi query
    else {
        my @outfiles = ();
        for (my $id = 1; $id <= ($stores); $id++) {
            $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out/$id";
            $localoutfile = $outfile . ".$id";

            # Copy result file out of hadoop
            my $testOut;
            if ($copyResults) {
              $testOut = $self->postProcessSingleOutputFile($localoutfile, $localdir, $testCmd, $log);
            } else {
              $testOut = "NO_COPY";
            }
            push(@outfiles, $testOut);
        }
        ##!!! originalOutputs not set! Needed?
        $result{'outputs'} = \@outfiles;
    }

    # Compare doesn't get the testCmd hash, so I need to stuff the necessary
    # info about sorting into the result.
    if (defined $testCmd->{'sortArgs'} && $testCmd->{'sortArgs'}) {
        $result{'sortArgs'} = $testCmd->{'sortArgs'};
    }

    return \%result;
}

sub compareSingleOutput
{
    my ($self, $testResult, $testOutput, $benchmarkOutput, $log) = @_;

print $log "testResult: $testResult testOutput: $testOutput benchmarkOutput: $benchmarkOutput\n";

    # cksum the the two files to see if they are the same
    my ($testChksm, $benchmarkChksm);
    IPC::Run::run((['cat', $testOutput], '|', ['cksum']), \$testChksm,
        $log) or die "$0: error: cannot run cksum on test results\n";
    IPC::Run::run((['cat', $benchmarkOutput], '|', ['cksum']),
        \$benchmarkChksm, $log) or die "$0: error: cannot run cksum on benchmark\n";

    chomp $testChksm;
    chomp $benchmarkChksm;
    print $log "test cksum: $testChksm\nbenchmark cksum: $benchmarkChksm\n";

    my $result;
    if ($testChksm ne $benchmarkChksm) {
        print $log "Test output checksum does not match benchmark checksum\n";
        print $log "Test checksum = <$testChksm>\n";
        print $log "Expected checksum = <$benchmarkChksm>\n";
        print $log "RESULTS DIFFER: vimdiff " . cwd . "/$testOutput " . cwd . "/$benchmarkOutput\n";
    } else {
        $result = 1;
    }

    # Now, check if the sort order is specified
    if (defined($testResult->{'sortArgs'})) {
        Util::setLocale();
	my @sortChk = ('sort', '-cs');
        push(@sortChk, @{$testResult->{'sortArgs'}});
        push(@sortChk, $testResult->{'originalOutput'});
        print $log "Going to run sort check command: " . join(" ", @sortChk) . "\n";
        IPC::Run::run(\@sortChk, \undef, $log, $log);
	my $sortrc = $?;
        if ($sortrc) {
            print $log "Sort check failed\n";
            $result = 0;
        }
    }

    return $result;
}

##############################################################################
# Count the number of stores in a Pig Latin script, so we know how many files
# we need to compare.
#
sub countStores($$)
{
    my ($self, $testCmd) = @_;

    if (defined $testCmd->{'pig'}) {
        my $count;

        # hope they don't have more than store per line
        # also note that this won't work if you comment out a store
        my @q = split(/\n/, $testCmd->{'pig'});
            for (my $i = 0; $i < @q; $i++) {
                $count += $q[$i] =~ /store\s+[a-zA-Z][a-zA-Z0-9_]*\s+into/i;
        }

        return $count;

    }
    else {
        #defined $testCmd->{'hadoop'}
        my $count;

        my @q = split(/\n/, $testCmd->{'hadoop'});
            for (my $i = 0; $i < @q; $i++) {
                $count += $q[$i] =~ /OUTPATH/ig;
        }

        return $count;

    }

}

1;
