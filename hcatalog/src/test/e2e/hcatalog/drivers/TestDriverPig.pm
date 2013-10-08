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

package TestDriverPig;

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

    my @cmd = (Util::getPigCmd($globalHash, $log), '-e', 'mkdir', $globalHash->{'outpath'});


    print $log "Going to run " . join(" ", @cmd) . "\n";
    IPC::Run::run(\@cmd, \undef, $log, $log) or die "Cannot create HDFS directory " . $globalHash->{'outpath'} . ": $? - $!\n";

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

    # Create the HDFS temporary directory
    @cmd = (Util::getPigCmd($globalHash, $log), '-e', 'mkdir', "tmp/$globalHash->{'runid'}");
	print $log "Going to run " . join(" ", @cmd) . "\n";
    IPC::Run::run(\@cmd, \undef, $log, $log) or die "Cannot create HDFS directory " . $globalHash->{'outpath'} . ": $? - $!\n";
}

sub globalCleanup
{
    # noop there because the removal of temp directories, which are created in #globalSetupConditional(), is to be
    # performed in method #globalCleanupConditional().
}

sub globalCleanupConditional
{
    my ($self, $globalHash, $log) = @_;

    IPC::Run::run(['rm', '-rf', $globalHash->{'tmpPath'}], \undef, $log, $log) or 
        warn "Cannot remove temporary directory " . $globalHash->{'tmpPath'} .
        " " . "$ERRNO\n";

    # Cleanup the HDFS temporary directory
    my @cmd = (Util::getPigCmd($globalHash, $log), '-e', 'fs', '-rmr', "tmp/$globalHash->{'runid'}");
	print $log "Going to run " . join(" ", @cmd) . "\n";
    IPC::Run::run(\@cmd, \undef, $log, $log) or die "Cannot create HDFS directory " . $globalHash->{'outpath'} . ": $? - $!\n";
}


sub runTest
{
    my ($self, $testCmd, $log) = @_;
    my $subName  = (caller(0))[3];

    # Check that we should run this test.  If the current execution type
    # doesn't match the execonly flag, then skip this one.
    if ($self->wrongExecutionMode($testCmd)) {
        print $log "Skipping test $testCmd->{'group'}" . "_" .
            $testCmd->{'num'} . " since it is executed only in " .
            $testCmd->{'execonly'} . " mode and we are executing in " .
            $testCmd->{'exectype'} . " mode.\n";
        my %result;
        return \%result;
    }

    if ( $testCmd->{'hcat_prep'} ) {
        Util::prepareHCat($self, $testCmd, $log);
    }
    # Handle the various methods of running used in 
    # the original TestDrivers

    if ( $testCmd->{'pig'} && $self->hasCommandLineVerifications( $testCmd, $log) ) {
       return $self->runPigCmdLine( $testCmd, $log, 1);
    } elsif( $testCmd->{'pig'} ){
       # If the results are written to a table run the command and then 
       # run a another Pig script to dump the results of the table.
       my $result;
       if (defined($testCmd->{'result_table'})) {
           $result = $self->runPig( $testCmd, $log, 0, 1);
           my @results = ();
           my @outputs = ();
           if (ref($testCmd->{'result_table'}) ne 'ARRAY') {
               $results[0] = $testCmd->{'result_table'};
           } else {
               @results = @{$testCmd->{'result_table'}};
           }
           my $stores = $self->countStores($testCmd);

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
                   $tableLoader = (defined $testCmd->{'result_table_loader'} ? $testCmd->{'result_table_loader'} : "org.apache.hive.hcatalog.pig.HCatLoader()");
                   $modifiedTestCmd{'pig'} = "a = load '$tableName' using $tableLoader; store a into ':OUTPATH:';";
                   my $r = $self->runPig(\%modifiedTestCmd, $log, 1, 1);
	           $outputs[$i] = $r->{'output'};
               } else {
                   $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out/$id";
                   my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

                   # Copy result file out of hadoop
                   my @baseCmd = Util::getPigCmd($testCmd, $log);
                   my $testOut = $self->postProcessSingleOutputFile($outfile, $localdir, \@baseCmd, $testCmd, $log);
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
           $result = $self->runPig( $testCmd, $log, 1, 1);
       }
       return $result;
    } elsif(  $testCmd->{'script'} ){
       return $self->runScript( $testCmd, $log );
    } else {
       die "$subName FATAL Did not find a testCmd that I know how to handle";
    }
}

sub runPigCmdLine
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;

    # Set up file locations
    my $pigfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".pig";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $outdir  = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $stdoutfile = "$outdir/stdout";
    my $stderrfile = "$outdir/stderr";

    mkpath( [ $outdir ] , 0, 0755) if ( ! -e outdir );
    if ( ! -e $outdir ){
       print $log "$0.$subName FATAL could not mkdir $outdir\n";
       die "$0.$subName FATAL could not mkdir $outdir\n";
    }

    # Write the pig script to a file.
    my $pigcmd = Util::replaceParameters( $testCmd->{'pig'}, $outfile, $testCmd, $log );

    open(FH, "> $pigfile") or die "Unable to open file $pigfile to write pig script, $ERRNO\n";
    print FH $pigcmd . "\n";
    close(FH);

    # Build the command
    my @baseCmd = Util::getPigCmd($testCmd, $log);
    my @cmd = @baseCmd;

    # Add pig parameters if they're provided
    if (defined($testCmd->{'pig_params'})) {
        # Processing :PARAMPATH: in parameters
        foreach my $param (@{$testCmd->{'pig_params'}}) {
            $param =~ s/:PARAMPATH:/$testCmd->{'paramPath'}/g;
        }
        push(@cmd, @{$testCmd->{'pig_params'}});
    }

    # Add pig file and redirections 
    push(@cmd, $pigfile);
    my $command= join (" ", @cmd) . " 1> $stdoutfile 2> $stderrfile";

    # Run the command
    print $log "$0:$subName Going to run command: ($command)\n";
    print $log "$0:$subName STD OUT IS IN FILE ($stdoutfile)\n";
    print $log "$0:$subName STD ERROR IS IN FILE ($stderrfile)\n";
    print $log "$0:$subName PIG SCRIPT CONTAINS ($pigfile):  \n$pigcmd\n";

    my @result=`$command`;
    $result{'rc'} = $? >> 8;
    $result{'output'} = $outfile;
    $result{'stdout'} = `cat $stdoutfile`;
    $result{'stderr'} = `cat $stderrfile`;
    $result{'stderr_file'} = $stderrfile;

    print $log "STD ERROR CONTAINS:\n$result{'stderr'}\n";

    return \%result;
}


sub runScript
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;

    # Set up file locations
    my $script = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".sh";
    my $outdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $outfile = "$outdir/script.out";
    my $stdoutfile = "$outdir/script.out";
    my $stderrfile = "$outdir/script.err";

    mkpath( [ $outdir ] , 0, 0755) if ( ! -e outdir );
    if ( ! -e $outdir ){
       print $log "$0.$subName FATAL could not mkdir $outdir\n";
       die "$0.$subName FATAL could not mkdir $outdir\n";
    }

    # Write the script to a file
    my $cmd = Util::replaceParameters( $testCmd->{'script'}, $outfile, $testCmd, $log );

    open(FH, ">$script") or die "Unable to open file $script to write script, $ERRNO\n";
    print FH $cmd . "\n";
    close(FH);

    my @result=`chmod +x $script`;

    # Build the command
    my $command= "$script 1> $stdoutfile 2> $stderrfile";

    # Run the script
    print $log "$0:$subName Going to run command: ($command)\n";
    print $log "$0:$subName STD OUT IS IN FILE ($stdoutfile)\n";
    print $log "$0:$subName STD ERROR IS IN FILE ($stderrfile)\n";
    print $log "$0:$subName SCRIPT CONTAINS ($script):  \n$cmd\n";

    @result=`$command`;
    $result{'rc'} = $? >> 8;
    $result{'output'} = $outfile;
    $result{'stdout'} = `cat $stdoutfile`;
    $result{'stderr'} = `cat $stderrfile`;
    $result{'stderr_file'} = $stderrfile;

    print $log "STD ERROR CONTAINS:\n$result{'stderr'}\n";

    return \%result;
}


sub runPig
{
    my ($self, $testCmd, $log, $copyResults, $noFailOnFail) = @_;
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
    print $log "$0::$className::$subName INFO: Going to run pig command: @cmd\n";
    print $log "With PIG_CLASSPATH set to $ENV{'PIG_CLASSPATH'}\n";
    print $log "and HADOOP_HOME set to $ENV{'HADOOP_HOME'}\n";

    my $runrc = IPC::Run::run(\@cmd, \undef, $log, $log);

    if (defined($noFailOnFail) && $noFailOnFail) {
    } else {
        die "Failed running $pigfile\n";
    }

    $result{'rc'} = $? >> 8;


    # Get results from the command locally
    my $localoutfile;
    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $stores = $self->countStores($testCmd);
       
    # single query
    if ($stores == 1) {
        if ($copyResults) {
            $result{'output'} = $self->postProcessSingleOutputFile($outfile, $localdir, \@baseCmd, $testCmd, $log);
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
              $testOut = $self->postProcessSingleOutputFile($localoutfile, $localdir, \@baseCmd, $testCmd, $log);
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

sub postProcessSingleOutputFile
{
    my ($self, $outfile, $localdir, $baseCmd, $testCmd, $log) = @_;
    my $subName  = (caller(0))[3];

    my @baseCmd = @{$baseCmd};
    my @copyCmd = @baseCmd;
    push(@copyCmd, ('-e', 'copyToLocal', $outfile, $localdir)); 
    print $log "$0::$className::$subName INFO: Going to run pig command: @copyCmd\n";
 
    IPC::Run::run(\@copyCmd, \undef, $log, $log) or die "Cannot copy results from HDFS $outfile to $localdir\n";


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
 
    if ($#SQLQuery == 0) {
        my $outfile = $self->generateSingleSQLBenchmark($testCmd, $SQLQuery[0], undef, $log);
        $result{'output'} = $outfile;
    } else {
        my @outfiles = ();
        for (my $id = 0; $id < ($#SQLQuery + 1); $id++) {
            my $sql = $SQLQuery[$id];
            my $outfile = $self->generateSingleSQLBenchmark($testCmd, $sql, ($id+1), $log); 
            push(@outfiles, $outfile);
        }
        $result{'outputs'} = \@outfiles;
    }

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

sub hasCommandLineVerifications
{
    my ($self, $testCmd, $log) = @_;

    foreach my $key ('rc', 'expected_out', 'expected_out_regex', 'expected_err', 'expected_err_regex', 
                     'not_expected_out', 'not_expected_out_regex', 'not_expected_err', 'not_expected_err_regex' ) {
      if (defined $testCmd->{$key}) {
         return 1;
      }
    }
    return 0;
}


sub compare
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];

    # Check that we should run this test.  If the current execution type
    # doesn't match the execonly flag, then skip this one.
    if ($self->wrongExecutionMode($testCmd)) {
        # Special magic value
        return $self->{'wrong_execution_mode'}; 
    }

    # For now, if the test has 
    # - testCmd pig, and 'sql' for benchmark, then use comparePig, i.e. using benchmark
    # - any verification directives formerly used by CmdLine or Script drivers (rc, regex on out and err...)
    #   then use compareScript even if testCmd is "pig"
    # - testCmd script, then use compareScript
    # - testCmd pig, and none of the above, then use comparePig
    #
    # Later, should add ability to have same tests both verify with the 'script' directives, 
    # and do a benchmark compare, if it was a pig cmd. E.g. 'rc' could still be checked when 
    # doing the benchmark compare.

    if ( $testCmd->{'script'} || $self->hasCommandLineVerifications( $testCmd, $log) ){
       return $self->compareScript ( $testResult, $log, $testCmd);
    } elsif( $testCmd->{'pig'} ){
       return $self->comparePig ( $testResult, $benchmarkResult, $log, $testCmd);
    } else {
       # Should have been caught by runTest, still...
       print $log "$0.$subName WARNING Did not find a testCmd that I know how to handle\n";
       return 0;
    } 
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
      if ($testResult->{'stderr'} !~ $testCmd->{'expected_err_regex'}) {
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


sub comparePig
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];

    my $result;
    my $stores = $self->countStores($testCmd);
    
    if ($stores == 1) {
        $result = $self->compareSingleOutput($testResult, $testResult->{'output'},
                $benchmarkResult->{'output'}, $log);
    } else {
        my $res = 0;
        for (my $id = 0; $id < ($stores); $id++) {
            my $testOutput = ($testResult->{'outputs'})->[$id];
            my $benchmarkOutput = ($benchmarkResult->{'outputs'})->[$id];
            $res += $self->compareSingleOutput($testResult, $testOutput,
                                               $benchmarkOutput, $log);
            $result = ($res == ($stores)) ? 1 : 0;
        }
    }

    return $result;
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

##############################################################################
# Check whether we should be running this test or not.
#
sub wrongExecutionMode($$)
{
    my ($self, $testCmd) = @_;

    # Check that we should run this test.  If the current execution type
    # doesn't match the execonly flag, then skip this one.
    return (defined $testCmd->{'execonly'} &&
            $testCmd->{'execonly'} ne $testCmd->{'exectype'});
}

##############################################################################
#  Sub: printGroupResultsXml
#  Print the results for the group using junit xml schema using values from the testStatuses hash.
#
# Paramaters:
# $report       - the report object to use to generate the report
# $groupName    - the name of the group to report totals for
# $testStatuses - the hash containing the results for the tests run so far
# $totalDuration- The total time it took to run the group of tests
#
# Returns:
# None.
#
sub printGroupResultsXml
{
        my ( $report, $groupName, $testStatuses,  $totalDuration) = @_;
        $totalDuration=0 if  ( !$totalDuration );

        my ($pass, $fail, $abort, $depend) = (0, 0, 0, 0);

        foreach my $key (keys(%$testStatuses)) {
              if ( $key =~ /^$groupName/ ){
                ($testStatuses->{$key} eq $passedStr) && $pass++;
                ($testStatuses->{$key} eq $failedStr) && $fail++;
                ($testStatuses->{$key} eq $abortedStr) && $abort++;
                ($testStatuses->{$key} eq $dependStr) && $depend++;
               }
        }

        my $total= $pass + $fail + $abort;
        $report->totals( $groupName, $total, $fail, $abort, $totalDuration );

}

1;
