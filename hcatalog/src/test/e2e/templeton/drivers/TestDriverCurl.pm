############################################################################           
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
                                                                                       
package TestDriverCurl;

###########################################################################
# Class: TestDriver
# A base class for TestDrivers.
# 

use TestDriverFactory;
use TestReport;
use File::Path;
use IPC::Run;
use Data::Dump qw(dump);
use JSON;
use HTTP::Daemon;
use HTTP::Status;
use Data::Compare;
use strict;
use English;
use Storable qw(dclone);
use File::Glob ':glob';
use JSON::Path;

my $passedStr = 'passed';
my $failedStr = 'failed';
my $abortedStr = 'aborted';
my $skippedStr = 'skipped';
my $dependStr = 'failed_dependency';


##############################################################################
#  Sub: printResults
#  Static function, can be used by test_harness.pl
#  Print the results so far, given the testStatuses hash.
#
# Paramaters:
# testStatuses - reference to hash of test status results.
# log    - reference to file handle to print results to.
# prefix - A title to prefix to the results
#
# Returns:
# None.
#
sub printResults
  {
    my ($testStatuses, $log, $prefix) = @_;

    my ($pass, $fail, $abort, $depend, $skipped) = (0, 0, 0, 0, 0);

    foreach (keys(%$testStatuses)) {
      ($testStatuses->{$_} eq $passedStr) && $pass++;
      ($testStatuses->{$_} eq $failedStr) && $fail++;
      ($testStatuses->{$_} eq $abortedStr) && $abort++;
      ($testStatuses->{$_} eq $dependStr) && $depend++;
      ($testStatuses->{$_} eq $skippedStr) && $skipped++;
    }

    my $msg = "$prefix, PASSED: $pass FAILED: $fail SKIPPED: $skipped ABORTED: $abort "
      . "FAILED DEPENDENCY: $depend";
    print $log "$msg\n";
    print "$msg\n";
         
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
      if ( $key =~ /^$groupName/ ) {
        ($testStatuses->{$key} eq $passedStr) && $pass++;
        ($testStatuses->{$key} eq $failedStr) && $fail++;
        ($testStatuses->{$key} eq $abortedStr) && $abort++;
        ($testStatuses->{$key} eq $dependStr) && $depend++;
      }
    }

    my $total= $pass + $fail + $abort;
    $report->totals( $groupName, $total, $fail, $abort, $totalDuration );

  }

##############################################################################
#  Sub: new
# Constructor, should only be called by TestDriverFactory.
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

    $self->{'wrong_execution_mode'} = "_xyz_wrong_execution_mode_zyx_";

    return $self;
  }

##############################################################################
#  Sub: globalSetup
# Set up before any tests are run.  This gives each individual driver a chance to do
# setup.  This function will only be called once, before all the tests are
# run.  A driver need not implement it.  It is a virtual function.
#
# Paramaters:
# globalHash - Top level hash from config file (does not have any group
# or test information in it).
# log - log file handle
#
# Returns:
# None
#
sub globalSetup
  {
    my ($self, $globalHash, $log) = @_;
    my $subName = (caller(0))[3];


    # Setup the output path
    my $me = `whoami`;
    chomp $me;
    #usernames on windows can be "domain\username" change the "\"
    # as runid is used in file names
    $me =~ s/\\/_/;
    $globalHash->{'runid'} = $me . "." . time;

    # if "-ignore false" was provided on the command line,
    # it means do run tests even when marked as 'ignore'
    if (defined($globalHash->{'ignore'}) && $globalHash->{'ignore'} eq 'false') {
      $self->{'ignore'} = 'false';
    }

    if (! defined $globalHash->{'localpathbase'}) {
      $globalHash->{'localpathbase'} = '/tmp';
    }

    $globalHash->{'outpath'} = $globalHash->{'outpathbase'} . "/" . $globalHash->{'runid'} . "/";
    $globalHash->{'localpath'} = $globalHash->{'localpathbase'} . "/" . $globalHash->{'runid'} . "/";
    $globalHash->{'webhdfs_url'} = $ENV{'WEBHDFS_URL'};
    $globalHash->{'templeton_url'} = $ENV{'TEMPLETON_URL'};
    $globalHash->{'current_user'} = $ENV{'USER_NAME'};
    $globalHash->{'DOAS_USER'} = $ENV{'DOAS_USER'};
    $globalHash->{'current_group_user'} = $ENV{'GROUP_USER_NAME'};
    $globalHash->{'current_other_user'} = $ENV{'OTHER_USER_NAME'};
    $globalHash->{'current_group'} = $ENV{'GROUP_NAME'};
    $globalHash->{'keytab_dir'} = $ENV{'KEYTAB_DIR'};



    $globalHash->{'inpdir_local'} = $ENV{'TH_INPDIR_LOCAL'};
    $globalHash->{'inpdir_hdfs'} = $ENV{'TH_INPDIR_HDFS'};
    $globalHash->{'db_connection_string'} = $ENV{'DB_CONNECTION_STRING'};
    $globalHash->{'db_user_name'} = $ENV{'DB_USER_NAME'};
    $globalHash->{'db_password'} = $ENV{'DB_PASSWORD'};

    $globalHash->{'is_secure_mode'} = $ENV{'SECURE_MODE'};
    $globalHash->{'user_realm'} = $ENV{'USER_REALM'};

    # add libexec location to the path
    if (defined($ENV{'PATH'})) {
      $ENV{'PATH'} = $globalHash->{'scriptPath'} . ":" . $ENV{'PATH'};
    } else {
      $ENV{'PATH'} = $globalHash->{'scriptPath'};
    }

    IPC::Run::run(['mkdir', '-p', $globalHash->{'localpath'}], \undef, $log, $log) or 
        die "Cannot create localpath directory " . $globalHash->{'localpath'} .
          " " . "$ERRNO\n";

    # Create the temporary directory
    IPC::Run::run(['mkdir', '-p', $globalHash->{'tmpPath'}], \undef, $log, $log) or 
        die "Cannot create temporary directory " . $globalHash->{'tmpPath'} .
          " " . "$ERRNO\n";

    my $testCmdBasics = $self->copyTestBasicConfig($globalHash);
    $testCmdBasics->{'method'} = 'PUT';
    $testCmdBasics->{'url'} = ':WEBHDFS_URL:/webhdfs/v1' . $globalHash->{'outpath'} . '?op=MKDIRS&permission=777';
    if (!defined $globalHash->{'is_secure_mode'} || $globalHash->{'is_secure_mode'} !~ /y.*/i) {
        $testCmdBasics->{'url'} = $testCmdBasics->{'url'} . '&user.name=' . $globalHash->{'current_user'};
    }
    my $curl_result = $self->execCurlCmd($testCmdBasics, "", $log);
    my $json = new JSON;
    $json->utf8->decode($curl_result->{'body'})->{'boolean'} or
        die "Cannot create hdfs directory " . $globalHash->{'outpath'} .
          " " . "$ERRNO\n";
  }

###############################################################################
# Sub: globalCleanup
# Clean up after all tests have run.  This gives each individual driver a chance to do
# cleanup.  This function will only be called once, after all the tests are
# run.  A driver need not implement it.  It is a virtual function.
#
# Paramaters:
# globalHash - Top level hash from config file (does not have any group
# or test information in it).
# log - log file handle
#
# Returns:
# None
sub globalCleanup
  {
    my ($self, $globalHash, $log) = @_;

    IPC::Run::run(['rm', '-rf', $globalHash->{'tmpPath'}], \undef, $log, $log) or 
        warn "Cannot remove temporary directory " . $globalHash->{'tmpPath'} .
          " " . "$ERRNO\n";

  }

###############################################################################
# Sub: runTest
# Run a test.  This is a pure virtual function.
#
# Parameters:
# testcmd - reference to hash with meta tags and command to run tests.
# Interpretation of the tags and the command are up to the subclass.
# log - reference to a stream pointer for the logs
#
# Returns:
# @returns reference to hash.  Contents of hash are defined by the subclass.
#
sub runTest
  {
    my ($self, $testCmd, $log) = @_;
    my $subName  = (caller(0))[3];

    # Check that we should run this test.  If the current hadoop version
    # is hadoop 2 and the test is marked as "ignore23", skip the test
    if ($self->wrongExecutionMode($testCmd, $log)) {
        my %result;
        return \%result;
    }
    # Handle the various methods of running used in 
    # the original TestDrivers

    if ( $testCmd->{'url'} ) {
      return $self->runCurlCmd( $testCmd, $log);
    } else {
      die "$subName FATAL Did not find a testCmd that " .
        "I know how to handle : " . $testCmd->{'Curl'};
    }


  }

###############################################################################

sub replaceParameters
  {
    my ($self, $testCmd, $aPfix, $log) = @_;

    my $url =  $testCmd->{$aPfix . 'url'};
    $url =~ s/:WEBHDFS_URL:/$testCmd->{'webhdfs_url'}/g;
    $url =~ s/:TEMPLETON_URL:/$testCmd->{'templeton_url'}/g;
    $url = $self->replaceParametersInArg($url, $testCmd, $log);
    $testCmd->{$aPfix . 'url'} = $url;

    $testCmd->{$aPfix . 'upload_file'} = 
      $self->replaceParametersInArg($testCmd->{$aPfix . 'upload_file'}, $testCmd, $log);

    $testCmd->{$aPfix . 'user_name'} = 
      $self->replaceParametersInArg($testCmd->{$aPfix . 'user_name'}, $testCmd, $log);

    if (defined $testCmd->{$aPfix . 'post_options'}) {
      my @options = @{$testCmd->{$aPfix . 'post_options'}};
      my @new_options = ();
      foreach my $option (@options) {
        $option = $self->replaceParametersInArg($option, $testCmd, $log);
        if (isWindows()) {
          my $equal_pos = index($option, '=');
          if ($equal_pos != -1) {
            my $left = substr($option, 0, $equal_pos);
            my $right = substr($option, $equal_pos+1);
            if ($right =~ /=/) {
              $right = '"'.$right.'"';
              $option = $left . "=" . $right;
            }
          }
        }
        push @new_options, ($option);
      }
      $testCmd->{$aPfix . 'post_options'} = \@new_options;
    }    
    if (defined $testCmd->{$aPfix . 'json_field_substr_match'}) {
      my $json_matches = $testCmd->{$aPfix . 'json_field_substr_match'};
      my @keys = keys %{$json_matches};

      foreach my $key (@keys) {
        my $new_value = $self->replaceParametersInArg($json_matches->{$key}, $testCmd, $log);
        $json_matches->{$key} = $new_value;
      }
    }    

    if (defined $testCmd->{$aPfix . 'json_path'}) {
      my $json_path_matches = $testCmd->{$aPfix . 'json_path'};
      my @keys = keys %{$json_path_matches};

      foreach my $key (@keys) {
        my $new_value = $self->replaceParametersInArg($json_path_matches->{$key}, $testCmd, $log);
        $json_path_matches->{$key} = $new_value;
      }
    }

  }

###############################################################################
sub replaceParametersInArg
  {
    my ($self, $arg, $testCmd, $log) = @_;
    if(! defined $arg){
      return $arg;
    }
    my $outdir = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'};
    $arg =~ s/:UNAME:/$testCmd->{'current_user'}/g;
    $arg =~ s/:DOAS:/$testCmd->{'DOAS_USER'}/g;
    $arg =~ s/:UNAME_GROUP:/$testCmd->{'current_group_user'}/g;
    $arg =~ s/:UNAME_OTHER:/$testCmd->{'current_other_user'}/g;
    $arg =~ s/:UGROUP:/$testCmd->{'current_group'}/g;
    $arg =~ s/:OUTDIR:/$outdir/g;
    $arg =~ s/:INPDIR_HDFS:/$testCmd->{'inpdir_hdfs'}/g;
    $arg =~ s/:INPDIR_LOCAL:/$testCmd->{'inpdir_local'}/g;
    $arg =~ s/:DB_CONNECTION_STRING:/$testCmd->{'db_connection_string'}/g;
    $arg =~ s/:DB_USER_NAME:/$testCmd->{'db_user_name'}/g;
    $arg =~ s/:DB_PASSWORD:/$testCmd->{'db_password'}/g;
    $arg =~ s/:TNUM:/$testCmd->{'num'}/g;
    return $arg;
  }



###############################################################################

sub getBaseCurlCmd(){
  my ($self) = @_; 
  my @curl_cmd = ("curl", '--silent','--show-error', '-H','Expect:');
  if (defined $ENV{'SOCKS_PROXY'}) {
    push @curl_cmd, ('--socks5-hostname', $ENV{'SOCKS_PROXY'});
  }
  return @curl_cmd;

}

###############################################################################
sub runCurlCmd(){
  my ($self, $testCmd, $log) = @_;
  if (defined $testCmd->{'upload_file'}) {
    return $self->upload_file($testCmd,$log);
  } else {
    #if there are setup steps, run them first
    if (defined $testCmd->{'setup'}) {
      my $i = 0;
      foreach my $setupCmd (@{$testCmd->{'setup'}}){
        $i++;
        print $log "\nRUNNING SETUP COMMAND: $i\n";
        my $pfix = "setup_${i}_";
        my $setupTestCmd = $self->createSetupCmd($testCmd, $setupCmd, $pfix, $log);
        my $setupResult = $self->execCurlCmd($setupTestCmd, $pfix, $log);
        
        #if status code is set in setup, check if it matches results
        if(defined $setupTestCmd->{"${pfix}status_code"}){
          $self->checkResStatusCode($setupResult, $setupTestCmd->{"${pfix}status_code"}, $log);
        }
      }
    }
    return $self->execCurlCmd($testCmd, "", $log);
  }
}
###############################################################################
sub createSetupCmd(){
  my ($self, $testCmd, $setupCmd, $pfix, $log) = @_;
  my $newTestCmd = dclone ($testCmd);
  for my $key (keys %$setupCmd){
    $newTestCmd->{$pfix . $key} = $setupCmd->{$key};
  }
  return $newTestCmd;
}

###############################################################################
sub upload_file(){
  my ($self, $testCmd, $log) = @_;
  $testCmd->{'method'} = 'PUT';
  my $result = $self->execCurlCmd($testCmd, "", $log);
  my $checkRes = $self->checkResStatusCode($result, 100, $log);
  if ($checkRes == 0) {
    #fail
    return 0;
  }
  my $header = $result->{'header_fields'};

  #final url where the file should be stored
  my $location = $header->{'Location'};
  $testCmd->{'url'} = $location;
    
  $result = $self->execCurlCmd($testCmd, "", $log);
  return $result;
}

###############################################################################
sub execCurlCmd(){
  my ($self, $testCmd, $argPrefix, $log) = @_;
  my @curl_cmd = $self->getBaseCurlCmd();
  # Set up file locations
  my $subName = (caller(0))[3];

  my $filePrefix = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $argPrefix . $testCmd->{'num'}; 
  my $cmd_body =  $filePrefix . ".cmd_body";

  #results
  my $res_header = $filePrefix . ".res_header";
  my $res_body = $filePrefix . ".res_body";

  my $outdir = $filePrefix .  ".out";
  my $stdoutfile = "$outdir/stdout";
  my $stderrfile = "$outdir/stderr";

  mkpath( [ $outdir ] , 0, 0755) if ( ! -e outdir );
  if ( ! -e $outdir ) {
    print $log "$0.$subName FATAL could not mkdir $outdir\n";
    die "$0.$subName FATAL could not mkdir $outdir\n";
  }

  $self->replaceParameters($testCmd, $argPrefix, $log );

  my $method = $testCmd->{ $argPrefix . 'method'};

  my $url = $testCmd->{ $argPrefix . 'url'};

  #allow curl to make insecure ssl connections and transfers
  if ($url =~ /^https:/) {
    push @curl_cmd, '-k';
  }

  my @options = ();
  if (defined $testCmd->{$argPrefix . 'post_options'}) {
    @options = @{$testCmd->{$argPrefix . 'post_options'}};
  }

  #handle authentication based on secure mode
  my $user_name = $testCmd->{ $argPrefix . 'user_name' }; 
  if (defined $testCmd->{'is_secure_mode'} &&  $testCmd->{'is_secure_mode'} =~ /y.*/i) {
    push @curl_cmd, ('--negotiate', '-u', ':');

    #if keytab dir is defined, look for a keytab file for user and do a kinit
    if(defined  $testCmd->{'keytab_dir'} && defined $user_name){
      $user_name =~ /(.*?)(\/|$)/;
      my $just_uname = $1; #uname without principal
      my $keytab_dir = $testCmd->{'keytab_dir'};
      print $log "regex " .  "${keytab_dir}/*${just_uname}\.*keytab";
      my @files = bsd_glob(  "${keytab_dir}/*${just_uname}\.*keytab" );
      if(scalar @files == 0){
        die "Could not find keytab file for user $user_name in $keytab_dir";
      } elsif(scalar @files > 1){
        die "More than one keytab file found for user $user_name in $keytab_dir";
      }
      my @cmd = ();
      if (defined $testCmd->{'user_realm'}){
          my $user_name_with_realm_name = $user_name.'@'.$testCmd->{'user_realm'};
          @cmd = ('kinit', '-k', '-t', $files[0], $user_name_with_realm_name);
      }
      else{
          @cmd = ('kinit', '-k', '-t', $files[0], $user_name);
      }
      print $log "Command  @cmd";
      IPC::Run::run(\@cmd, \undef, $log, $log) or 
          die "Could not kinit as $user_name using " .  $files[0] . " $ERRNO";
    }

  } else { 
    #if mode is unsecure
    if (defined $user_name) {
      my $user_param = "user.name=${user_name}";
      if ($method eq 'POST' ) {
        push @options, $user_param;
      } else {
        if ($url =~ /\?/) {
          #has some parameters in url
          $url = $url . '&' . $user_param;
        } else {
          $url = $url . '?' . $user_param;
        }
      }
    }

  }
  
  if (defined $testCmd->{'format_header'}) {
    push @curl_cmd, ('-H', $testCmd->{'format_header'});
  }

  
  
  
  if (defined $testCmd->{$argPrefix . 'format_header'}) {
    push @curl_cmd, ('-H', $testCmd->{$argPrefix . 'format_header'});
  }

  if (defined $testCmd->{$argPrefix . 'upload_file'}) {
    push @curl_cmd, ('-T', $testCmd->{$argPrefix . 'upload_file'});
  }

  #    if(!defined $testCmd->{'post_options'}){
  #	$testCmd->{'post_options'} = \();
  #    }

  if (defined $testCmd->{$argPrefix . 'check_call_back'}) {
    my $d = HTTP::Daemon->new || die;
    $testCmd->{'http_daemon'} = $d;
    $testCmd->{'callback_url'} = $d->url . 'templeton/$jobId';
    push @curl_cmd, ('-d', 'callback=' . $testCmd->{'callback_url'});
    push @{$testCmd->{$argPrefix . 'post_options'}}, ('callback=' . $testCmd->{'callback_url'});
    #	#my @options = @{$testCmd->{'post_options'}};
    #	print $log "post options  @options\n";
  }

  foreach my $option (@options) {
    push @curl_cmd, ('-d', $option);
  }

  push @curl_cmd, ("-X", $method, "-o", $res_body, "-D", $res_header);  
  push @curl_cmd, ($url);

  print $log "$0:$subName Going to run command : " .  join (' , ', @curl_cmd);
  print $log "\n";


  my %result;
  my $out;
  my $err;
  IPC::Run::run(\@curl_cmd, \undef, $out, $err) 
      or die "Failed running curl cmd " . join ' ', @curl_cmd;

  $result{'rc'} = $? >> 8;
  $result{'stderr'} = $err;
  $result{'stdout'} = $out;
  $result{'body'} = `cat $res_body`;
 
  my @full_header = `cat $res_header`;
  $result{'full_header'} = join '\n', @full_header;

  #find the final http status code
  for my $line ( @full_header){
    if($line =~ /.*(HTTP\/1.1)\s+(\S+)/){
      $result{'status_code'}  = $2;
    }
  }

  my %header_field;
  foreach my $line (@full_header) {
    chomp $line;
    $line =~ /(.*?)\s*:\s*(.*)/;
    if (defined $1 && defined $2 ) {
      $header_field{$1} = $2;
    }
  }
  $result{'header_fields'} = \%header_field;

  print $log "result : " . dump(%result);
  #dump(%result);
    
  return \%result;

}


###############################################################################
# Sub: generateBenchmark
# Generate benchmark results.  This is a pure virtual function.
#
# Parameters:
# benchmarkcmd - reference to hash with meta tags and command to
# generate benchmark.  Interpretation of the tags and the command are up to
# log - reference to a stream pointer for the logs
# the subclass.
#
# Returns:
# @returns reference to hash.  Contents of hash are defined by the subclass.
#
sub generateBenchmark
  {
    my %result;
    return \%result;
  }

###############################################################################
# Sub: compare
# Compare the results of the test run with the generated benchmark results.  
# This is a pure virtual function.
#
# Parameters:
# benchmarkcmd - reference to hash with meta tags and command to
# testResult - reference to hash returned by runTest.
# benchmarkResult - reference to hash returned by generateBenchmark.
# log - reference to a stream pointer for the logs
# testHash - reference to hash with meta tags and commands
#
# Returns:
# @returns reference true if results are the same, false otherwise.  "the
# same" is defined by the subclass.
#
sub compare
  {

    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];

    # Check that we should run this test.  If the current hadoop version
    # is hadoop 2 and the test is marked as "ignore23", skip the test
    if ($self->wrongExecutionMode($testCmd, $log)) {
        # Special magic value
        return $self->{'wrong_execution_mode'};
    }

    my $result = 1;             # until proven wrong...
    if (defined $testCmd->{'status_code'}) {
      my $res = $self->checkResStatusCode($testResult, $testCmd->{'status_code'}, $log);
      if ($res == 0) {
        $result = 0;
      }
    }

    my $json_hash;
    my %json_info;
    # for information on JSONPath, check http://goessner.net/articles/JsonPath/
    if (defined $testCmd->{'json_path'}) {
      my $json_matches = $testCmd->{'json_path'};
      foreach my $key (keys %$json_matches) {
        my $regex_expected_value = $json_matches->{$key};
        my $path = JSON::Path->new($key);

        # decode $testResult->{'body'} to an array of hash
        my $body = decode_json $testResult->{'body'};
        my @filtered_body;
        if (defined $testCmd->{'filter_job_names'}) {
          foreach my $filter (@{$testCmd->{'filter_job_names'}}) {
            my @filtered_body_tmp = grep { $_->{detail}{profile}{jobName} eq $filter } @$body;
            @filtered_body = (@filtered_body, @filtered_body_tmp);
          }
        } else {
          @filtered_body = @$body;
        }
        my @sorted_filtered_body;
        if (ref @$body[0] eq 'HASH') {
          @sorted_filtered_body = sort { $a->{id} cmp $b->{id} } @filtered_body;
        } else {
          @sorted_filtered_body = sort { $a cmp $b } @filtered_body;
        }
        my $value = $path->value(\@sorted_filtered_body);
        if (JSON::is_bool($value)) {
          $value = $value ? 'true' : 'false';
        }
        
        if ($value !~ /$regex_expected_value/s) {
          print $log "$0::$subName INFO check failed:"
            . " json pattern check failed. For field "
              . "$key, regex <" . $regex_expected_value
                . "> did not match the result <" . $value
                  . ">\n";
          $result = 0;
          last;
        }
      }
    } 
    if (defined $testCmd->{'json_field_substr_match'} || $testCmd->{'json_field_match_object'}) {
      my $json = new JSON;
      $json_hash = $json->utf8->decode($testResult->{'body'});
      my $json_matches = $testCmd->{'json_field_substr_match'};
      my $json_matches_object = $testCmd->{'json_field_match_object'};

      %json_info = %$json_hash;
      if (defined $json_info{'info'}) {
        %json_info = %{$json_info{'info'}};
        
      }
      print $log "\n\n json_info";
      print $log dump(%json_info);
      print $log "\n\n";

      if (defined $json_hash->{'id'}) {
        print STDERR "jobid " . $json_hash->{'id'} . "\n";        
        $json_info{'id'} = $json_hash->{'id'};
      }

      if(defined $json_matches->{'location_perms'} || defined $json_matches->{'location_group'}){
        $self->setLocationPermGroup(\%json_info, $testCmd, $log);
      }

      foreach my $key (keys %$json_matches) {
        my $json_field_val = $json_info{$key};
        if( (ref($json_field_val) && ! UNIVERSAL::isa($json_field_val,'SCALAR')) ||
            (!ref($json_field_val) && ! UNIVERSAL::isa(\$json_field_val,'SCALAR')) ){
          #flatten the object into a string
          $json_field_val = dump($json_field_val);
        }
        if (JSON::is_bool($json_field_val)) {
          $json_field_val = $json_field_val ? 'true' : 'false';
        }
        my $regex_expected_value = $json_matches->{$key};
        print $log "Comparing $key: $json_field_val with regex /$regex_expected_value/\n";

        if ($json_field_val !~ /$regex_expected_value/s) {
          print $log "$0::$subName WARN check failed:" 
            . " json pattern check failed. For field "
              . "$key, regex <" . $regex_expected_value 
                . "> did not match the result <" . $json_field_val
                  . ">\n";
          $result = 0;
        }
      }

      foreach my $key (keys %$json_matches_object) {
        my $json_field_val = $json_info{$key};
        my $regex_expected_obj = $json->utf8->decode($json_matches_object->{$key});
        print $log "Comparing $key: " . dump($json_field_val) . ",expected value:  " . dump($regex_expected_obj);

        if (!Compare($json_field_val, $regex_expected_obj)) {
          print $log "$0::$subName WARN check failed:" 
            . " json compare failed. For field "
              . "$key, regex <" . dump($regex_expected_obj)
                . "> did not match the result <" . dump($json_field_val)
                  . ">\n";
          $result = 0;
        }
      }


    }
    
    #kill it if there is a request to kill
    if($testCmd->{'kill_job_timeout'}){
      sleep $testCmd->{'kill_job_timeout'};
      my $jobid = $json_hash->{'id'};
      if (!defined $jobid) {
        print $log "$0::$subName WARN check failed: " 
          . "no jobid (id field)found in result";
        $result = 0;
      } else {
        $self->killJob($testCmd, $jobid, $log);
      }
    }


    #try to get the call back url request until timeout
    if ($result == 1 && defined $testCmd->{'check_call_back'}) {

      my $timeout = 300; #wait for 5 mins for callback
      if(defined $testCmd->{'timeout'}){
        $timeout = $testCmd->{'timeout'};
      }

      my $d = $testCmd->{'http_daemon'};
      $d->timeout($timeout);
      my $url_requested;
      $testCmd->{'callback_url'} =~ s/\$jobId/$json_hash->{'id'}/g;
      print $log "Expanded callback url : <" . $testCmd->{'callback_url'} . ">\n";
      do{
        print $log "Waiting for call back url request\n";
        if (my $c = $d->accept) {
          if (my $r = $c->get_request) {
            my $durl = $d->url;
            chop $durl;
            $url_requested = $durl . $r->uri->path ;
            print $log "Got request at url <" .  $url_requested  . ">\n";
            $c->send_status_line(200);
            $c->close;
          }
          undef($c);
        } else {
          print $log "Timeout on wait on call back url"  . "\n";
          $result = 0;
        }
      }while (defined $url_requested && $url_requested  ne $testCmd->{'callback_url'});
      $d->close;
      if (!defined $url_requested || $url_requested  ne $testCmd->{'callback_url'}) {
        print $log "failed to recieve request on call back url";
        $result = 0;
      }

    }

    if ( (defined $testCmd->{'check_job_created'})
         || (defined $testCmd->{'check_job_complete'})
         || (defined $testCmd->{'check_job_exit_value'})
         || (defined $testCmd->{'check_job_percent_complete'}) ) {    
      my $jobid = $json_hash->{'id'};
      if (!defined $jobid) {
        print $log "$0::$subName WARN check failed: " 
          . "no jobid (id field)found in result";
        $result = 0;
      } else {
        my $jobResult = $self->getJobResult($testCmd, $jobid, $log);
        my $json = new JSON;
        my $res_hash = $json->utf8->decode($jobResult->{'body'});
        if (! defined $res_hash->{'status'}) {
          print $log "$0::$subName WARN check failed: " 
            . "jobresult not defined ";
          $result = 0;
        }
        if (defined($testCmd->{'check_job_complete'}) || defined($testCmd->{'check_job_exit_value'})
            || defined($testCmd->{'check_job_percent_complete'})) {
          my $jobComplete;
          my $NUM_RETRIES = 60;
          my $SLEEP_BETWEEN_RETRIES = 5;

          #first wait for job completion
          while ($NUM_RETRIES-- > 0) {
            $jobComplete = $res_hash->{'status'}->{'jobComplete'};
            if (defined $jobComplete && (lc($jobComplete) eq "true" || lc($jobComplete) eq "1")) {
              last;
            }
            sleep $SLEEP_BETWEEN_RETRIES;
            $jobResult = $self->getJobResult($testCmd, $jobid, $log);
            $json = new JSON;
            $res_hash = $json->utf8->decode($jobResult->{'body'});
          }
          if ( (!defined $jobComplete) || (lc($jobComplete) ne "true" && lc($jobComplete) ne "1")) {
            print $log "$0::$subName WARN check failed: " 
              . " timeout on wait for job completion ";
            $result = 0;
          } else { 
            # job has completed, check the runState value
            if (defined($testCmd->{'check_job_complete'})) {
              my $runState = $res_hash->{'status'}->{'runState'};
              my $runStateVal = $self->getRunStateNum($testCmd->{'check_job_complete'});
              if ( (!defined $runState) || $runState ne $runStateVal) {
                print $log "check_job_complete failed. got runState  $runState,  expected  $runStateVal";
                $result = 0;
              }
            }
            if (defined($testCmd->{'check_job_exit_value'})) {
              my $exitValue = $res_hash->{'exitValue'};
              my $expectedExitValue = $testCmd->{'check_job_exit_value'};
              if ( (!defined $exitValue) || $exitValue % 128 ne $expectedExitValue) {
                print $log "check_job_exit_value failed. got exitValue $exitValue,  expected  $expectedExitValue";
                $result = 0;
              }
            }
            # check the percentComplete value
            if (defined($testCmd->{'check_job_percent_complete'})) {
              my $pcValue = $res_hash->{'percentComplete'};
              my $expectedPercentComplete = $testCmd->{'check_job_percent_complete'};
              if ( (!defined $pcValue) || $pcValue !~ m/$expectedPercentComplete/ ) {
                print $log "check_job_percent_complete failed. got percentComplete $pcValue,  expected  $expectedPercentComplete";
                $result = 0;
              }
            }
          }

	  #Check userargs
	  print $log "$0::$subName INFO Checking userargs";
          my @options = @{$testCmd->{'post_options'}};
          if( !defined $res_hash->{'userargs'}){
            print $log "$0::$subName INFO expected userargs" 
                . " but userargs not defined\n";
            $result = 0;
          }

	  #create exp_userargs hash from @options
          my %exp_userargs = ();
          foreach my $opt ( @options ){
            print $log "opt $opt";
            my ($key, $val) = split q:=:, $opt, 2;   
            if(defined $exp_userargs{$key}){

              #if we have already seen this value
              #then make the value an array and push new value in
              if(ref($exp_userargs{$key}) eq ""){
                my @ar = ($exp_userargs{$key});
                $exp_userargs{$key} = \@ar;
              }
              my $ar = $exp_userargs{$key}; 
              push @$ar, ($val); 
            }
            else{
              $exp_userargs{$key} = $val;	
            }
          }

          my %r_userargs = %{$res_hash->{'userargs'}};
          foreach my $key( keys %exp_userargs){
            if($key eq 'inputreader'){
              next;
            }
            if( !defined $r_userargs{$key}){
              print $log "$0::$subName INFO $key not found in userargs \n";
              $result = 0;
              next;
            }
              
            print $log "$0::$subName DEBUG comparing expected " 
                . " $key ->" . dump($exp_userargs{$key})
                . " With result $key ->" . dump($r_userargs{$key}) . "\n";

            if (!Compare($exp_userargs{$key}, $r_userargs{$key})) {
              print $log "$0::$subName WARN check failed:" 
                  . " json compare failed. For field "
                  . "$key, regex <" . dump($r_userargs{$key})
                  . "> did not match the result <" . dump($exp_userargs{$key})
                  . ">\n";
              $result = 0;
            }
          }
		  if ($result != 0 && $testCmd->{'check_logs'}) {
            my $testCmdBasics = $self->copyTestBasicConfig($testCmd);
            $testCmdBasics->{'method'} = 'GET';
            $testCmdBasics->{'url'} = ':WEBHDFS_URL:/webhdfs/v1:OUTDIR:' . '/status/logs?op=LISTSTATUS';
            my $curl_result = $self->execCurlCmd($testCmdBasics, "", $log);
            my $path = JSON::Path->new("FileStatuses.FileStatus[*].pathSuffix");
            my @value = $path->values($curl_result->{'body'});
            if ($testCmd->{'check_logs'}->{'job_num'} && $testCmd->{'check_logs'}->{'job_num'} ne (scalar @value)-1) {
              print $log "$0::$subName INFO check failed: "
                . " Expect " . $testCmd->{'check_logs'}->{'job_num'} . " jobs in logs, but get " . scalar @value;
              $result = 0;
              return $result;
            }
            foreach my $jobid (@value) {
              if ($jobid eq 'list.txt') {
                next;
              }
              my $testCmdBasics = $self->copyTestBasicConfig($testCmd);
              $testCmdBasics->{'method'} = 'GET';
              $testCmdBasics->{'url'} = ':WEBHDFS_URL:/webhdfs/v1:OUTDIR:' . '/status/logs/' . $jobid . '?op=LISTSTATUS';
              my $curl_result = $self->execCurlCmd($testCmdBasics, "", $log);

              my $path = JSON::Path->new("FileStatuses.FileStatus[*]");
              my @value = $path->values($curl_result->{'body'});

              my $foundjobconf = 0;
              foreach my $elem (@value) {
                if ($elem->{'pathSuffix'} eq "job.xml.html") {
                  $foundjobconf = 1;
                  if ($elem->{'length'} eq "0") {
                    print $log "$0::$subName INFO check failed: "
                      . " job.xml.html for " . $jobid . " is empty";
					$result = 0;
					return $result;
                  }
                  next;
                }
                my $attempt = $elem->{'pathSuffix'};
                my $testCmdBasics = $self->copyTestBasicConfig($testCmd);
                $testCmdBasics->{'method'} = 'GET';
                $testCmdBasics->{'url'} = ':WEBHDFS_URL:/webhdfs/v1:OUTDIR:' . '/status/logs/' . $jobid . '/' . $attempt . '?op=LISTSTATUS';
                my $curl_result = $self->execCurlCmd($testCmdBasics, "", $log);
                my $path = JSON::Path->new("FileStatuses.FileStatus[*].pathSuffix");
                my @value = $path->values($curl_result->{'body'});
                my @files = ('stderr', 'stdout', 'syslog');
                foreach my $file (@files) {
                  if ( !grep( /$file/, @value ) ) {
                    print $log "$0::$subName INFO check failed: "
                      . " Cannot find " . $file . " in logs/" . $attempt;
                    $result = 0;
                    return $result;
                  }
                }
                $path = JSON::Path->new("FileStatuses.FileStatus[*].length");
                @value = $path->values($curl_result->{'body'});
                my $foundnonzerofile = 0;
                foreach my $length (@value) {
                  if ($length ne "0") {
                    $foundnonzerofile = 1;
                  }
                }
                if (!$foundnonzerofile) {
                  print $log "$0::$subName INFO check failed: "
                    . " All files in logs/" . $attempt . " are empty";
                  $result = 0;
                  return $result;
                }
              }
              if (!$foundjobconf) {
                print $log "$0::$subName INFO check failed: "
                  . " Cannot find job.xml.html for " . $jobid;
				$result = 0;
				return $result;
              }
            }
          }
        }
      }
    }
    return $result;
  }

##############################################################################
# Check whether we should be running this test or not.
#
sub wrongExecutionMode($$)
{
    my ($self, $testCmd, $log) = @_;

    my $wrong = 0;

    if (defined $testCmd->{'ignore23'} && $testCmd->{'hadoopversion'}=='23') {
        $wrong = 1;
    }

    if ($wrong) {
        print $log "Skipping test $testCmd->{'group'}" . "_" .
            $testCmd->{'num'} . " since it is not suppsed to be run in hadoop 23\n";
    }

    return  $wrong;
}

###############################################################################
sub  setLocationPermGroup{
  my ($self, $job_info, $testCmd, $log) = @_;
  my $location = $job_info->{'location'};
  $location =~ /hdfs.*:\d+(\/.*)\/(.*)/;  
  my $dir = $1;
  my $file = $2;

  my $testCmdBasics = $self->copyTestBasicConfig($testCmd);
  $testCmdBasics->{'method'} = 'GET';
  $testCmdBasics->{'num'} = $testCmdBasics->{'num'} . "_checkFile";
  $testCmdBasics->{'url'} = ':WEBHDFS_URL:/webhdfs/v1' 
    . $dir . '?op=LISTSTATUS';


  my $result =  $self->execCurlCmd($testCmdBasics, "", $log);

  my $json = new JSON;
  my $json_hash = $json->utf8->decode($result->{'body'});
  my @filestatuses = @{$json_hash->{'FileStatuses'}->{'FileStatus'}};
  foreach my $filestatus (@filestatuses){
    if($filestatus->{'pathSuffix'} eq $file){
      $job_info->{'location_perms'} =  numPermToStringPerm($filestatus->{'permission'});
      $job_info->{'location_group'} = $filestatus->{'group'};
      $job_info->{'location_owner'} = $filestatus->{'owner'};
      last;
    }

  }

}

###############################################################################
#convert decimal string to binary string
sub dec2bin {
    my $decimal = shift;
    my $binary = unpack("B32", pack("N", $decimal));
    $binary =~ s/^0+(?=\d)//;   # remove leading zeros                                                                                                                                                                                                                                                   
    return $binary;
}

###############################################################################
#convert single digit of the numeric permission format to string format
sub digitPermToStringPerm{
    my $numPerm = shift;
    my $binaryPerm = dec2bin($numPerm);
    my $stringPerm = "";
    if($binaryPerm =~ /1\d\d$/){
        $stringPerm .= "r";
      }else{
        $stringPerm .= "-";
    }

    if($binaryPerm =~ /\d1\d$/){
        $stringPerm .= "w";
      }else{
        $stringPerm .= "-";
    }

    if($binaryPerm =~ /\d\d1$/){
        $stringPerm .= "x";
      }else{
        $stringPerm .= "-";
    }

}

###############################################################################
# convert numeric permission format to string format
sub numPermToStringPerm{
    my $numPerm = shift;
    $numPerm =~ /(\d)(\d)(\d)$/;
    return digitPermToStringPerm($1)
        . digitPermToStringPerm($2)
        . digitPermToStringPerm($3);

}

###############################################################################
sub getRunStateNum{
  my ($self, $job_complete_state) = @_;
  if (lc($job_complete_state) eq 'success') {
    return 2;
  } elsif (lc($job_complete_state) eq 'failure') {
    return 3;
  } elsif (lc($job_complete_state) eq 'killed') {
    return 5;
  }

}


###############################################################################
sub getJobResult{
  my ($self, $testCmd, $jobid, $log) = @_;
  my $testCmdBasics = $self->copyTestBasicConfig($testCmd);
  $testCmdBasics->{'method'} = 'GET';
  $testCmdBasics->{'num'} = $testCmdBasics->{'num'} . "_jobStatusCheck";
  $testCmdBasics->{'url'} = ':TEMPLETON_URL:/templeton/v1/jobs/'
    . $jobid . '?' . "user.name=:UNAME:" ;
  return $self->execCurlCmd($testCmdBasics, "", $log);
}
###############################################################################
sub killJob{
  my ($self, $testCmd, $jobid, $log) = @_;
  my $testCmdBasics = $self->copyTestBasicConfig($testCmd);
  $testCmdBasics->{'method'} = 'DELETE';
  $testCmdBasics->{'num'} = $testCmdBasics->{'num'} . "_killJob";
  $testCmdBasics->{'url'} = ':TEMPLETON_URL:/templeton/v1/jobs/'
    . $jobid . '?' . "user.name=:UNAME:" ;
  return $self->execCurlCmd($testCmdBasics, "", $log);
}
###############################################################################
#Copy test config essential for running a sub command
sub copyTestBasicConfig{
  my ($self, $testCmd) = @_;
  my %testCmdBasics;
  foreach my $key (keys %$testCmd) {
    if ($key ne 'method'
        && $key ne 'url'
        && $key ne 'upload_file'
        && $key ne 'post_options'
       ) {
      $testCmdBasics{$key} = $testCmd->{$key};
    }
  }
  #   $testCmdBasics{'localpath'} = $testCmd->{'localpath'};
  #   $testCmdBasics{'group'} = $testCmd->{'group'};
  #   $testCmdBasics{'num'} = $testCmd->{'num'};
  return \%testCmdBasics;
}
###############################################################################
sub checkResStatusCode{
  my ($self, $testResult, $e_status_code, $log) = @_;
  my $subName  = (caller(0))[3];

  #    print STDERR "expected " . $e_status_code . " was " . $testResult->{'status_code'};

  if (!defined $testResult->{'status_code'} || 
      $testResult->{'status_code'} != $e_status_code) {
    print $log "$0::$subName INFO Check failed: status_code " .
      "$e_status_code expected, test returned " .
        "<$testResult->{'status_code'}>\n";
    return 0;
  }
  return 1;

}

###############################################################################
# Sub: recordResults
# Record results of the test run.  The required fields are filled in by the
# test harness.  This call gives an individual driver a chance to fill in
# additional fields of cmd, cmd_id, expected_results, and actual_results.
# this function does not have to be implemened.
# This is a virtual function.
#
# Parameters:
# status - status of test passing, true or false
# testResult - reference to hash returned by runTest.
# benchmarkResult - reference to hash returned by generateBenchmark.
# dbinfo - reference to hash that will be used to fill in db.
# log - reference to hash that will be used to fill in db.
#
# Returns:
# None
#
sub recordResults
  {
  }

###############################################################################
# Sub: cleanup
# Clean up after a test.  This gives each individual driver a chance to do
# cleanup.  A driver need not implement it.  It is a virtual function.
#
# Parameters:
# status - status of test passing, true or false
# testHash - reference to hash that was passed to runTest() and
# generateBenchmark().
# testResult - reference to hash returned by runTest.
# benchmarkResult - reference to hash returned by generateBenchmark.
# log - reference to a stream pointer for the logs
# 
# Returns:
# None
#
sub cleanup
  {
  }

###############################################################################
# Sub: run
# Run all the tests in the configuration file.
#
# Parameters:
# testsToRun - reference to array of test groups and ids to run
# testsToMatch - reference to array of test groups and ids to match.
# If a test group_num matches any of these regular expressions it will be run.
# cfg - reference to contents of cfg file
# log - reference to a stream pointer for the logs
# dbh - reference database connection
# testStatuses- reference to hash of test statuses
# confFile - config file name
# startat - test to start at.
# logname - name of the xml log for reporting results
#
# Returns:
# @returns nothing
# failed.
#
sub run
  {
    my ($self, $testsToRun, $testsToMatch, $cfg, $log, $dbh, $testStatuses,
        $confFile, $startat, $logname ) = @_;

    my $subName      = (caller(0))[3];
    my $msg="";
    my $duration=0;
    my $totalDuration=0;
    my $groupDuration=0;
    my $sawstart = !(defined $startat);
    # Rather than make each driver handle our multi-level cfg, we'll flatten
    # the hashes into one for it.
    my %globalHash;

    my $runAll = ((scalar(@$testsToRun) == 0) && (scalar(@$testsToMatch) == 0));

    # Read the global keys
    foreach (keys(%$cfg)) {
      next if $_ eq 'groups';
      $globalHash{$_} = $cfg->{$_};
    }

    $globalHash{$_} = $cfg->{$_};
    # Do the global setup
    $self->globalSetup(\%globalHash, $log);

    my $report=0;
    #        my $properties= new Properties(0, $globalHash{'propertiesFile'});

    my %groupExecuted;
    foreach my $group (@{$cfg->{'groups'}}) {
 
      print $log "INFO $subName at ".__LINE__.": Running TEST GROUP(".$group->{'name'}.")\n";
                
      my %groupHash = %globalHash;
      $groupHash{'group'} = $group->{'name'};

      # Read the group keys
      foreach (keys(%$group)) {
        next if $_ eq 'tests';
        $groupHash{$_} = $group->{$_};
      }


      # Run each test
      foreach my $test (@{$group->{'tests'}}) {
        # Check if we're supposed to run this one or not.
        if (!$runAll) {
          # check if we are supposed to run this test or not.
          my $foundIt = 0;
          foreach (@$testsToRun) {
            if (/^$groupHash{'group'}(_[0-9]+)?$/) {
              if (not defined $1) {
                # In this case it's just the group name, so we'll
                # run every test in the group
                $foundIt = 1;
                last;
              } else {
                # maybe, it at least matches the group
                my $num = "_" . $test->{'num'};
                if ($num eq $1) {
                  $foundIt = 1;
                  last;
                }
              }
            }
          }
          foreach (@$testsToMatch) {
            my $protoName = $groupHash{'group'} . "_" .  $test->{'num'};
            if ($protoName =~ /$_/) {
              if (not defined $1) {
                # In this case it's just the group name, so we'll
                # run every test in the group
                $foundIt = 1;
                last;
              } else {
                # maybe, it at least matches the group
                my $num = "_" . $test->{'num'};
                if ($num eq $1) {
                  $foundIt = 1;
                  last;
                }
              }
            }
          }

          next unless $foundIt;
        }

        # This is a test, so run it.
        my %testHash = %groupHash;
        foreach (keys(%$test)) {
          $testHash{$_} = $test->{$_};
        }

        my $testName = $testHash{'group'} . "_" . $testHash{'num'};

        #            if ( $groupExecuted{ $group->{'name'} }== 0 ){
        #                $groupExecuted{ $group->{'name'} }=1;
        #               
        #                my $xmlDir= $globalHash{'localxmlpathbase'}."/run".$globalHash->{'UID'};
        #                mkpath( [ $xmlDir ] , 1, 0777) if ( ! -e $xmlDir );
        #
        #                my $filename = $group->{'name'}.".xml";
        #                $report = new TestReport ( $properties, "$xmlDir/$filename" );
        #                $report->purge();
        #            }

        # Check that ignore isn't set for this file, group, or test
        if (defined $testHash{'ignore'}) {
          print $log "Ignoring test $testName, ignore message: " .
            $testHash{'ignore'} . "\n";
          next;
        }

        # Have we not reached the starting point yet?
        if (!$sawstart) {
          if ($testName eq $startat) {
            $sawstart = 1;
          } else {
            next;
          }
        }

        # Check that this test doesn't depend on an earlier test or tests
        # that failed.  Don't abort if that test wasn't run, just assume the
        # user knew what they were doing and set it up right.
        my $skipThisOne = 0;
        foreach (keys(%testHash)) {
          if (/^depends_on/ && defined($testStatuses->{$testHash{$_}}) &&
              $testStatuses->{$testHash{$_}} ne $passedStr) {
            print $log "Skipping test $testName, it depended on " .
              "$testHash{$_} which returned a status of " .
                "$testStatuses->{$testHash{$_}}\n";
            $testStatuses->{$testName} = $dependStr;
            $skipThisOne = 1;
            last;
          }
        }
        if ($skipThisOne) {
          printResults($testStatuses, $log, "Results so far");
          next;
        }

        print $log "\n******************************************************\n";
        print $log "\nTEST: $confFile::$testName\n";
        print $log  "******************************************************\n";
        print $log "Beginning test $testName at " . time . "\n";
        my %dbinfo = (
                      'testrun_id' => $testHash{'trid'},
                      'test_type' => $testHash{'driver'},
                      #'test_file' => $testHash{'file'},
                      'test_file' => $confFile,
                      'test_group' => $testHash{'group'},
                      'test_num' => $testHash{'num'},
                     );
        my $beginTime = time;
        my $endTime = 0;
        my ($testResult, $benchmarkResult);
        eval {
          $testResult = $self->runTest(\%testHash, $log);
          $endTime = time;
          $benchmarkResult = $self->generateBenchmark(\%testHash, $log);
          my $result =
            $self->compare($testResult, $benchmarkResult, $log, \%testHash);
          $msg = "INFO: $subName() at ".__LINE__.":Test $testName";

          if ($result eq $self->{'wrong_execution_mode'}) {
            $msg .= " SKIPPED";
            $testStatuses->{$testName} = $skippedStr;
          } elsif ($result) {
            $msg .= " SUCCEEDED";
            $testStatuses->{$testName} = $passedStr;

          } else {
            $msg .= " FAILED";
            $testStatuses->{$testName} = $failedStr;

          }
          $msg .= "\nEnding test $testName at " . $endTime ."\n";
          #print $msg;
          print $log $msg;
          $duration = $endTime - $beginTime;
          $dbinfo{'duration'} = $duration;
          $self->recordResults($result, $testResult
                               , $benchmarkResult, \%dbinfo, $log);
                                  
        };


        if ($@) {
          $msg= "ERROR $subName at : ".__LINE__." Failed to run test $testName <$@>\n";
          $msg .= "Ending test $testName at " . time ."\n";
          #print $msg;
          print $log $msg;
          $testStatuses->{$testName} = $abortedStr;
          $dbinfo{'duration'} = $duration;
        }


        eval {
          $dbinfo{'status'} = $testStatuses->{$testName};
          if ($dbh) {
            $dbh->insertTestCase(\%dbinfo);
          }
        };
        if ($@) {
          chomp $@;
          warn "Failed to insert test case info, error <$@>\n";
        }

        $self->cleanup($testStatuses->{$testName}, \%testHash, $testResult,
                       $benchmarkResult, $log);
        #$report->testcase( $group->{'name'}, $testName, $duration, $msg, $testStatuses->{$testName}, $testResult ) if ( $report );
        $report->testcase( $group->{'name'}, $testName, $duration, $msg, $testStatuses->{$testName} ) if ( $report );
        $groupDuration = $groupDuration + $duration;
        $totalDuration = $totalDuration + $duration;
        printResults( $testStatuses, $log, "Results so far" );
      }

      if ( $report ) {
        $report->systemOut( $logname, $group->{'name'});
        printGroupResultsXml( $report, $group->{'name'}, $testStatuses, $groupDuration );
      }
      $report = 0;
      $groupDuration=0;


    }

    # Do the global cleanup
    $self->globalCleanup(\%globalHash, $log);
  }

# TODO These should be removed

sub tmpIPCRun(){

  my $self = shift;
  my $subName      = (caller(0))[3];
  my $runningSubName= shift;
  my $refCmd  = shift;
  my @cmd     = @$refCmd;
  my $log    = shift;
  my $msg    = shift;

  print $log "$0::$subName INFO Running ( @cmd )\n";

  my $result= `@cmd`;
  if ( $@ ) {
    my $msg= "$0::$subName FATAL Failed to run from $runningSubName $msg < $@ >\n$result\n";
    print $log $msg;
    die "$msg";
  }

  return $?;
}

sub tmpIPCRunSplitStdoe {

  my $self = shift;
  my $subName      = (caller(0))[3];
  my $runningSubName= shift;
  my $refCmd  = shift;
  my @cmd     = @$refCmd;
  my $dir    = shift;
  my $log    = shift;
  my $msg    = shift;
  my $die    = shift;


  my $failed = 0;
  
  my $outfilename = $dir."out.tmp";
  my $errfilename = $dir."err.tmp";
  print $log "$0::$subName INFO Running from $runningSubName ( @cmd 1>$outfilename 2>$errfilename )\n";
  #make sure files are writeable
  open( TMP, ">$outfilename" ) || die "$0::$subName FATAL: Cannot open $outfilename for writing\n";
  close( TMP );
  open( TMP, ">$errfilename" ) || die "$0::$subName FATAL: Cannot open $errfilename for writing\n";
  close( TMP );

  #RUN CMD
  my $msg;
  print $log `@cmd 1>$outfilename 2>$errfilename`;
   
  my $failed=0;
  if ( $@ ) { 
    $msg= "$0::$subName FATAL < $@ >\n"; 
    $failed++;
  }
   
  #READ FILES
  my $stdout=""; 
  my $stderr="";;
  open( TMP, "$outfilename" ) || die "$0::$subName FATAL: Cannot open $outfilename for reading\n";
  while ( <TMP> ) {
    $stdout .= $_;
  }
  close( TMP );
 
  open( TMP, "$errfilename" ) || die "$0::$subName FATAL: Cannot open $errfilename for reading\n";
  while ( <TMP> ) {
    $stderr .= $_;
  }
  close( TMP );

  #DIE IF Test Failed, otherwise return stdout and stderr
  if ( $failed ) {

    $msg = "$0::$subName FATAL: Faied from $runningSubName \nSTDOUT:" . $stdout . "\nSTDERR:" . $stderr . "\n" if ( $failed );
    print $log "$msg";
    die $msg if ( $die != "1" ); #die by defaultast
    return ( -1, $stdout, $stderr );

  }

  return ( $?, $stdout, $stderr);
}

sub tmpIPCRunJoinStdoe {

  my $self = shift;
  my $subName      = (caller(0))[3];
  my $runningSubName= shift;
  my $refCmd  = shift;
  my @cmd     = @$refCmd;
  my $outfilename= shift;
  my $log    = shift;
  my $msg    = shift;
  my $die    = shift;

  #make sure files are writeable
  open( TMP, ">$outfilename" ) || die "$0::$subName FATAL: Cannot open $outfilename for writing\n";
  close( TMP );

  #RUN CMD
  my $msg;
  my $failed=0;
  print $log "$0::$subName INFO Running ( @cmd 2>&1$outfilename 2>/dev/null )\n";
  print $log `@cmd 2>&1 > $outfilename 2>/dev/null`;
  if ( $@ ) { 
    $failed++;
    $msg= "$0::$subName FATAL < $@ >\n"; 
  }
   
  #READ FILES
  my $stdoe=""; 
  open( TMP, "$outfilename" ) || die "$0::$subName FATAL: Cannot open $outfilename for reading\n";
  while ( <TMP> ) {
    $stdoe .= $_;
  }
  close( TMP );

  if ( $failed ) {
    print $log "$msg";
    die $msg if ( $die != "1" ); #die by default
    return ( -1 ); 
  }
  return ( $? );
}

sub isWindows
{
    if($^O =~ /mswin/i) {
        return 1;
    }
    else {
        return 0;
    }
}
1;
