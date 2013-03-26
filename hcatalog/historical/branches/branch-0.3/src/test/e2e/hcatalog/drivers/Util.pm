#!/usr/bin/env perl 

############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


###########################################################################
# Class: Util
#
# A collection of  helper subroutines.
#


package Util;

use IPC::Run qw(run);

sub prepareHCat
{
    my ($self, $testCmd, $log) = @_;
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $hcatCmd = $self->replaceParameters( $testCmd->{'hcat_prep'}, $outfile, $testCmd, $log);

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

##############################################################################
#  Sub: setupHiveProperties
#
#  Assure that necessary values are set in config in order to set Hive
#  Java properties.
#
#  Returns:
#  Nothing
sub  setupHiveProperties($$)
{
    my ($cfg, $log) = @_;

    # Set up values for the metastore
    if (defined($cfg->{'metastore_thrift'}) && $cfg->{'metastore_thrift'} == 1) {
        if (! defined $cfg->{'metastore_host'} || $cfg->{'metastore_host'} eq "") {
            print $log "When using thrift, you must set the key " .
                " 'metastore_host' to the machine your metastore is on\n";
            die "metastore_host is not set in existing.conf\n";
        }

        $cfg->{'metastore_connection'} =
            "jdbc:$cfg->{'metastore_db'}://$cfg->{'metastore_host'}/hivemetastoredb?createDatabaseIfNotExist=true";
   
        if (! defined $cfg->{'metastore_passwd'} || $cfg->{'metastore_passwd'} eq "") {
            $cfg->{'metastore_passwd'} = 'hive';
        }

        if (! defined $cfg->{'metastore_port'} || $cfg->{'metastore_port'} eq "") {
            $cfg->{'metastore_port'} = '9933';
        }

        $cfg->{'metastore_uri'} =
            "thrift://$cfg->{'metastore_host'}:$cfg->{'metastore_port'}";
    } else {
        $cfg->{'metastore_connection'} =
            "jdbc:derby:;databaseName=metastore_db;create=true";
        $cfg->{'metastore_driver'} = "org.apache.derby.jdbc.EmbeddedDriver";
    }
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

    push (@baseCmd, '--config', $properties->{'testconfigpath'}) if defined($properties->{'testconfigpath'});

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

    my @cmd = ("$cfg->{'hivehome'}/bin/hive");

    # Add all of the modified properties we want to set
    push(@cmd, "--hiveconf", "hive.metastore.uris=$cfg->{'thriftserver'}");
    push(@cmd, "--hiveconf", "hive.metastore.local=false");

    if( defined($cfg->{'metastore.principal'}) && ($cfg->{'metastore.principal'} =~ m/\S+/)
        &&  ($cfg->{'metastore.principal'} ne '${metastore.principal}')){
        push(@cmd, "--hiveconf", "hive.metastore.sasl.enabled=true",  "--hiveconf", "hive.metastore.kerberos.principal=$cfg->{'metastore.principal'}");
    } else {
        push(@cmd, "--hiveconf", "hive.metastore.sasl.enabled=false");
    }

    if (defined($cfg->{'additionaljarspath'})) {
        $ENV{'HIVE_AUX_JARS_PATH'} = $cfg->{'additionaljarspath'};
    }

    if (defined($cfg->{'hiveconf'})) {
        foreach my $hc (@{$cfg->{'hiveconf'}}) {
            push(@cmd, "--hiveconf", $hc);
        }
    }

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
        die "Cannot run hive when HADOOP_HOME environment variable is not set.";
    }

    $outfile = $log if (!defined($outfile));
    $errfile = $log if (!defined($errfile));

    # unset HADOOP_CLASSPATH
    $ENV{'HADOOP_CLASSPATH'} = "";
    $ENV{'HADOOP_CLASSPATH'} = $cfg->{'pigjar'};

    my @cmd;
    if (defined($sql)) {
        @cmd = ("$cfg->{'hcathome'}/bin/hcat", "-f", $sql);
    } else {
        @cmd = ("$cfg->{'hcathome'}/bin/hcat");
    }

    my $envStr;
    for my $k (keys(%ENV)) {
        $envStr .= $k . "=" . $ENV{$k} . " " if ($k =~ /HADOOP/ || $k =~ /HIVE/);
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
        "--skip-column-names");

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
    my $subName        = (caller(0))[3];
    my $jarkey         = shift;
    my ( $properties ) = @_;
    my $isPigSqlEnabled= 0;
    my @baseCmd;
    die "$0.$subName: null properties" if (! $properties );

show_call_stack();
    #UGLY HACK for pig sql support
    if ( $jarkey =~ /testsql/ ) {

       $isPigSqlEnabled= 1;
       $jarkey = "testjar";

    }

    my $cmd;
    if ( $properties->{'use-pig.pl'} ) {
      # The directive gives that
      # 1) the 'pig' command will be called, as opposed to java
      # 2) the conf file has full control over what options and parameters are 
      #    passed to pig. 
      #    I.e. no parameters should be passed automatically by the script here. 
      #
      # This allows for testing of the pig script as installed, and for testin of
      # the pig script's options, including error testing. 

print 'use-pig.pl?????';

      $cmd = $properties->{'gridstack.root'} . "/pig/" . $properties->{'pigTestBuildName'} . "/bin/pig";
      if ( ! -x "$cmd" ) {
        print STDERR "\n$0::$subName WARNING: Can't find pig command: $cmd\n";
        $cmd = `which pig`;
        chomp $cmd;
        print STDERR "$0::$subName WARNING: Instead using command: $cmd\n";
      }
      die "\n$0::$subName FATAL: Pig command does not exist: $cmd\n" if ( ! -x $cmd  );
      push (@baseCmd, $cmd );
   
       if(defined($properties->{'additionaljars'})) {
          push( @baseCmd,'-Dpig.additional.jars='.$properties->{'additionaljars'});
        }
        $ENV{'PIG_CLASSPATH'}=$properties->{'additionaljars'};  

      if ( $properties->{'use-pig.pl'} eq 'raw' ) { # add _no_ arguments automatically
        # !!! 
	return @baseCmd;
      }

    } else {
        $cmd="java";

print 'not use-pig.pl?????';
        # Set JAVA options

        # User can provide only one of
        # (-c <cluster>) OR (-testjar <jar> -testconfigpath <path>)
        # "-c <cluster>" is allowed only in non local mode
        if(defined($properties->{'cluster.name'})) {
            # use provided cluster
            @baseCmd = ($cmd, '-c', $properties->{'cluster.name'});
        } else {
    
                die "\n$0::$subName FATAL: The jar file name must be passed in at the command line or defined in the configuration file\n" if ( !defined( $properties->{$jarkey} ) );
                die "\n$0::$subName FATAL: The jar file does not exist.\n" . $properties->{$jarkey}."\n" if ( ! -e  $properties->{$jarkey}  );
    
            # use user provided jar
                my $classpath;

				if (defined $properties->{'jythonjar'}) {
					$classpath = "$classpath:" . $properties->{'jythonjar'};
				}
                if( $properties->{'exectype'} eq "local") {
                   # in local mode, we should not use
                   # any hadoop-site.xml
                   $classpath= "$classpath:" . $properties->{$jarkey};
                   $classpath= "$classpath:$properties->{'classpath'}" if ( defined( $properties->{'classpath'} ) );
                   @baseCmd = ($cmd, '-cp', $classpath, '-Xmx1024m');
    
                } else {
    
                   # non local mode, we also need to specify
                   # location of hadoop-site.xml
                   die "\n$0::$subName FATAL: The hadoop configuration file name must be passed in at the command line or defined in the configuration file\n" 
			if ( !defined( $properties->{'testconfigpath'} ) );
                   die "\n$0::$subName FATAL $! " . $properties->{'testconfigpath'}."\n\n"  
                   	if (! -e $properties->{'testconfigpath'} );

                   $classpath= "$classpath:" . $properties->{$jarkey}.":".$properties->{'testconfigpath'};
                   $classpath= "$classpath:$properties->{'classpath'}" if ( defined( $properties->{'classpath'} ) );
                   $classpath= "$classpath:$properties->{'howl.jar'}" if ( defined( $properties->{'howl.jar'} ) );
                   @baseCmd = ($cmd, '-cp', $classpath );
            }
        }
    
        # sets the queue, for exampel "grideng"
        if(defined($properties->{'queue'})) {
          push( @baseCmd,'-Dmapred.job.queue.name='.$properties->{'queue'});
        }
    
        if(defined($properties->{'additionaljars'})) {
          push( @baseCmd,'-Dpig.additional.jars='.$properties->{'additionaljars'});
        }
    
        if( ( $isPigSqlEnabled == 1 ) ){

	    if(defined($properties->{'metadata.uri'})) {
		push( @baseCmd, '-Dmetadata.uri='.$properties->{'metadata.uri'});
	    }

	    if(defined($properties->{'metadata.impl'})) {
		push( @baseCmd, '-Dmetadata.impl='.$properties->{'metadata.impl'});
	    }else{
		push( @baseCmd, '-Dmetadata.impl=org.apache.hadoop.owl.pig.metainterface.OwlPigMetaTables');
	    }
        }

        # Add howl support
	if(defined($properties->{'howl.metastore.uri'})) {
	  push( @baseCmd, '-Dhowl.metastore.uri='.$properties->{'howl.metastore.uri'});
	}
    
      # Set local mode property
      # if ( defined($properties->{'exectype'}) && $properties->{'exectype'}=~ "local" ) {
      # Removed above 'if...' for Pig 8.
        my $java=`which java`;
        my $version=`file $java`;
        if ( $version =~ '32-bit' ){
           push(@baseCmd,'-Djava.library.path='.$ENV{HADOOP_HOME}.'/lib/native/Linux-i386-32');
        } else {
           push(@baseCmd,'-Djava.library.path='.$ENV{HADOOP_HOME}.'/lib/native/Linux-amd64-64');
        }
      # }


        # Add user provided java options if they exist
        if (defined($properties->{'java_params'})) {
          push(@baseCmd, @{$properties->{'java_params'}});
        }
    
        if(defined($properties->{'hod'})) {
          push( @baseCmd, '-Dhod.server=');
        }

      # sets the permissions on the jobtracker for the logs
      push( @baseCmd,'-Dmapreduce.job.acl-view-job=*');


      # Add Main
      push(@baseCmd, 'org.apache.pig.Main');

      # Set local mode PIG option
      if ( defined($properties->{'exectype'}) && $properties->{'exectype'}=~ "local" ) {
          push(@baseCmd, '-x');
          push(@baseCmd, 'local');
      }

      # Set Pig SQL options
      if( ( $isPigSqlEnabled == 1 ) && defined($properties->{'metadata.uri'})) {
  
         if ( defined($properties->{'testoutpath'}) ) {
           push( @baseCmd, '-u' );
           push( @baseCmd, $properties->{'testoutpath'} );
         }
  
         push( @baseCmd, '-s' );
         push( @baseCmd, '-f' );
      }

    } # end else of if use-pig.pl


    # Add -latest or -useversion 
    if ( $cmd =~ 'pig$' ) {
      # Add -latest, or -useversion if 'current' is not target build
      if ( defined($properties->{'pigTestBuildName'})) {
        if ($properties->{'pigTestBuildName'} eq 'latest') {
            push(@baseCmd, '-latest');
        } elsif ($properties->{'pigTestBuildName'} ne 'current') {
            push(@baseCmd, '-useversion', "$properties->{'pigTestBuildName'}");
        }
      }
    } elsif ( $cmd =~ 'java' ) {

      # is this ever used: ???
      # Add latest if it's there
      if (defined($properties->{'latest'})) {
          push(@baseCmd, '-latest');
      }
    }

    return @baseCmd;
}


sub setLocale
{
   my $locale= shift;
#   $locale = "en_US.UTF-8" if ( !$locale );
$locale = "ja_JP.utf8" if ( !$locale );
   $ENV[LC_CTYPE]="$locale";
   $ENV[LC_NUMERIC]="$locale";
   $ENV[LC_TIME]="$locale";
   $ENV[LC_COLLATE]="$locale";
   $ENV[LC_MONETARY]="$locale";
   $ENV[LC_MESSAGES]="$locale";
   $ENV[LC_PAPER]="$locale";
   $ENV[LC_NAME]="$locale";
   $ENV[LC_ADDRESS]="$locale";
   $ENV[LC_TELEPHONE]="$locale";
   $ENV[LC_MEASUREMENT]="$locale";
   $ENV[LC_IDENTIFICATION]="$locale";
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

sub findPigWithoutHadoopJar($$)
{
    my ($cfg, $log) = @_;

    my $jar = `ls $cfg->{'pigpath'}/pig-*withouthadoop.jar`;
    chomp $jar;
    return $jar;
}

1;
