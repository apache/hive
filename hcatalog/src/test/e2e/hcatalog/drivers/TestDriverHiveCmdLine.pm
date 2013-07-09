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

################################################################################


# Test driver for hive nightly tests.
# 
#

package TestDriverHiveCmdLine;
use TestDriverHive;
use IPC::Run; # don't do qw(run), it screws up TestDriver which also has a run method
use Util;
use File::Path;
use Cwd;

use strict;
use English;

our $className= "TestDriverHive";
our @ISA = "$className";

sub new
{
    # Call our parent
    my ($proto) = @_;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new;

    bless($self, $class);
    return $self;
}

sub runTest
{
    my ($self, $testCmd, $log) = @_;

    my %result;

    my ($stdout, $stderr);

    # If they provided a hive script in 'sql', write it to a file.
    my $hivefile = undef;
    if (defined($testCmd->{'sql'})) {
        $hivefile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" .
            $testCmd->{'num'} . ".sql";

        open(FH, "> $hivefile") or
            die "Unable to open file $hivefile to write SQL script, $ERRNO\n";
        print FH $testCmd->{'sql'} . "\n";
        close(FH);
    }
    Util::runHiveCmdFromFile($testCmd, $log, $hivefile, \$stdout, \$stderr, 1);
    $result{'rc'} = $? >> 8;

    $result{'stdout'} = $stdout;
    $result{'stderr'} = $stderr;

    return \%result;
}



sub generateBenchmark
{
    # Intentionally empty
}

sub compare
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;

    my $result = 1;  # until proven wrong...

    # Return Code
    if (defined $testCmd->{'rc'}) {
        if ((! defined $testResult->{'rc'}) ||
                ($testResult->{'rc'} != $testCmd->{'rc'})) {
            print $log "Check failed: rc = <" . $testCmd->{'rc'} .
                "> expected, test returned rc = <" . $testResult->{'rc'}
                . ">\n";
            $result = 0;
        }
    }

    # Standard Out
    if (defined $testCmd->{'expected_out'}) {
        if ($testResult->{'stdout'} ne $testCmd->{'expected_out'}) {
            print $log "Check failed: exact match of <" .
                $testCmd->{'expected_out'} .
                "> expected in stdout:<" . $testResult->{'stdout'} 
                . ">\n";
            $result = 0;
        }
    }

    if (defined $testCmd->{'not_expected_out'}) {
        if ($testResult->{'stdout'} eq $testCmd->{'not_expected_out'}) {
            print $log "Check failed: NON-match of <" .
                $testCmd->{'expected_out'} . "> expected to stdout:<" .
                $testResult->{'stdout'} . ">\n";
            $result = 0;
        }
    }

    if (defined $testCmd->{'expected_out_regex'}) {
        if ($testResult->{'stdout'} !~ $testCmd->{'expected_out_regex'}) {
            print $log "Check failed: regex match of <" . 
                $testCmd->{'expected_out_regex'} . "> expected in stdout:<" .
                $testResult->{'stdout'} . ">\n";
            $result = 0;
        }
    }

    if (defined $testCmd->{'not_expected_out_regex'}) {
        if ($testResult->{'stdout'} =~ $testCmd->{'not_expected_out_regex'}) {
            print $log "Check failed: regex NON-match of <" . 
                $testCmd->{'not_expected_out_regex'} .
                "> expected in stdout:<" . $testResult->{'stdout'} . ">\n";
            $result = 0;
        }
    }

    # Standard Error
    if (defined $testCmd->{'expected_err'}) {
        if ($testResult->{'stderr'} ne $testCmd->{'expected_err'}) {
            print $log "Check failed: exact match of <" .
                $testCmd->{'expected_err'} .
                "> expected in stderr:<" . $testResult->{'stderr'} 
                . ">\n";
            $result = 0;
        }
    }

    if (defined $testCmd->{'not_expected_err'}) {
        if ($testResult->{'stderr'} eq $testCmd->{'not_expected_err'}) {
            print $log "Check failed: NON-match of <" .
                $testCmd->{'expected_err'} . "> expected to stderr:<" .
                $testResult->{'stderr'} . ">\n";
            $result = 0;
        }
    }

    if (defined $testCmd->{'expected_err_regex'}) {
        if ($testResult->{'stderr'} !~ $testCmd->{'expected_err_regex'}) {
            print $log "Check failed: regex match of <" . 
                $testCmd->{'expected_err_regex'} . "> expected in stderr:<" .
                $testResult->{'stderr'} . ">\n";
            $result = 0;
        }
    }

    if (defined $testCmd->{'not_expected_err_regex'}) {
        if ($testResult->{'stderr'} =~ $testCmd->{'not_expected_err_regex'}) {
            print $log "Check failed: regex NON-match of <" . 
                $testCmd->{'not_expected_err_regex'} .
                "> expected in stderr:<" . $testResult->{'stderr'} . ">\n";
            $result = 0;
        }
    }


  return $result;
}

1;
