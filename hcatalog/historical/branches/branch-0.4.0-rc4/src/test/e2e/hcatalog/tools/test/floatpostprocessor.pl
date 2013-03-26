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
                                                                                       
#
# A simple tool to make sure all floats in the output are written the same way.
# It is assumed that the data in question is being read from stdin.
#
#

use strict;

our @floats;
our $delim;

sub parseLine($)
{
	my $line = shift;
	chomp $line;
	return split(/$delim/, $line);
}

sub postprocess($)
{
	my @fields = parseLine(shift);

	for (my $i = 0; $i < @fields; $i++) {
		if ($i != 0) { print($delim); }
		if ($floats[$i]) {
			printf("%.3f", $fields[$i]);
		} else {
			print($fields[$i]);
		}
	}
	print "\n";
}

sub is_float {
	my $n = shift;
	if(!defined $n || $n eq ""){
		return 0;
	}
	if($n =~ /^[+-]?\d+\.\d+([eE][-+]?[0-9]+)?$/){
		return 1;
	}

	my $abs = abs($n);
	if ($abs - int($abs) > 0) {
		return 1;
	}
	return 0;
}


# main
{
	$delim = shift;
	if (!defined($delim)) {
		die "Usage: $0 delimiter\n";
	}

	my @sampled;
    my $line;
    # read away any empty lines into the sample
    do {
	    $line = <STDIN>;
	    push(@sampled, $line);
    } while($line && $line =~ /^\s*$/);
	# Sample the next thousand lines to figure out which columns have floats.
	for (my $i = 0; $i < 1000 && ($line = <STDIN>); $i++) {
		push(@sampled, $line);
	}
    foreach my $line (@sampled) {
		my @fields = parseLine($line);
		for (my $j = 0; $j < @fields; $j++) {
			if(is_float($fields[$j])){
				$floats[$j] = 1;				
			}


		}
    }

	# Now, play each of the sampled lines through the postprocessor
	foreach my $line (@sampled) {
		postprocess($line);
	}

	while (<STDIN>) {
		postprocess($_);
	}

}



	
