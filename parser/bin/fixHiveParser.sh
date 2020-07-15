#!/bin/bash

# This is a temporary solution for the issue of the "code too large" problem related to HiveParser.java
# We got to a point where adding anything to the antlr files lead to an issue about having a HiveParser.java that can not be compiled due to the compiled code size limitation in java (maximum 65536 bytes), so to avoid it we temorarly add this script to remove the huge tokenNames array into a separate file.
# The real solution would be to switch to antlr 4

input="target/generated-sources/antlr3/org/apache/hadoop/hive/ql/parse/HiveParser.java"
output="target/generated-sources/antlr3/org/apache/hadoop/hive/ql/parse/HiveParser.java-fixed"
tokenFile="target/generated-sources/antlr3/org/apache/hadoop/hive/ql/parse/HiveParserTokens.java"

# create HiveParserTokens containing the tokenNames 

rm $tokenFile > /dev/null 2>&1

cat <<EOT >> $tokenFile
package org.apache.hadoop.hive.ql.parse;

public class HiveParserTokens {
EOT

awk '/tokenNames/ { matched = 1 } matched' $input | awk '{print} /};/ {exit}' >> $tokenFile

echo "}" >> $tokenFile

# remove tokenNames array from the original file

rm $output > /dev/null 2>&1

awk '/tokenNames/ {exit} {print}' $input >> $output
echo "  public static final String[] tokenNames = HiveParserTokens.tokenNames;" >> $output
awk 'matched; /};$/ { matched = 1 }' $input >> $output

mv $output $input
