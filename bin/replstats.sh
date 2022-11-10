# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ $1 = "toHive" ]; then
#Loading of already generated CSV file into a hive table.
if [ $# -lt 5 ]; then
 echo "Invalid Arguments."
 echo "Usage: replstat.sh [toHive <table-name> <csv location> <hdfs location> <partition name> [beeline args]] |
 [policyName executionId [pathForFiles]]"
 echo "toHive mode is to run the script to load csv generated using replstat.sh policyName executionId [pathForFiles] into hive table"
    exit 1;
fi
tablename=$2
loc=$3
hdfsloc=$4
part="$hdfsloc/org=""$5"

#Load data to filesystem
hdfs dfs -mkdir -p $part;
hdfs dfs -put -f $loc $part;

#Create table in beeline
beelineargs=$6

#Run beeline command to create a table, then add the partition, and then create a view from the table in a format on
# which mathematical operations can be performed.
beeline $beelineargs -e "create external table IF NOT EXISTS $2 (JobId string, SubmitTime string, LaunchTime string, LaunchDuration string, FinishedTime string, FinishDuration string, Mapper_Success int, Mappers_Failed int, BytesCopied BIGINT, Files int, Status string)  partitioned by (org string) row format delimited fields terminated by ',' stored as textfile location '$hdfsloc' TBLPROPERTIES ('skip.header.line.count'='1');alter table $2 add IF NOT EXISTS partition(org='""$5""');create view IF NOT EXISTS "$2"_view as select org, jobid,  (unix_timestamp(launchtime,'dd-MMM-yyyy HH:mm:ss') - unix_timestamp(submittime,'dd-MMM-yyyy HH:mm:ss')) as launchDuration, (unix_timestamp(finishedtime,'dd-MMM-yyyy HH:mm:ss') - unix_timestamp(launchtime,'dd-MMM-yyyy HH:mm:ss')) as finishDuration, mapper_success, mappers_failed, bytescopied, files, status from "$2

echo "Created table $2 and view ""$2""_view with partition $5"

#Creation of Stats part(CSV & Stats generation)
else
#Validate the number of arguments.
if [ $# -gt 3 ]; then
    echo "Invalid Arguments. Required Max 3, but provided: ""$#"
    echo "Usage: replstat.sh [toHive <table-name> <csv location> <hdfs location> <partition name> [beeline args]] |
 [policyName executionId [pathForFiles]]"
    exit 1;
fi

if [ $# -lt 2 ]; then
    echo "Missing Arguments"
    echo  "Usage: replStat.sh policyName executionId [pathForFiles]"
    exit 1;
fi

policy=$1
index=$2
togrep="Repl#"$policy"#"$index
csv="~/"$togrep"values.csv"
echo "Getting details of mapred jobs containing ""$togrep"" as part of their Application Name"
path=$3

#If path isn't specified
if [ $# -eq 2 ]; then
    echo "Path isn't explicitily provided. Using ""$PWD"
    path=$PWD
fi

#Mapred command to list all jobs, Then from that extract the list of job ids which contains the application name, then get history for all the job ids and dump into the stat file one after the other

mapred job  -list all | grep $togrep > $path/$togrep"list.txt";awk -F "\t"  '{print system("mapred job -history all "$1)}' $path/$togrep"list.txt" > $path/$togrep"stats.txt";rm $path/$togrep"list.txt" 

#Header for CLI output
printf '%25s %25s %30s %25s %30s %25s %18s %18s %18s %10s %15s\n' "Job Id" "Submit Time" "Launch Time" "Launch Duration" "Finished Time" "Finish Duration" "Mappers(Success)" "Mappers(Failed)" "Bytes Copied" "Files" "Status"

#Header for CSV file.
header="Job Id,Submit Time,Launch Time,Launch Duration,Finished Time,Finish Duration,Mappers(Success),Mappers(Failed),Bytes Copied,Status"

#Print the Header of the CLI
echo "$header">$path/$togrep"values.csv"

#Read the stat file and generate the stats for each jobId
while IFS= read -r line; do
  if [[ $line == *"Hadoop job:"* ]]; then
  jobId="${line/Hadoop job:/}"
fi
  if [[ $line == *"Submitted At: "* ]]; then
  submit="${line/Submitted At: /}"
fi
  if [[ $line == *"Launched At: "* ]]; then
  launch="${line/Launched At: /}"
  ltime="${line#*(}"
  ltime="${ltime%?}"
  launch=${launch% (*}
fi
  if [[ $line == *"Finished At: "* ]]; then
  finish="${line/Finished At: /}"
  ftime="${line#*(}"
  ftime="${ftime%?}"
  finish=${finish% (*}
fi
    if [[ $line == *"Status: "* ]]; then
  status="${line/Status: /}"
fi

  if [[ $line == *"Map"* ]]; then
   if [[ $line != *"Map-Reduce"* ]]; then
        if [[ $line != *"Map Value"* ]]; then
stringarray=($line)
success=${stringarray[2]}
failedmappers=${stringarray[3]}

#This is the last entry for the job id, so we have all the fields now, we can print on the CLI as well as write on the csv file now.
printf '%25s %25s %30s %25s %30s %25s %18s %18s %18s %10s %15s\n' "$jobId" "$submit" "$launch" "$ltime" "$finish" "$ftime" "$success" "$failedmappers" "$bytes" "$files" "$status"
entry="$jobId"",""$submit"",""$launch"","$ltime",""$finish"",""$ftime"",""$success"","$failedmappers","$bytes",""$files"",""$status"
echo "$entry" >>$path/$togrep"values.csv"
     fi
   fi
fi
  if [[ $line == *"Bytes Copied"* ]]; then
bytes="${line#*|*|*|*|*|}";
stringarray=($bytes)
bytes=${stringarray[0]}
fi
  if [[ $line == *"Files Copied"* ]]; then
files="${line#*|*|*|*|*|}";
stringarray=($files)
files=${stringarray[0]}
fi

done < $path/$togrep"stats.txt"

#Finished, Print the path of the files.
echo "Stats File is at "$path/$togrep"stats.txt and CSV file is at "$path/$togrep"values.csv"

fi