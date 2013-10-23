/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

/**
 * This is a map reduce test for testing hcat which goes against the "numbers"
 * table. It performs a group by on the first column and a SUM operation on the
 * other columns. This is to simulate a typical operation in a map reduce
 * program to test that hcat hands the right data to the map reduce program
 *
 * Usage: hadoop jar sumnumbers <serveruri> <output dir> <-libjars hive-hcat
 * jar> The <tab|ctrla> argument controls the output delimiter The hcat jar
 * location should be specified as file://<full path to jar>
 */
public class HBaseReadWrite extends Configured implements Tool {

  public static class HBaseWriteMap extends
    Mapper<LongWritable, Text, Text, Text> {

    String name;
    String age;
    String gpa;

    @Override
    protected void map(
      LongWritable key,
      Text value,
      org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException {
      String line = value.toString();
      String[] tokens = line.split("\t");
      name = tokens[0];

      context.write(new Text(name), value);
    }
  }


  public static class HBaseWriteReduce extends
    Reducer<Text, Text, WritableComparable, HCatRecord> {

    String name;
    String age;
    String gpa;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      name = key.toString();
      int count = 0;
      double sum = 0;
      for (Text value : values) {
        String line = value.toString();
        String[] tokens = line.split("\t");
        name = tokens[0];
        age = tokens[1];
        gpa = tokens[2];

        count++;
        sum += Double.parseDouble(gpa.toString());
      }

      HCatRecord record = new DefaultHCatRecord(2);
      record.set(0, name);
      record.set(1, Double.toString(sum));

      context.write(null, record);
    }
  }

  public static class HBaseReadMap extends
    Mapper<WritableComparable, HCatRecord, Text, Text> {

    String name;
    String age;
    String gpa;

    @Override
    protected void map(
      WritableComparable key,
      HCatRecord value,
      org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, Text, Text>.Context context)
      throws IOException, InterruptedException {
      name = (String) value.get(0);
      gpa = (String) value.get(1);
      context.write(new Text(name), new Text(gpa));
    }
  }


  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    args = new GenericOptionsParser(conf, args).getRemainingArgs();

    String serverUri = args[0];
    String inputDir = args[1];
    String tableName = args[2];
    String outputDir = args[3];
    String dbName = null;

    String principalID = System
      .getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
    if (principalID != null)
      conf.set(HCatConstants.HCAT_METASTORE_PRINCIPAL, principalID);
    conf.set("hcat.hbase.output.bulkMode", "false");
    Job job = new Job(conf, "HBaseWrite");
    FileInputFormat.setInputPaths(job, inputDir);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(HCatOutputFormat.class);
    job.setJarByClass(HBaseReadWrite.class);
    job.setMapperClass(HBaseWriteMap.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(HBaseWriteReduce.class);
    job.setOutputKeyClass(WritableComparable.class);
    job.setOutputValueClass(DefaultHCatRecord.class);
    HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName,
      tableName, null));

    boolean succ = job.waitForCompletion(true);

    if (!succ) return 1;

    job = new Job(conf, "HBaseRead");
    HCatInputFormat.setInput(job, dbName, tableName);

    job.setInputFormatClass(HCatInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setJarByClass(HBaseReadWrite.class);
    job.setMapperClass(HBaseReadMap.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    TextOutputFormat.setOutputPath(job, new Path(outputDir));

    succ = job.waitForCompletion(true);

    if (!succ) return 2;

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HBaseReadWrite(), args);
    System.exit(exitCode);
  }
}

