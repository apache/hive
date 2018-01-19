/*
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

package org.apache.hive.hcatalog.utils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

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
public class GroupByAge extends Configured implements Tool {

  public static class Map extends
    Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {

    int age;

    @Override
    protected void map(
      WritableComparable key,
      HCatRecord value,
      org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable>.Context context)
      throws IOException, InterruptedException {
      age = (Integer) value.get(1);
      context.write(new IntWritable(age), new IntWritable(1));
    }
  }

  public static class Reduce extends Reducer<IntWritable, IntWritable,
    WritableComparable, HCatRecord> {


    @Override
    protected void reduce(IntWritable key, java.lang.Iterable<IntWritable>
      values, org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable, WritableComparable, HCatRecord>.Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum++;
        iter.next();
      }
      HCatRecord record = new DefaultHCatRecord(2);
      record.set(0, key.get());
      record.set(1, sum);

      context.write(null, record);
    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    args = new GenericOptionsParser(conf, args).getRemainingArgs();

    String serverUri = args[0];
    String inputTableName = args[1];
    String outputTableName = args[2];
    String dbName = null;

    String principalID = System
      .getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
    if (principalID != null)
      conf.set(HCatConstants.HCAT_METASTORE_PRINCIPAL, principalID);
    Job job = new Job(conf, "GroupByAge");
    HCatInputFormat.setInput(job, dbName,
      inputTableName);
    // initialize HCatOutputFormat

    job.setInputFormatClass(HCatInputFormat.class);
    job.setJarByClass(GroupByAge.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(WritableComparable.class);
    job.setOutputValueClass(DefaultHCatRecord.class);
    HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName,
      outputTableName, null));
    HCatSchema s = HCatOutputFormat.getTableSchema(job);
    System.err.println("INFO: output schema explicitly set for writing:"
      + s);
    HCatOutputFormat.setSchema(job, s);
    job.setOutputFormatClass(HCatOutputFormat.class);
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new GroupByAge(), args);
    System.exit(exitCode);
  }
}
