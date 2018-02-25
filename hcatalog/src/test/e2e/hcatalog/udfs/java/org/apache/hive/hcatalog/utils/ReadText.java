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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

/**
 * This is a map reduce test for testing hcat which goes against the "numbers"
 * table. It performs a group by on the first column and a SUM operation on the
 * other columns. This is to simulate a typical operation in a map reduce program
 * to test that hcat hands the right data to the map reduce program
 *
 * Usage: hadoop jar sumnumbers <serveruri> <output dir> <-libjars hive-hcat jar>
 The <tab|ctrla> argument controls the output delimiter
 The hcat jar location should be specified as file://<full path to jar>
 */
public class ReadText extends Configured implements Tool {

  public static class Map
    extends Mapper<WritableComparable, HCatRecord, IntWritable, HCatRecord> {

    byte t;
    short si;
    int i;
    long b;
    float f;
    double d;
    String s;

    @Override
    protected void map(WritableComparable key, HCatRecord value,
               org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord,
                 IntWritable, HCatRecord>.Context context)
      throws IOException, InterruptedException {
      t = (Byte) value.get(0);
      si = (Short) value.get(1);
      i = (Integer) value.get(2);
      b = (Long) value.get(3);
      f = (Float) value.get(4);
      d = (Double) value.get(5);
      s = (String) value.get(6);

      HCatRecord record = new DefaultHCatRecord(7);
      record.set(0, t);
      record.set(1, si);
      record.set(2, i);
      record.set(3, b);
      record.set(4, f);
      record.set(5, d);
      record.set(6, s);

      context.write(null, record);

    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    args = new GenericOptionsParser(conf, args).getRemainingArgs();

    String serverUri = args[0];
    String tableName = args[1];
    String outputDir = args[2];
    String dbName = null;

    String principalID = System.getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
    if (principalID != null)
      conf.set(HCatConstants.HCAT_METASTORE_PRINCIPAL, principalID);
    Job job = new Job(conf, "ReadText");
    HCatInputFormat.setInput(job,
      dbName, tableName);
    // initialize HCatOutputFormat

    job.setInputFormatClass(HCatInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setJarByClass(ReadText.class);
    job.setMapperClass(Map.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(HCatRecord.class);
    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ReadText(), args);
    System.exit(exitCode);
  }
}
