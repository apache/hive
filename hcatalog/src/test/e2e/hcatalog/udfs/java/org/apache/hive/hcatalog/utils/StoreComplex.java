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

package org.apache.hive.hcatalog.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

/**
 * This is a map reduce test for testing hcat which goes against the "complex"
 * table and writes to "complex_nopart_empty_initially" table. It reads data from complex which
 * is an unpartitioned table and stores the data as-is into complex_empty_initially table
 * (which is also unpartitioned)
 *
 * Usage: hadoop jar testudf.jar storecomplex <serveruri> <-libjars hive-hcat jar>  
 The hcat jar location should be specified as file://<full path to jar>
 */
public class StoreComplex {

  private static final String COMPLEX_TABLE_NAME = "complex";
  private static final String COMPLEX_NOPART_EMPTY_INITIALLY_TABLE_NAME = "complex_nopart_empty_initially";


  public static class ComplexMapper
    extends Mapper<WritableComparable, HCatRecord, WritableComparable, HCatRecord> {

    @Override
    protected void map(WritableComparable key, HCatRecord value,
               org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord,
                 WritableComparable, HCatRecord>.Context context)
      throws IOException, InterruptedException {
      // just write out the value as-is
      context.write(new IntWritable(0), value);

    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    args = new GenericOptionsParser(conf, args).getRemainingArgs();
    String[] otherArgs = new String[1];
    int j = 0;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-libjars")) {
        // generic options parser doesn't seem to work!
        conf.set("tmpjars", args[i + 1]);
        i = i + 1; // skip it , the for loop will skip its value
      } else {
        otherArgs[j++] = args[i];
      }
    }
    if (otherArgs.length != 1) {
      usage();
    }
    String serverUri = otherArgs[0];
    String tableName = COMPLEX_TABLE_NAME;
    String dbName = "default";
    Map<String, String> outputPartitionKvps = new HashMap<String, String>();
    String outputTableName = null;
    outputTableName = COMPLEX_NOPART_EMPTY_INITIALLY_TABLE_NAME;
    // test with null or empty randomly
    if (new Random().nextInt(2) == 0) {
      System.err.println("INFO: output partition keys set to null for writing");
      outputPartitionKvps = null;
    }
    String principalID = System.getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
    if (principalID != null)
      conf.set(HCatConstants.HCAT_METASTORE_PRINCIPAL, principalID);
    Job job = new Job(conf, "storecomplex");
    // initialize HCatInputFormat

    HCatInputFormat.setInput(job,
      dbName, tableName);
    // initialize HCatOutputFormat
    HCatOutputFormat.setOutput(job, OutputJobInfo.create(
      dbName, outputTableName, outputPartitionKvps));


    HCatSchema s = HCatInputFormat.getTableSchema(job);
    HCatOutputFormat.setSchema(job, s);
    job.setInputFormatClass(HCatInputFormat.class);
    job.setOutputFormatClass(HCatOutputFormat.class);
    job.setJarByClass(StoreComplex.class);
    job.setMapperClass(ComplexMapper.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DefaultHCatRecord.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


  /**
   *
   */
  private static void usage() {
    System.err.println("Usage: hadoop jar testudf.jar storecomplex <serveruri> <-libjars hive-hcat jar>\n" +
      "The hcat jar location should be specified as file://<full path to jar>\n");
    System.exit(2);

  }


}
