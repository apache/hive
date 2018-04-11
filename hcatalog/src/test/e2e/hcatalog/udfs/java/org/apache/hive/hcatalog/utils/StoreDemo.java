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
import java.util.HashMap;
import java.util.Map;

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
 * This is a map reduce test for testing hcat which goes against the "numbers"
 * table and writes data to another table. It reads data from numbers which
 * is an unpartitioned table and adds 10 to each field. It stores the result into
 * the datestamp='20100101' partition of the numbers_part_empty_initially table if the second
 * command line arg is "part". If the second cmdline arg is "nopart" then the
 * result is stored into the 'numbers_nopart_empty_initially' (unpartitioned) table.
 * If the second cmdline arg is "nopart_pig", then the result is stored into the
 * 'numbers_nopart_pig_empty_initially' (unpartitioned) table with the tinyint
 * and smallint columns in "numbers" being stored as "int" (since pig cannot handle
 * tinyint and smallint)
 *
 * Usage: hadoop jar storenumbers <serveruri> <part|nopart|nopart_pig> <-libjars hive-hcat jar>
 If the second argument is "part" data is written to datestamp = '2010101' partition of the numbers_part_empty_initially table.
 If the second argument is "nopart", data is written to the unpartitioned numbers_nopart_empty_initially table.
 If the second argument is "nopart_pig", data is written to the unpartitioned numbers_nopart_pig_empty_initially table.
 The hcat jar location should be specified as file://<full path to jar>
 */
public class StoreDemo {

  private static final String NUMBERS_PARTITIONED_TABLE_NAME = "demo_partitioned";
  private static final String NUMBERS_TABLE_NAME = "demo";

  public static class SumMapper
    extends Mapper<WritableComparable, HCatRecord, WritableComparable, HCatRecord> {


    Integer intnum;

    Double doublenum;

    @Override
    protected void map(WritableComparable key, HCatRecord value,
               org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord,
                 WritableComparable, HCatRecord>.Context context)
      throws IOException, InterruptedException {
      intnum = ((Integer) value.get(0));
      value.set(0, intnum + 20);
      doublenum = ((Double) value.get(1));
      value.set(1, (Double) (doublenum + 20));
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

    String tableName = NUMBERS_TABLE_NAME;
    String dbName = "default";
    Map<String, String> outputPartitionKvps = new HashMap<String, String>();
    String outputTableName = NUMBERS_PARTITIONED_TABLE_NAME;
    outputPartitionKvps.put("datestamp", "20100102");

    String principalID = System.getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
    if (principalID != null)
      conf.set(HCatConstants.HCAT_METASTORE_PRINCIPAL, principalID);
    Job job = new Job(conf, "storedemo");
    // initialize HCatInputFormat
    HCatInputFormat.setInput(job,
      dbName, tableName);
    // initialize HCatOutputFormat
    HCatOutputFormat.setOutput(job, OutputJobInfo.create(
      dbName, outputTableName, outputPartitionKvps));
    // test with and without specifying schema randomly
    HCatSchema s = HCatInputFormat.getTableSchema(job);
    System.err.println("INFO: output schema explicitly set for writing:" + s);
    HCatOutputFormat.setSchema(job, s);

    job.setInputFormatClass(HCatInputFormat.class);
    job.setOutputFormatClass(HCatOutputFormat.class);
    job.setJarByClass(StoreDemo.class);
    job.setMapperClass(SumMapper.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setNumReduceTasks(0);
    job.setOutputValueClass(DefaultHCatRecord.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


  /**
   *
   */
  private static void usage() {
    System.err.println("Usage: hadoop jar storenumbers <serveruri> <part|nopart|nopart_pig> <-libjars hive-hcat jar>\n" +
      "\tIf the second argument is \"part\" data is written to datestamp = '2010101' partition of " +
      "the numbers_part_empty_initially table.\n\tIf the second argument is \"nopart\", data is written to " +
      "the unpartitioned numbers_nopart_empty_initially table.\n\tIf the second argument is \"nopart_pig\", " +
      "data is written to the unpartitioned numbers_nopart_pig_empty_initially table.\nt" +
      "The hcat jar location should be specified as file://<full path to jar>\n");
    System.exit(2);

  }


}
