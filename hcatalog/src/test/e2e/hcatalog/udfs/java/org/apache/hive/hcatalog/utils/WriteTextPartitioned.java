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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

/**
 * This is a map reduce test for testing hcat writing to partitioned tables.
 * table. It performs a group by on the first column and a SUM operation on the
 * other columns. This is to simulate a typical operation in a map reduce
 * program to test that hcat hands the right data to the map reduce program
 *
 * Usage: hadoop jar org.apache.hive.hcatalog.utils.WriteTextPartitioned -libjars
 * &lt;hcat_jar&gt; * &lt;serveruri&gt; &lt;input_tablename&gt; &lt;output_tablename&gt; [filter]
 * If filter is given it will be provided as the partition to write to.
 */
public class WriteTextPartitioned extends Configured implements Tool {

  static String filter = null;

  public static class Map extends
    Mapper<WritableComparable, HCatRecord, WritableComparable, HCatRecord> {

    @Override
    protected void map(
      WritableComparable key,
      HCatRecord value,
      org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, WritableComparable, HCatRecord>.Context context)
      throws IOException, InterruptedException {
      String name = (String) value.get(0);
      int age = (Integer) value.get(1);
      String ds = (String) value.get(3);

      HCatRecord record = (filter == null ? new DefaultHCatRecord(3) : new DefaultHCatRecord(2));
      record.set(0, name);
      record.set(1, age);
      if (filter == null) record.set(2, ds);

      context.write(null, record);

    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    args = new GenericOptionsParser(conf, args).getRemainingArgs();

    String serverUri = args[0];
    String inputTableName = args[1];
    String outputTableName = args[2];
    if (args.length > 3) filter = args[3];
    String dbName = null;

    String principalID = System
      .getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
    if (principalID != null)
      conf.set(HCatConstants.HCAT_METASTORE_PRINCIPAL, principalID);
    Job job = new Job(conf, "WriteTextPartitioned");
    HCatInputFormat.setInput(job, dbName,
      inputTableName, filter);
    // initialize HCatOutputFormat

    job.setInputFormatClass(HCatInputFormat.class);
    job.setJarByClass(WriteTextPartitioned.class);
    job.setMapperClass(Map.class);
    job.setOutputKeyClass(WritableComparable.class);
    job.setOutputValueClass(DefaultHCatRecord.class);
    job.setNumReduceTasks(0);

    java.util.Map<String, String> partitionVals = null;
    if (filter != null) {
      String[] s = filter.split("=");
      String val = s[1].replace('"', ' ').trim();
      partitionVals = new HashMap<String, String>(1);
      partitionVals.put(s[0], val);
    }
    HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName,
      outputTableName, partitionVals));
    HCatSchema s = HCatInputFormat.getTableSchema(job);
    // Build the schema for this table, which is slightly different than the
    // schema for the input table
    List<HCatFieldSchema> fss = new ArrayList<HCatFieldSchema>(3);
    fss.add(s.get(0));
    fss.add(s.get(1));
    fss.add(s.get(3));
    HCatOutputFormat.setSchema(job, new HCatSchema(fss));
    job.setOutputFormatClass(HCatOutputFormat.class);
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new WriteTextPartitioned(), args);
    System.exit(exitCode);
  }
}
