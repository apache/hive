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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hive.hcatalog.common.HCatConstants;
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
public class SumNumbers {

  private static final String NUMBERS_TABLE_NAME = "numbers";
  private static final String TAB = "\t";

  public static class SumMapper
    extends Mapper<WritableComparable, HCatRecord, IntWritable, SumNumbers.ArrayWritable> {

    IntWritable intnum1000;
    // though id is given as a Short by hcat, the map will emit it as an
    // IntWritable so we can just sum in the reduce
    IntWritable id;

    // though intnum5 is handed as a Byte by hcat, the map() will emit it as
    // an IntWritable so we can just sum in the reduce
    IntWritable intnum5;
    IntWritable intnum100;
    IntWritable intnum;
    LongWritable longnum;
    FloatWritable floatnum;
    DoubleWritable doublenum;

    @Override
    protected void map(WritableComparable key, HCatRecord value,
               org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord,
                 IntWritable, SumNumbers.ArrayWritable>.Context context)
      throws IOException, InterruptedException {
      intnum1000 = new IntWritable((Integer) value.get(0));
      id = new IntWritable((Short) value.get(1));
      intnum5 = new IntWritable(((Byte) value.get(2)));
      intnum100 = new IntWritable(((Integer) value.get(3)));
      intnum = new IntWritable((Integer) value.get(4));
      longnum = new LongWritable((Long) value.get(5));
      floatnum = new FloatWritable((Float) value.get(6));
      doublenum = new DoubleWritable((Double) value.get(7));
      SumNumbers.ArrayWritable outputValue = new SumNumbers.ArrayWritable(id,
        intnum5, intnum100, intnum, longnum, floatnum, doublenum);
      context.write(intnum1000, outputValue);

    }
  }

  public static class SumReducer extends Reducer<IntWritable, SumNumbers.ArrayWritable,
    LongWritable, Text> {


    LongWritable dummyLong = null;

    @Override
    protected void reduce(IntWritable key, java.lang.Iterable<ArrayWritable>
      values, org.apache.hadoop.mapreduce.Reducer<IntWritable, ArrayWritable, LongWritable, Text>.Context context)
      throws IOException, InterruptedException {
      String output = key.toString() + TAB;
      Long sumid = 0l;
      Long sumintnum5 = 0l;
      Long sumintnum100 = 0l;
      Long sumintnum = 0l;
      Long sumlongnum = 0l;
      Float sumfloatnum = 0.0f;
      Double sumdoublenum = 0.0;
      for (ArrayWritable value : values) {
        sumid += value.id.get();
        sumintnum5 += value.intnum5.get();
        sumintnum100 += value.intnum100.get();
        sumintnum += value.intnum.get();
        sumlongnum += value.longnum.get();
        sumfloatnum += value.floatnum.get();
        sumdoublenum += value.doublenum.get();
      }
      output += sumid + TAB;
      output += sumintnum5 + TAB;
      output += sumintnum100 + TAB;
      output += sumintnum + TAB;
      output += sumlongnum + TAB;
      output += sumfloatnum + TAB;
      output += sumdoublenum + TAB;
      context.write(dummyLong, new Text(output));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    args = new GenericOptionsParser(conf, args).getRemainingArgs();
    String[] otherArgs = new String[4];
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
    if (otherArgs.length != 4) {
      System.err.println("Usage: hadoop jar sumnumbers <serveruri> <output dir> <-libjars hive-hcat jar>\n" +
        "The <tab|ctrla> argument controls the output delimiter.\n" +
        "The hcat jar location should be specified as file://<full path to jar>\n");
      System.exit(2);
    }
    String serverUri = otherArgs[0];
    String tableName = NUMBERS_TABLE_NAME;
    String outputDir = otherArgs[1];
    String dbName = "default";

    String principalID = System.getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
    if (principalID != null)
      conf.set(HCatConstants.HCAT_METASTORE_PRINCIPAL, principalID);
    Job job = new Job(conf, "sumnumbers");
    HCatInputFormat.setInput(job,
      dbName, tableName);
    // initialize HCatOutputFormat

    job.setInputFormatClass(HCatInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setJarByClass(SumNumbers.class);
    job.setMapperClass(SumMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(ArrayWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class ArrayWritable implements Writable {

    // though id is given as a Short by hcat, the map will emit it as an
    // IntWritable so we can just sum in the reduce
    IntWritable id;

    // though intnum5 is handed as a Byte by hcat, the map() will emit it as
    // an IntWritable so we can just sum in the reduce
    IntWritable intnum5;

    IntWritable intnum100;
    IntWritable intnum;
    LongWritable longnum;
    FloatWritable floatnum;
    DoubleWritable doublenum;

    /**
     *
     */
    public ArrayWritable() {
      id = new IntWritable();
      intnum5 = new IntWritable();
      intnum100 = new IntWritable();
      intnum = new IntWritable();
      longnum = new LongWritable();
      floatnum = new FloatWritable();
      doublenum = new DoubleWritable();
    }


    /**
     * @param id
     * @param intnum5
     * @param intnum100
     * @param intnum
     * @param longnum
     * @param floatnum
     * @param doublenum
     */
    public ArrayWritable(IntWritable id, IntWritable intnum5,
               IntWritable intnum100, IntWritable intnum, LongWritable longnum,
               FloatWritable floatnum, DoubleWritable doublenum) {
      this.id = id;
      this.intnum5 = intnum5;
      this.intnum100 = intnum100;
      this.intnum = intnum;
      this.longnum = longnum;
      this.floatnum = floatnum;
      this.doublenum = doublenum;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
      id.readFields(in);
      intnum5.readFields(in);
      intnum100.readFields(in);
      intnum.readFields(in);
      longnum.readFields(in);
      floatnum.readFields(in);
      doublenum.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      id.write(out);
      intnum5.write(out);
      intnum100.write(out);
      intnum.write(out);
      longnum.write(out);
      floatnum.write(out);
      doublenum.write(out);

    }

  }
}
