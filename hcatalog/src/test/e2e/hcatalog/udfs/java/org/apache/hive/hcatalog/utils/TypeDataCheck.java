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
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

/**
 * This is a map reduce test for testing hcat that checks that the columns
 * handed by hcat have the right type and right values. It achieves the first
 * objective by checking the type of the Objects representing the columns against
 * the schema provided as a cmdline arg. It achieves the second objective by
 * writing the data as Text to be compared against golden results.
 *
 * The schema specification consists of the types as given by "describe <table>"
 * with each column's type separated from the next column's type by a '+'
 *
 * Can be used against "numbers" and "complex" tables.
 *
 * Usage: hadoop jar testudf.jar typedatacheck <serveruri> <tablename> 
 * <hive types of cols + delimited> <output dir> <tab|ctrla> <-libjars hive-hcat jar>
 The <tab|ctrla> argument controls the output delimiter.
 The hcat jar location should be specified as file://<full path to jar>
 */
public class TypeDataCheck implements Tool {

  static String SCHEMA_KEY = "schema";
  static String DELIM = "delim";
  private static Configuration conf = new Configuration();

  public static class TypeDataCheckMapper
    extends Mapper<WritableComparable, HCatRecord, Long, Text> {

    Long dummykey = null;
    String[] types;
    String delim = "\u0001";

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, Long, Text>.Context context)
      throws IOException, InterruptedException {
      String typesStr = context.getConfiguration().get(SCHEMA_KEY);
      delim = context.getConfiguration().get(DELIM);
      if (delim.equals("tab")) {
        delim = "\t";
      } else if (delim.equals("ctrla")) {
        delim = "\u0001";
      }
      types = typesStr.split("\\+");
      for (int i = 0; i < types.length; i++) {
        types[i] = types[i].toLowerCase();
      }


    }

    String check(HCatRecord r) throws IOException {
      String s = "";
      for (int i = 0; i < r.size(); i++) {
        s += Util.check(types[i], r.get(i));
        if (i != r.size() - 1) {
          s += delim;
        }
      }
      return s;
    }

    @Override
    protected void map(WritableComparable key, HCatRecord value,
               org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, Long, Text>.Context context)
      throws IOException, InterruptedException {
      context.write(dummykey, new Text(check(value)));
    }
  }

  public static void main(String[] args) throws Exception {
    TypeDataCheck self = new TypeDataCheck();
    System.exit(ToolRunner.run(conf, self, args));
  }

  public int run(String[] args) {
    try {
      args = new GenericOptionsParser(conf, args).getRemainingArgs();
      String[] otherArgs = new String[5];
      int j = 0;
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-libjars")) {
          conf.set("tmpjars", args[i + 1]);
          i = i + 1; // skip it , the for loop will skip its value
        } else {
          otherArgs[j++] = args[i];
        }
      }
      if (otherArgs.length != 5) {
        System.err.println("Other args:" + Arrays.asList(otherArgs));
        System.err.println("Usage: hadoop jar testudf.jar typedatacheck " +
          "<serveruri> <tablename> <hive types of cols + delimited> " +
          "<output dir> <tab|ctrla> <-libjars hive-hcat jar>\n" +
          "The <tab|ctrla> argument controls the output delimiter.\n" +
          "The hcat jar location should be specified as file://<full path to jar>\n");
        System.err.println(" The <tab|ctrla> argument controls the output delimiter.");
        System.exit(2);
      }
      String serverUri = otherArgs[0];
      String tableName = otherArgs[1];
      String schemaStr = otherArgs[2];
      String outputDir = otherArgs[3];
      String outputdelim = otherArgs[4];
      if (!outputdelim.equals("tab") && !outputdelim.equals("ctrla")) {
        System.err.println("ERROR: Specify 'tab' or 'ctrla' for output delimiter");
      }
      String dbName = "default";

      String principalID = System.getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
      if (principalID != null) {
        conf.set(HCatConstants.HCAT_METASTORE_PRINCIPAL, principalID);
      }
      Job job = new Job(conf, "typedatacheck");
      // initialize HCatInputFormat
      HCatInputFormat.setInput(job,
        dbName, tableName);
      HCatSchema s = HCatInputFormat.getTableSchema(job);
      job.getConfiguration().set(SCHEMA_KEY, schemaStr);
      job.getConfiguration().set(DELIM, outputdelim);
      job.setInputFormatClass(HCatInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setJarByClass(TypeDataCheck.class);
      job.setMapperClass(TypeDataCheckMapper.class);
      job.setNumReduceTasks(0);
      job.setOutputKeyClass(Long.class);
      job.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(job, new Path(outputDir));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
      return 0;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    TypeDataCheck.conf = conf;
  }

}
