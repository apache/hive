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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.LlapRecordReader;
import org.apache.hadoop.hive.metastore.api.Schema;

public class LlapDump {

  private static final Logger LOG = LoggerFactory.getLogger(LlapDump.class);

  private static String url = "jdbc:hive2://localhost:10000/default";
  private static String user = "hive";
  private static String pwd = "";
  private static String query = "select * from test";

  public static void main(String[] args) throws Exception {
    Options opts = createOptions();
    CommandLine cli = new GnuParser().parse(opts, args);

    if (cli.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("orcfiledump", opts);
      return;
    }

    if (cli.hasOption('l')) {
      url = cli.getOptionValue("l");
    }

    if (cli.hasOption('u')) {
      user = cli.getOptionValue("u");
    }

    if (cli.hasOption('p')) {
      pwd = cli.getOptionValue("p");
    }

    if (cli.getArgs().length > 0) {
      query = cli.getArgs()[0];
    }

    System.out.println("url: "+url);
    System.out.println("user: "+user);
    System.out.println("query: "+query);

    LlapInputFormat format = new LlapInputFormat(url, user, pwd, query);
    JobConf job = new JobConf();
    InputSplit[] splits = format.getSplits(job, 1);
    RecordReader<NullWritable, Text> reader = format.getRecordReader(splits[0], job, null);

    if (reader instanceof LlapRecordReader) {
      Schema schema = ((LlapRecordReader)reader).getSchema();
      System.out.println(""+schema);
    }
    System.out.println("Results: ");
    System.out.println("");

    Text value = reader.createValue();
    while (reader.next(NullWritable.get(), value)) {
      System.out.println(value);
    }
  }

  static Options createOptions() {
    Options result = new Options();

    result.addOption(OptionBuilder
        .withLongOpt("location")
        .withDescription("HS2 url")
	.hasArg()
        .create('l'));

    result.addOption(OptionBuilder
        .withLongOpt("user")
        .withDescription("user name")
        .hasArg()
        .create('u'));

    result.addOption(OptionBuilder
        .withLongOpt("pwd")
        .withDescription("password")
        .hasArg()
        .create('p'));

    return result;
  }
}
