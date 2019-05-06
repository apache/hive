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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap;

import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;

/**
 * Utility to test query and data retrieval via the LLAP input format.
 * llapdump --hiveconf hive.zookeeper.quorum=localhost --hiveconf hive.zookeeper.client.port=2181\
 *   --hiveconf hive.llap.daemon.service.hosts=@llap_MiniLlapCluster 'select * from employee where employee_id &lt; 10'
 *
 */
public class LlapDump {

  private static final Logger LOG = LoggerFactory.getLogger(LlapDump.class);

  private static String url = "jdbc:hive2://localhost:10000/default";
  private static String user = "hive";
  private static String pwd = "";
  private static String query = null;
  private static String numSplits = "1";

  public static void main(String[] args) throws Exception {
    Options opts = createOptions();
    CommandLine cli = new GnuParser().parse(opts, args);

    if (cli.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("llapdump", opts);
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

    if (cli.hasOption('n')) {
      numSplits = cli.getOptionValue("n");
    }

    Properties configProps = cli.getOptionProperties("hiveconf");

    if (cli.getArgs().length > 0) {
      query = cli.getArgs()[0];
    }

    if (query == null) {
      throw new IllegalArgumentException("No query string specified");
    }

    System.out.println("url: "+url);
    System.out.println("user: "+user);
    System.out.println("query: "+query);

    LlapRowInputFormat format = new LlapRowInputFormat();

    JobConf job = new JobConf();
    job.set(LlapBaseInputFormat.URL_KEY, url);
    job.set(LlapBaseInputFormat.USER_KEY, user);
    job.set(LlapBaseInputFormat.PWD_KEY, pwd);
    job.set(LlapBaseInputFormat.QUERY_KEY, query);

    // Additional conf settings specified on the command line
    for (String key: configProps.stringPropertyNames()) {
      job.set(key, configProps.getProperty(key));
    }

    InputSplit[] splits = format.getSplits(job, Integer.parseInt(numSplits));

    if (splits.length == 0) {
      System.out.println("No splits returned - empty scan");
      System.out.println("Results: ");
    } else {
      boolean first = true;

      for (InputSplit s: splits) {
        LOG.info("Processing input split s from " + Arrays.toString(s.getLocations()));
        RecordReader<NullWritable, Row> reader = format.getRecordReader(s, job, null);

        if (reader instanceof LlapRowRecordReader && first) {
          Schema schema = ((LlapRowRecordReader)reader).getSchema();
          System.out.println(""+schema);
        }

        if (first) {
          System.out.println("Results: ");
          System.out.println("");
          first = false;
        }

        Row value = reader.createValue();
        while (reader.next(NullWritable.get(), value)) {
          printRow(value);
        }
      }
      System.exit(0);
    }
  }

  private static void printRow(Row row) {
    Schema schema = row.getSchema();
    StringBuilder sb = new StringBuilder();
    int length = schema.getColumns().size();
    for (int idx = 0; idx < length; ++idx) {
      sb.append(row.getValue(idx));
      if (idx != length - 1) {
        sb.append(", ");
      }
    }
    System.out.println(sb.toString());
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

    result.addOption(OptionBuilder
        .withLongOpt("num")
        .withDescription("number of splits")
        .hasArg()
        .create('n'));

    result.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("property=value")
        .withLongOpt("hiveconf")
        .withDescription("Use value for given property")
        .create());

    result.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("help")
        .hasArg(false)
        .create('h'));

    return result;
  }
}
