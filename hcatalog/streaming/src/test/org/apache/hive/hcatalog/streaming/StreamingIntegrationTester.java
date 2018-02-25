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
package org.apache.hive.hcatalog.streaming;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.util.StringUtils;

import java.util.Arrays;
import java.util.Random;

/**
 * A stand alone utility to write data into the streaming ingest interface.
 */
public class StreamingIntegrationTester {

  static final private Logger LOG = LoggerFactory.getLogger(StreamingIntegrationTester.class.getName());

  public static void main(String[] args) {

    try {
      LogUtils.initHiveLog4j();
    } catch (LogUtils.LogInitializationException e) {
      System.err.println("Unable to initialize log4j " + StringUtils.stringifyException(e));
      System.exit(-1);
    }

    Options options = new Options();

    options.addOption(OptionBuilder
      .hasArg()
      .withArgName("abort-pct")
      .withDescription("Percentage of transactions to abort, defaults to 5")
      .withLongOpt("abortpct")
      .create('a'));

    options.addOption(OptionBuilder
      .hasArgs()
      .withArgName("column-names")
      .withDescription("column names of table to write to")
      .withLongOpt("columns")
      .withValueSeparator(',')
      .isRequired()
      .create('c'));

    options.addOption(OptionBuilder
      .hasArg()
      .withArgName("database")
      .withDescription("Database of table to write to")
      .withLongOpt("database")
      .isRequired()
      .create('d'));

    options.addOption(OptionBuilder
      .hasArg()
      .withArgName("frequency")
      .withDescription("How often to commit a transaction, in seconds, defaults to 1")
      .withLongOpt("frequency")
      .create('f'));

    options.addOption(OptionBuilder
      .hasArg()
      .withArgName("iterations")
      .withDescription("Number of batches to write, defaults to 10")
      .withLongOpt("num-batches")
      .create('i'));

    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("metastore-uri")
        .withDescription("URI of Hive metastore")
        .withLongOpt("metastore-uri")
        .isRequired()
        .create('m'));

    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("num_transactions")
        .withDescription("Number of transactions per batch, defaults to 100")
        .withLongOpt("num-txns")
        .create('n'));

    options.addOption(OptionBuilder
         .hasArgs()
         .withArgName("partition-values")
         .withDescription("partition values, must be provided in order of partition columns, " +
             "if not provided table is assumed to not be partitioned")
         .withLongOpt("partition")
         .withValueSeparator(',')
         .create('p'));

    options.addOption(OptionBuilder
         .hasArg()
         .withArgName("records-per-transaction")
         .withDescription("records to write in each transaction, defaults to 100")
         .withLongOpt("records-per-txn")
         .withValueSeparator(',')
         .create('r'));

    options.addOption(OptionBuilder
         .hasArgs()
         .withArgName("column-types")
         .withDescription("column types, valid values are string, int, float, decimal, date, " +
             "datetime")
         .withLongOpt("schema")
         .withValueSeparator(',')
         .isRequired()
         .create('s'));

    options.addOption(OptionBuilder
      .hasArg()
      .withArgName("table")
      .withDescription("Table to write to")
      .withLongOpt("table")
      .isRequired()
      .create('t'));

    options.addOption(OptionBuilder
      .hasArg()
      .withArgName("num-writers")
      .withDescription("Number of writers to create, defaults to 2")
      .withLongOpt("writers")
      .create('w'));

    options.addOption(OptionBuilder
            .hasArg(false)
            .withArgName("pause")
            .withDescription("Wait on keyboard input after commit & batch close. default: disabled")
            .withLongOpt("pause")
            .create('x'));


    Parser parser = new GnuParser();
    CommandLine cmdline = null;
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      usage(options);
    }

    boolean pause = cmdline.hasOption('x');
    String db = cmdline.getOptionValue('d');
    String table = cmdline.getOptionValue('t');
    String uri = cmdline.getOptionValue('m');
    int txnsPerBatch = Integer.parseInt(cmdline.getOptionValue('n', "100"));
    int writers = Integer.parseInt(cmdline.getOptionValue('w', "2"));
    int batches = Integer.parseInt(cmdline.getOptionValue('i', "10"));
    int recordsPerTxn = Integer.parseInt(cmdline.getOptionValue('r', "100"));
    int frequency = Integer.parseInt(cmdline.getOptionValue('f', "1"));
    int ap = Integer.parseInt(cmdline.getOptionValue('a', "5"));
    float abortPct = ((float)ap) / 100.0f;
    String[] partVals = cmdline.getOptionValues('p');
    String[] cols = cmdline.getOptionValues('c');
    String[] types = cmdline.getOptionValues('s');

    StreamingIntegrationTester sit = new StreamingIntegrationTester(db, table, uri,
        txnsPerBatch, writers, batches, recordsPerTxn, frequency, abortPct, partVals, cols, types
            , pause);
    sit.go();
  }

  static void usage(Options options) {
    HelpFormatter hf = new HelpFormatter();
    hf.printHelp(HelpFormatter.DEFAULT_WIDTH, "sit [options]", "Usage: ", options, "");
    System.exit(-1);
  }

  private String db;
  private String table;
  private String uri;
  private int txnsPerBatch;
  private int writers;
  private int batches;
  private int recordsPerTxn;
  private int frequency;
  private float abortPct;
  private String[] partVals;
  private String[] cols;
  private String[] types;
  private boolean pause;


  private StreamingIntegrationTester(String db, String table, String uri, int txnsPerBatch,
                                     int writers, int batches,  int recordsPerTxn,
                                     int frequency, float abortPct, String[] partVals,
                                     String[] cols, String[] types, boolean pause) {
    this.db = db;
    this.table = table;
    this.uri = uri;
    this.txnsPerBatch = txnsPerBatch;
    this.writers = writers;
    this.batches = batches;
    this.recordsPerTxn = recordsPerTxn;
    this.frequency = frequency;
    this.abortPct = abortPct;
    this.partVals = partVals;
    this.cols = cols;
    this.types = types;
    this.pause = pause;
  }

  private void go() {
    HiveEndPoint endPoint = null;
    try {
      if (partVals == null) {
        endPoint = new HiveEndPoint(uri, db, table, null);
      } else {
        endPoint = new HiveEndPoint(uri, db, table, Arrays.asList(partVals));
      }

      for (int i = 0; i < writers; i++) {
        Writer w = new Writer(endPoint, i, txnsPerBatch, batches, recordsPerTxn, frequency, abortPct,
                cols, types, pause);
        w.start();
      }

    } catch (Throwable t) {
      System.err.println("Caught exception while testing: " + StringUtils.stringifyException(t));
    }
  }

  private static class Writer extends Thread {
    private HiveEndPoint endPoint;
    private int txnsPerBatch;
    private int batches;
    private int writerNumber;
    private int recordsPerTxn;
    private int frequency;
    private float abortPct;
    private String[] cols;
    private String[] types;
    private boolean pause;
    private Random rand;

    Writer(HiveEndPoint endPoint, int writerNumber, int txnsPerBatch, int batches,
           int recordsPerTxn, int frequency, float abortPct, String[] cols, String[] types
            , boolean pause) {
      this.endPoint = endPoint;
      this.txnsPerBatch = txnsPerBatch;
      this.batches = batches;
      this.writerNumber = writerNumber;
      this.recordsPerTxn = recordsPerTxn;
      this.frequency = frequency * 1000;
      this.abortPct = abortPct;
      this.cols = cols;
      this.types = types;
      this.pause = pause;
      rand = new Random();
    }

    @Override
    public void run() {
      StreamingConnection conn = null;
      try {
        conn = endPoint.newConnection(true, "UT_" + Thread.currentThread().getName());
        RecordWriter writer = new DelimitedInputWriter(cols, ",", endPoint);

        for (int i = 0; i < batches; i++) {
          long start = System.currentTimeMillis();
          LOG.info("Starting batch " + i);
          TransactionBatch batch = conn.fetchTransactionBatch(txnsPerBatch, writer);
          try {
            while (batch.remainingTransactions() > 0) {
              batch.beginNextTransaction();
              for (int j = 0; j < recordsPerTxn; j++) {
                batch.write(generateRecord(cols, types));
              }
              if (rand.nextFloat() < abortPct) batch.abort();
              else
              batch.commit();
              if (pause) {
                System.out.println("Writer " + writerNumber +
                        " committed... press Enter to continue. " + Thread.currentThread().getId());
                System.in.read();
              }
            }
            long end = System.currentTimeMillis();
            if (end - start < frequency) Thread.sleep(frequency - (end - start));
          } finally {
            batch.close();
            if (pause) {
              System.out.println("Writer " + writerNumber +
                  " has closed a Batch.. press Enter to continue. " + Thread.currentThread().getId());
              System.in.read();
            }
          }
        }
      } catch (Throwable t) {
       System.err.println("Writer number " + writerNumber
               + " caught exception while testing: " + StringUtils.stringifyException(t));
      } finally {
        if (conn!=null) conn.close();
      }
    }

    private byte[] generateRecord(String[] cols, String[] types) {
      // TODO make it so I can randomize the column order

      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < types.length; i++) {
        buf.append(generateColumn(types[i]));
        buf.append(",");
      }
      return buf.toString().getBytes();
    }

    private String generateColumn(String type) {
      if ("string".equals(type.toLowerCase())) {
        return  "When that Aprilis with his showers swoot";
      } else if (type.toLowerCase().startsWith("int")) {
        return "42";
      } else if (type.toLowerCase().startsWith("dec") || type.toLowerCase().equals("float")) {
        return "3.141592654";
      } else if (type.toLowerCase().equals("datetime")) {
        return "2014-03-07 15:33:22";
      } else if (type.toLowerCase().equals("date")) {
        return "1955-11-12";
      } else {
        throw new RuntimeException("Sorry, I don't know the type " + type);
      }
    }
  }
}
