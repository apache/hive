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
package org.apache.hadoop.hive.metastore.hbase;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A tool to dump contents from the HBase store in a human readable form
 */
public class HBaseSchemaTool {

  public static void main(String[] args) {
    Options options = new Options();

    options.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("You're looking at it")
        .create('h'));

    options.addOption(OptionBuilder
        .withLongOpt("install")
        .withDescription("Install the schema onto an HBase cluster.")
        .create('i'));

    options.addOption(OptionBuilder
        .withLongOpt("key")
        .withDescription("Key to scan with.  This should be an exact key (not a regular expression")
        .hasArg()
        .create('k'));

    options.addOption(OptionBuilder
        .withLongOpt("list-tables")
        .withDescription("List tables in HBase metastore")
        .create('l'));

    options.addOption(OptionBuilder
        .withLongOpt("regex-key")
        .withDescription("Regular expression to scan keys with.")
        .hasArg()
        .create('r'));

    options.addOption(OptionBuilder
        .withLongOpt("table")
        .withDescription("HBase metastore table to scan")
        .hasArg()
        .create('t'));

    CommandLine cli = null;
    try {
      cli = new GnuParser().parse(options, args);
    } catch (ParseException e) {
      System.err.println("Parse Exception: " + e.getMessage());
      usage(options);
      return;
    }

    if (cli.hasOption('h')) {
      usage(options);
      return;
    }

    Configuration conf = new Configuration();

    if (cli.hasOption('i')) {
      new HBaseSchemaTool().install(conf, System.err);
      return;
    }

    String key = null;
    if (cli.hasOption('k')) key = cli.getOptionValue('k');
    String regex = null;
    if (cli.hasOption('r')) regex = cli.getOptionValue('r');
    if (key != null && regex != null) {
      usage(options);
      return;
    }
    if (key == null && regex == null) regex = ".*";

    // I do this in the object rather than in the static main so that it's easier to test.
    new HBaseSchemaTool().go(cli.hasOption('l'), cli.getOptionValue('t'), key, regex, conf,
        System.out, System.err);
  }

  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String header = "This tool dumps contents of your hbase metastore.  You need to specify\n" +
        "the table to dump.  You can optionally specify a regular expression on the key for\n" +
        "the table.  Keep in mind that the key is often a compound.  For partitions regular\n" +
        "expressions are not used because non-string values are\nstored in binary.  Instead for " +
        "partition you can specify as much of the exact prefix as you want.  So you can give " +
        "dbname.tablename or dbname.tablename.pval1...";
    String footer = "If neither key or regex is provided a regex of .* will be assumed.  You\n" +
        "cannot set both key and regex.";
    formatter.printHelp("hbaseschematool", header, options, footer);
    return;
  }

  @VisibleForTesting void go(boolean listTables, String table, String key, String regex,
                             Configuration conf, PrintStream out, PrintStream err) {
    List<String> lines = new ArrayList<>();
    if (listTables) {
      lines = Arrays.asList(HBaseReadWrite.tableNames);
    } else {
      // If they've used '.' as a key separator we need to replace it with the separator used by
      // HBaseUtils
      if (key != null) key = key.replace('.', HBaseUtils.KEY_SEPARATOR);
      try {
        HBaseReadWrite.setConf(conf);
        HBaseReadWrite hrw = HBaseReadWrite.getInstance();
        if (table.equalsIgnoreCase(HBaseReadWrite.DB_TABLE)) {
          if (key != null) lines.add(hrw.printDatabase(key));
          else lines.addAll(hrw.printDatabases(regex));
        } else if (table.equalsIgnoreCase(HBaseReadWrite.FUNC_TABLE)) {
          if (key != null) lines.add(hrw.printFunction(key));
          else lines.addAll(hrw.printFunctions(regex));
        } else if (table.equalsIgnoreCase(HBaseReadWrite.GLOBAL_PRIVS_TABLE)) {
          // Ignore whatever they passed, there's always only either one or zero global privileges
          lines.add(hrw.printGlobalPrivs());
        } else if (table.equalsIgnoreCase(HBaseReadWrite.PART_TABLE)) {
          if (key != null) lines.add(hrw.printPartition(key));
          else lines.addAll(hrw.printPartitions(regex));
        } else if (table.equalsIgnoreCase(HBaseReadWrite.USER_TO_ROLE_TABLE)) {
          if (key != null) lines.add(hrw.printRolesForUser(key));
          else lines.addAll(hrw.printRolesForUsers(regex));
        } else if (table.equalsIgnoreCase(HBaseReadWrite.ROLE_TABLE)) {
          if (key != null) lines.add(hrw.printRole(key));
          else lines.addAll(hrw.printRoles(regex));
        } else if (table.equalsIgnoreCase(HBaseReadWrite.TABLE_TABLE)) {
          if (key != null) lines.add(hrw.printTable(key));
          else lines.addAll(hrw.printTables(regex));
        } else if (table.equalsIgnoreCase(HBaseReadWrite.SD_TABLE)) {
          if (key != null) lines.add(hrw.printStorageDescriptor(Base64.decodeBase64(key)));
          else lines.addAll(hrw.printStorageDescriptors());
        } else if (table.equalsIgnoreCase(HBaseReadWrite.SECURITY_TABLE)) {
          // We always print all of security, we don't worry about finding particular entries.
          lines.addAll(hrw.printSecurity());
        } else if (table.equalsIgnoreCase(HBaseReadWrite.SEQUENCES_TABLE)) {
          // We always print all of sequences, we don't worry about finding particular entries.
          lines.addAll(hrw.printSequences());
        } else {
          err.println("Unknown table: " + table);
          return;
        }
      } catch (Exception e) {
        err.println("Caught exception " + e.getClass() + " with message: " + e.getMessage());
        return;
      }
    }
    for (String line : lines) out.println(line);
  }

  @VisibleForTesting void install(Configuration conf, PrintStream err) {
    try {
      // We need to set the conf because createTablesIfNotExist will get a thread local version
      // which requires that the configuration object be set.
      HBaseReadWrite.setConf(conf);
      HBaseReadWrite.createTablesIfNotExist();
    } catch (Exception e) {
      err.println("Caught exception " + e.getClass() + " with message: " + e.getMessage());
      return;
    }
  }
}
