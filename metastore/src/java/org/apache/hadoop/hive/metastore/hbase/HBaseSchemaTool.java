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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * A tool to dump contents from the HBase store in a human readable form
 */
public class HBaseSchemaTool {

  private static String[] commands = {"db", "part", "parts", "role", "table", "function",
                                      "install"};

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder
        .withLongOpt("column")
        .withDescription("Comma separated list of column names")
        .hasArg()
        .create('c'));

    options.addOption(OptionBuilder
        .withLongOpt("db")
        .withDescription("Database name")
        .hasArg()
        .create('d'));

    options.addOption(OptionBuilder
        .withLongOpt("function")
        .withDescription("Function name")
        .hasArg()
        .create('f'));

    options.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("You're looking at it")
        .create('h'));

    options.addOption(OptionBuilder
        .withLongOpt("role")
        .withDescription("Role name")
        .hasArg()
        .create('r'));

    options.addOption(OptionBuilder
        .withLongOpt("partvals")
        .withDescription("Comma separated list of partition values, in order of partition columns")
        .hasArg()
        .create('p'));

    options.addOption(OptionBuilder
        .withLongOpt("stats")
        .withDescription("Get statistics rather than catalog object")
        .create('s'));

    options.addOption(OptionBuilder
        .withLongOpt("table")
        .withDescription("Table name")
        .hasArg()
        .create('t'));

    CommandLine cli = new GnuParser().parse(options, args);

    if (cli.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("hbaseschematool", options);
      return;
    }

    String[] cmds = cli.getArgs();
    if (cmds.length != 1) {
      System.err.print("Must include a cmd, valid cmds are: ");
      for (int i = 0; i < commands.length; i++) {
        if (i != 0) System.err.print(", ");
        System.err.print(commands[i]);
      }
      System.err.println();
      System.exit(1);
    }
    String cmd = cmds[0];

    List<String> parts = null;
    if (cli.hasOption('p')) {
      parts = Arrays.asList(cli.getOptionValue('p').split(","));
    }

    List<String> cols = null;
    if (cli.hasOption('c')) {
      cols = Arrays.asList(cli.getOptionValue('c').split(","));
    }

    HBaseSchemaTool tool = new HBaseSchemaTool(cli.getOptionValue('d'), cli.getOptionValue('t'),
        parts, cli.getOptionValue('f'), cli.getOptionValue('r'), cols, cli.hasOption('s'));
    Method method = tool.getClass().getMethod(cmd);
    method.invoke(tool);


  }

  private HBaseReadWrite hrw;
  private String dbName;
  private String funcName;
  private String tableName;
  private List<String> partVals;
  private String roleName;
  private List<String> colNames;
  private boolean hasStats;

  private HBaseSchemaTool(String dbname, String tn, List<String> pv, String fn, String rn,
                          List<String> cn, boolean s) {
    dbName = dbname;
    tableName = tn;
    partVals = pv;
    funcName = fn;
    roleName = rn;
    colNames = cn;
    hasStats = s;
    HBaseReadWrite.setConf(new Configuration());
    hrw = HBaseReadWrite.getInstance();
  }

  public void db() throws IOException, TException {
    Database db = hrw.getDb(dbName);
    if (db == null) System.err.println("No such database: " + db);
    else dump(db);
  }

  public void install() throws IOException {
    HBaseReadWrite.createTablesIfNotExist();
  }

  public void part() throws IOException, TException {
    if (hasStats) {
      Table table = hrw.getTable(dbName, tableName);
      if (table == null) {
        System.err.println("No such table: " + dbName + "." + tableName);
        return;
      }
      String partName = HBaseStore.buildExternalPartName(table, partVals);
      List<ColumnStatistics> stats = hrw.getPartitionStatistics(dbName, tableName,
          Arrays.asList(partName), Arrays.asList(partVals), colNames);
      if (stats == null) {
        System.err.println("No stats for " + dbName + "." + tableName + "." +
            StringUtils.join(partVals, ':'));
      } else {
        for (ColumnStatistics stat : stats) dump(stat);
      }
    } else {
      Partition part = hrw.getPartition(dbName, tableName, partVals);
      if (part == null) {
        System.err.println("No such partition: " + dbName + "." + tableName + "." +
            StringUtils.join(partVals, ':'));
      } else {
        dump(part);
      }
    }
  }

  public void parts() throws IOException, TException {
    List<Partition> parts = hrw.scanPartitionsInTable(dbName, tableName, -1);
    if (parts == null) {
      System.err.println("No such table: " + dbName + "." + tableName);
    } else {
      for (Partition p : parts) dump(p);
    }
  }

  public void role() throws IOException, TException {
    Role role = hrw.getRole(roleName);
    if (role == null) System.err.println("No such role: " + roleName);
    else dump(role);
  }

  public void table() throws IOException, TException {
    if (hasStats) {
      ColumnStatistics stats = hrw.getTableStatistics(dbName, tableName, colNames);
      if (stats == null) System.err.println("No stats for " + dbName + "." + tableName);
      else dump(stats);
    } else {
      Table table = hrw.getTable(dbName, tableName);
      if (table == null) System.err.println("No such table: " + dbName + "." + tableName);
      else dump(table);
    }
  }

  public void function() throws IOException, TException {
    Function func = hrw.getFunction(dbName, funcName);
    if (func == null) System.err.println("No such function: " + dbName + "." + funcName);
    else dump(func);
  }

  private void dump(TBase thriftObj) throws TException {
    TMemoryBuffer buf = new TMemoryBuffer(1000);
    TProtocol protocol = new TSimpleJSONProtocol(buf);
    thriftObj.write(protocol);
    System.out.println(new String(buf.getArray()));
  }


}
