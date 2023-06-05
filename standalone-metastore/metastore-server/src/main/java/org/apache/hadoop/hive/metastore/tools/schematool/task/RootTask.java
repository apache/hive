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
package org.apache.hadoop.hive.metastore.tools.schematool.task;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParserFactory;

import java.io.IOException;
import java.util.Set;

/**
 * The {@link RootTask} is responsible for setting up the common components in the {@link TaskContext} object.
 * ({@link org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.MetaStoreConnectionInfo},
 * {@link org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParser})
 */
public class RootTask extends SchemaToolTask {

  private static final Set<String> VALID_DB_TYPES = ImmutableSet.of(HiveSchemaHelper.DB_DERBY,
      HiveSchemaHelper.DB_HIVE, HiveSchemaHelper.DB_MSSQL, HiveSchemaHelper.DB_MYSQL,
      HiveSchemaHelper.DB_POSTGRES, HiveSchemaHelper.DB_ORACLE);

  private static final Set<String> VALID_META_DB_TYPES = ImmutableSet.of(HiveSchemaHelper.DB_DERBY,
      HiveSchemaHelper.DB_MSSQL, HiveSchemaHelper.DB_MYSQL, HiveSchemaHelper.DB_POSTGRES,
      HiveSchemaHelper.DB_ORACLE);
  private static final String USER_NAME = "userName";
  private static final String PASS_WORD = "passWord";
  private static final String URL = "url";
  private static final String DRIVER = "driver";

  private final NestedScriptParserFactory scriptParserFactory;
  

  public Set<String> usedCommandLineArguments() {
    return Sets.newHashSet("dbType", "metaDbType", "dryRun", "verbose", USER_NAME, PASS_WORD, URL, DRIVER);
  }

  @Override
  @SuppressWarnings("squid:S2095")
  protected void execute(TaskContext context) throws HiveMetaException {
    SchemaToolCommandLine commandLine = context.getCommandLine();
    Configuration conf = context.getConfiguration();
    String dbType = commandLine.getDbType();
    String metaDbType = commandLine.getMetaDbType();

    if (!VALID_DB_TYPES.contains(dbType)) {
      throw new HiveMetaException("Unsupported dbType " + dbType);
    }
    if (metaDbType != null) {
      if (!dbType.equals(HiveSchemaHelper.DB_HIVE)) {
        throw new HiveMetaException("metaDbType may only be set if dbType is hive");
      }
      if (!VALID_META_DB_TYPES.contains(metaDbType)) {
        throw new HiveMetaException("Unsupported metaDbType " + metaDbType);
      }
    } else if (dbType.equalsIgnoreCase(HiveSchemaHelper.DB_HIVE)) {
      throw new HiveMetaException("metaDbType must be set if dbType is hive");
    }

    String url, driver, userName, passWord;

    // If the dbType is "hive", this is setting up the information schema in Hive.
    // We will set the default jdbc url and driver.
    // It is overridden by command line options if passed (-url and -driver)
    if (dbType.equalsIgnoreCase(HiveSchemaHelper.DB_HIVE)) {
      url = HiveSchemaHelper.EMBEDDED_HS2_URL;
      driver = HiveSchemaHelper.HIVE_JDBC_DRIVER;
    } else {
      try {
        url = HiveSchemaHelper.getValidConfVar(MetastoreConf.ConfVars.CONNECT_URL_KEY, conf);
        driver = HiveSchemaHelper.getValidConfVar(MetastoreConf.ConfVars.CONNECTION_DRIVER, conf);
      } catch (IOException e) {
        throw new HiveMetaException("Error getting metastore url and driver", e);
      }
    }
    if (commandLine.hasOption(URL)) {
      url = commandLine.getOptionValue(URL);
    }
    if (commandLine.hasOption(DRIVER)) {
      driver = commandLine.getOptionValue(DRIVER);
    }

    userName = commandLine.hasOption(USER_NAME)
        ? commandLine.getOptionValue(USER_NAME)
        : MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.CONNECTION_USER_NAME);

    try {
      passWord = commandLine.hasOption(PASS_WORD)
          ? commandLine.getOptionValue(PASS_WORD)
          : MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.PWD);
    } catch (IOException err) {
      throw new HiveMetaException("Error getting metastore password", err);
    }

    LOG.info("DRYRUN: " + context.getCommandLine().hasOption("dryRun"));

    HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo = new HiveSchemaHelper.MetaStoreConnectionInfo(
        userName, passWord, url, driver, false, commandLine.getDbType());
    context.setConnectionInfo(connectionInfo);
    context.setParser(scriptParserFactory.getNestedScriptParser(dbType, commandLine.getOptionValue("dbOpts"), commandLine.getMetaDbType()));
  }

  public RootTask(NestedScriptParserFactory scriptParserFactory) {
    this.scriptParserFactory = scriptParserFactory;
  }
}
