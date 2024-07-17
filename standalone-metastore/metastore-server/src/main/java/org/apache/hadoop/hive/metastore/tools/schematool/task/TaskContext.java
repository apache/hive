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

import liquibase.Contexts;
import liquibase.Liquibase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParser;
import org.apache.hadoop.hive.metastore.SchemaInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.scriptexecution.ScriptExecutor;

import java.sql.Connection;

/**
 * Contains all the contaxtual information which may need by the {@link SchemaToolTask} subclasses in order to execute
 * their job. (for example: {@link SchemaToolCommandLine}, {@link org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.MetaStoreConnectionInfo},
 * {@link Liquibase} instance, etc.)
 */
public class TaskContext {

  private final String metastoreHome;
  private final Configuration configuration;
  private final SchemaToolCommandLine commandLine;

  private HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo;
  private NestedScriptParser parser;
  private ScriptExecutor scriptExecutor;
  private SchemaInfo schemaInfo;

  private Liquibase liquibase;
  private Contexts liquibaseContext;

  public String getMetastoreHome() {
    return metastoreHome;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public SchemaToolCommandLine getCommandLine() {
    return commandLine;
  }

  public void setConnectionInfo(HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo) {
    this.connectionInfo = connectionInfo;
  }

  public HiveSchemaHelper.MetaStoreConnectionInfo getConnectionInfo(boolean printInfo) {
    return new HiveSchemaHelper.MetaStoreConnectionInfo(
        connectionInfo.getUsername(), connectionInfo.getPassword(), connectionInfo.getUrl(), connectionInfo.getDriver(),
        printInfo, connectionInfo.getDbType()
    );
  }

  public void setSchemaInfo(SchemaInfo schemaInfo) {
    this.schemaInfo = schemaInfo;
  }

  public SchemaInfo getSchemaInfo() {
    return schemaInfo;
  }

  public void setParser(NestedScriptParser parser) {
    this.parser = parser;
  }

  public NestedScriptParser getParser() {
    return parser;
  }

  public void setScriptExecutor(ScriptExecutor scriptExecutor) {
    this.scriptExecutor = scriptExecutor;
  }

  public ScriptExecutor getScriptExecutor() {
    return scriptExecutor;
  }

  public Liquibase getLiquibase() {
    return liquibase;
  }

  public void setLiquibase(Liquibase liquibase) {
    this.liquibase = liquibase;
  }

  public Contexts getLiquibaseContext() {
    return liquibaseContext;
  }

  public void setLiquibaseContext(Contexts liquibaseContext) {
    this.liquibaseContext = liquibaseContext;
  }

  public Connection getConnectionToMetastore(boolean printInfo) throws HiveMetaException {
    HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo = getConnectionInfo(printInfo);
    String schema = ( HiveSchemaHelper.DB_HIVE.equals(connectionInfo.getDbType()) ? "SYS" : null );

    return HiveSchemaHelper.getConnectionToMetastore(connectionInfo, configuration, schema);
  }

  public TaskContext(String metastoreHome, Configuration configuration, SchemaToolCommandLine commandLine) {
    this.metastoreHome = metastoreHome;
    this.configuration = configuration;
    this.commandLine = commandLine;
  }
}
