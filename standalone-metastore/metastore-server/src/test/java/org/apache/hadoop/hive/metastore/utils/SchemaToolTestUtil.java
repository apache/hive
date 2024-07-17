/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.CommandBuilder;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParserFactory;
import org.apache.hadoop.hive.metastore.tools.schematool.hms.EmbeddedTaskProvider;
import org.apache.hadoop.hive.metastore.tools.schematool.liquibase.LiquibaseTaskProvider;
import org.apache.hadoop.hive.metastore.tools.schematool.scriptexecution.ScriptExecutor;
import org.apache.hadoop.hive.metastore.tools.schematool.scriptexecution.SqlLineScriptExecutor;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTaskFactory;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

public class SchemaToolTestUtil {

  private static final SchemaToolTaskFactory taskFactory = new SchemaToolTaskFactory(
      new NestedScriptParserFactory(),
      new LiquibaseTaskProvider(new EmbeddedTaskProvider()));

  public static String[] buildArray(String... strs) {
    return Arrays.stream(strs).filter(Objects::nonNull).toArray(String[]::new);
  }

  @VisibleForTesting
  public static void runScript(HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo, InputStream scriptStream) throws IOException {
    // Cannot run script directly from input stream thus copy is necessary.
    File scriptFile = File.createTempFile("schemaToolTmpScript", "sql");
    scriptFile.deleteOnExit();
    FileUtils.copyToFile(scriptStream, scriptFile);
    new SqlLineScriptExecutor(new CommandBuilder(connectionInfo, new String[]{ "--isolation=" + ScriptExecutor.TRANSACTION_READ_COMMITTED })).execSql(scriptFile.getAbsolutePath());
  }

  public static void executeCommand(Configuration conf,
                                    String[] args) throws ParseException, HiveMetaException {
    executeCommand(System.getProperty("test.tmp.dir", "target/tmp"),  conf, args);
  }

  public static void executeCommand(String metastoreHome,
                                    Configuration conf,
                                    String[] args) throws ParseException, HiveMetaException {
    executeCommand(taskFactory, metastoreHome, conf, args);
  }

  public static void executeCommand(SchemaToolTaskFactory factory, String metastoreHome,
                                    Configuration conf,
                                    String[] args) throws ParseException, HiveMetaException {
    OptionGroup additionalGroup = new OptionGroup();
    Option metaDbTypeOpt = Option.builder("metaDbType")
        .argName("metaDatabaseType")
        .hasArgs()
        .desc("Used only if upgrading the system catalog for hive")
        .build();
    additionalGroup.addOption(metaDbTypeOpt);
    SchemaToolCommandLine commandLine = new SchemaToolCommandLine(args, additionalGroup);
    SchemaToolTask task = factory.getTask(commandLine);
    task.executeChain(new TaskContext(metastoreHome, conf, commandLine));
  }

}
