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
package org.apache.hadoop.hive.metastore.tools.schematool.liquibase;

import com.google.common.collect.Sets;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.DirectoryResourceAccessor;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.SchemaInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.CommandBuilder;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.tools.schematool.scriptexecution.SqlLineScriptExecutor;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

/**
 * Sets up the {@link Liquibase}, {@link Contexts} and {@link SqlLineScriptExecutor} objects in the {@link TaskContext}.
 * Required to be the parent of all liquibase related {@link SchemaToolTask} implementations. All child tasks are executed within the created liquibase
 * {@link Scope}.
 */
class LiquibaseContextTask extends SchemaToolTask {

  @Override
  public Set<String> usedCommandLineArguments() {
    return Sets.newHashSet("contexts");
  }

  @SuppressWarnings("squid:S2095")
  @Override
  protected void execute(TaskContext context) throws HiveMetaException {
    try {
      SchemaToolCommandLine commandLine = context.getCommandLine();
      HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo = context.getConnectionInfo(false);

      Contexts liquibaseContexts = new Contexts();
      if (commandLine.hasOption("contexts")) {
        Arrays.asList(commandLine.getOptionValue("contexts").split(",")).forEach(liquibaseContexts::add);
      }
      context.setLiquibaseContext(liquibaseContexts);

      context.setLiquibase(new Liquibase("changelog.xml",
          new DirectoryResourceAccessor(
              Paths.get(
                  context.getMetastoreHome() + File.separatorChar + "scripts" + File.separatorChar + "metastore" +
                      File.separatorChar + "upgrade"
              )
          ),
          DatabaseFactory.getInstance().findCorrectDatabaseImplementation(
              new JdbcConnection(context.getConnectionToMetastore(true)))
          )
      );
      context.setSchemaInfo(
          new LiquibaseSchemaInfo(
              context.getMetastoreHome(), connectionInfo, context.getConfiguration(), context.getLiquibase(), liquibaseContexts
          )
      );
      context.setScriptExecutor(
          new SqlLineScriptExecutor(
              new CommandBuilder(
                  connectionInfo,
                  new String[] {"--isolation=TRANSACTION_READ_COMMITTED"}).setVerbose(commandLine.hasOption("verbose")
              )
          )
      );
    } catch (HiveMetaException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveMetaException("Failed to initialize liquibase context", e);
    }
  }

  @Override
  public void executeChain(TaskContext context) throws HiveMetaException {
    try {
      Scope.child(new HashMap<>(), () -> {
        execute(context);
        for(SchemaToolTask child : getChildren()) {
          child.executeChain(context);
        }
      });
    } catch (HiveMetaException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveMetaException("Failed to execute liquibase task", e);
    }
  }
}