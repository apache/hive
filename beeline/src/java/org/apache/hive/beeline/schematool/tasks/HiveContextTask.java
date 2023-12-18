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
package org.apache.hive.beeline.schematool.tasks;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.CommandBuilder;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.util.Set;

/**
 * Sets up the {@link HiveSchemaInfo}, and {@link org.apache.hadoop.hive.metastore.tools.schematool.scriptexecution.ScriptExecutor} 
 * objects in the {@link TaskContext}. Required to be the parent of all Hive schema related {@link SchemaToolTask} implementations.
 */
class HiveContextTask extends SchemaToolTask {

  @Override
  protected Set<String> usedCommandLineArguments() {
    return null;
  }

  @Override
  protected void execute(TaskContext context) throws HiveMetaException {
    HiveSchemaHelper.MetaStoreConnectionInfo connectionInfo = context.getConnectionInfo(false);
    context.setSchemaInfo(new HiveSchemaInfo(context.getMetastoreHome(), connectionInfo, context.getConfiguration()));
    context.setScriptExecutor(new BeelineScriptExecutor(context.getParser(),
        new CommandBuilder(connectionInfo, null).setVerbose(context.getCommandLine().hasOption("verbose"))));
  }
}
