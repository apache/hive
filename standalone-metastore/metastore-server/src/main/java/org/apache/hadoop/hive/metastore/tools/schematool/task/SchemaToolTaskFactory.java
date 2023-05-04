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
package org.apache.hadoop.hive.metastore.tools.schematool.task;

import org.apache.commons.cli.Option;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParserFactory;

import java.util.Set;

/**
 * Responsible for creating the {@link SchemaToolTask} chain based on the input provided via the {@link SchemaToolCommandLine}
 * object
 */
public class SchemaToolTaskFactory {

  private final SchemaToolTaskProvider[] providers;
  private final NestedScriptParserFactory scriptParserFactory;

  /**
   * Creates the {@link SchemaToolTask} chain based on the input provided via the {@link SchemaToolCommandLine}.
   * Checks for dangling command line arguments (arguments which are not used by any of the tasks in the created task chain).
   * @param cmdLine A {@link SchemaToolCommandLine} instance containing the parsed command line arguments.
   * @return Returns with the created {@link SchemaToolTask} chain.
   * @throws HiveMetaException Thrown in the following cases:
   * <ul>
   *   <li>Dangling arguments found</li>
   *   <li>No task found for the given command line arguments</li>
   *   <li>None of the available {@link SchemaToolTaskProvider} instances are supporting the database type provided via the
   *   '-dbType' option. For list of supported DB types see: {@link org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper} constants</li>
   * </ul>
   */
  public SchemaToolTask getTask(SchemaToolCommandLine cmdLine) throws HiveMetaException {
    for(SchemaToolTaskProvider provider : providers) {
      if(!provider.getSupportedDatabases().contains(cmdLine.getDbType().toLowerCase())) {
        continue;
      }
      SchemaToolTask rootTask = new RootTask(scriptParserFactory);
      for (Option command : cmdLine.getOptions()) {
        SchemaToolTask task = provider.getTask(command.getOpt());
        if (task != null) {
          rootTask.addChild(task);
          //Check for unused command line arguments
          Set<String> arguments = rootTask.getUsedCommandLineArguments();
          arguments.add(command.getOpt());
          arguments = cmdLine.getDanglingArguments(arguments);
          if (arguments.size() > 0) {
            throw new HiveMetaException("The following arguments are not used by the given command (" + command + "): " + String.join(",", arguments));
          }
          return rootTask;
        }
      }
      throw new HiveMetaException("No task defined for the given command and database type pair!");
    }
    throw new HiveMetaException("No SchemaToolTaskProvider found for database type: " + cmdLine.getDbType());
  }

  public SchemaToolTaskFactory(NestedScriptParserFactory scriptParserFactory,  SchemaToolTaskProvider... provider) {
    this.scriptParserFactory = scriptParserFactory;
    this.providers = provider;
  }

}
