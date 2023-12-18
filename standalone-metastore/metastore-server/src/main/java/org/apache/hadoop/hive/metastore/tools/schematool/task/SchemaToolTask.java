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

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base class of all Schema tool tasks. Subclasses must implement the {@link #execute(TaskContext)} and
 * {@link #usedCommandLineArguments()} methods. Can have child tasks which are executed after this task has been executed.
 * This allows to create task execution chains.
 */
public abstract class SchemaToolTask {

  protected static final Logger LOG = LoggerFactory.getLogger(SchemaToolTask.class);

  private final List<SchemaToolTask> children = new ArrayList<>();

  public final SchemaToolTask addChild(SchemaToolTask childTask) {
    children.add(childTask);
    return this;
  }

  protected final Iterable<SchemaToolTask> getChildren() {
    return children;
  }

  /**
   * @return Returns the name of command line arguments used by this task
   */
  protected abstract Set<String> usedCommandLineArguments();

  /**
   * Subclasses must implement this method to hold the code to be executed by this task.
   * @param context The {@link TaskContext} Contains all the necessary contextual objects which may reqired during task
   *                execution
   * @throws HiveMetaException Thrown if the task execution fails
   */
  protected abstract void execute(TaskContext context) throws HiveMetaException;

  /**
   * Executes this task, and all of its subtasks by calling the {@link #execute(TaskContext)} methods.
   * @param context The {@link TaskContext} Contains all the necessary contextual objects which may reqired during task
   *                execution
   * @throws HiveMetaException Thrown if the task execution fails
   */
  public void executeChain(TaskContext context) throws HiveMetaException {
    execute(context);
    for(SchemaToolTask child : children) {
      child.executeChain(context);
    }
  }

  /**
   * @return Returns the name of command line arguments used by this task and by its children
   */
  public final Set<String> getUsedCommandLineArguments() {
    Set<String> arguments = usedCommandLineArguments();
    if (arguments == null) {
      arguments = new HashSet<>();
    }
    for(SchemaToolTask child : children) {
      arguments.addAll(child.getUsedCommandLineArguments());
    }
    return arguments;
  }

  protected String quote(String stmt, NestedScriptParser parser) {
    stmt = stmt.replace("<q>", parser.needsQuotedIdentifier() ? parser.getQuoteCharacter() : "");
    stmt = stmt.replace("<qa>", parser.getQuoteCharacter());
    return stmt;
  }

}
