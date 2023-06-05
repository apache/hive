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
import liquibase.exception.LiquibaseException;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.io.PrintWriter;
import java.util.Set;

/**
 * This task utilizes {@link Liquibase} to init or upgrade the HMS schema. From Liquibase perspective both operations are 
 * the same.
 */
class LiquibaseUpdateTask extends SchemaToolTask {

  @Override
  protected Set<String> usedCommandLineArguments() {
    return Sets.newHashSet("dryRun");
  }

  @Override
  protected void execute(TaskContext context) throws HiveMetaException {
    try {
      Liquibase liquibase = context.getLiquibase();
      Contexts liquibaseContexts = context.getLiquibaseContext();

      liquibase.getLog().info("Starting metastore schema upgrade to latest version");

      if (context.getCommandLine().hasOption("dryRun")) {
        liquibase.update(liquibaseContexts, new PrintWriter(System.out));
      } else {
        liquibase.update(liquibaseContexts);
      }

      liquibase.getLog().info("Metastore schema upgraded to latest version");
    } catch (LiquibaseException e) {
      throw new HiveMetaException(TASK_FAIL_ERROR_MESSAGE, e);
    }
  }

}