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
import liquibase.LabelExpression;
import liquibase.Labels;
import liquibase.Liquibase;
import liquibase.changelog.ChangeSetStatus;
import liquibase.exception.LiquibaseException;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.io.PrintWriter;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class LiquibaseUpdateToTask extends SchemaToolTask {

  @Override
  protected Set<String> usedCommandLineArguments() {
    return Sets.newHashSet("initSchemaTo");
  }

  @Override
  protected void execute(TaskContext context) throws HiveMetaException {
    try {
      String toVersion = context.getCommandLine().getOptionValue("initSchemaTo");

      Liquibase liquibase = context.getLiquibase();
      Contexts liquibaseContexts = context.getLiquibaseContext();
      
      liquibase.getLog().info("Starting metastore schema upgrade to version: " + toVersion);

      List<ChangeSetStatus> unappliedChanges = liquibase
          .getChangeSetStatuses(liquibaseContexts, null, false)
          .stream()
          .filter(s -> !s.getPreviouslyRan())
          .collect(Collectors.toList());

      StringBuilder logEntry = new StringBuilder("The following scripts will be applied: ");
      StringBuilder labelFilter = new StringBuilder();
      boolean foundToVersion = false;
      for (int i = 0; i < unappliedChanges.size(); i++) {
        logEntry.append(unappliedChanges.get(i).getChangeSet().getFilePath());

        Labels currentVerison = unappliedChanges.get(i).getChangeSet().getLabels();

        if (labelFilter.length() > 0) {
          labelFilter.append(" OR ");
          logEntry.append(", ");
        }

        labelFilter.append(String.join(" OR ", currentVerison.getLabels()));

        if (currentVerison.getLabels().contains(toVersion)) {
          foundToVersion = true;
          break;
        }
      }
      if (!foundToVersion) {
        throw new HiveMetaException("The required version (" + toVersion + ") could not be found among the version scripts!");
      }
      liquibase.getLog().info(logEntry.toString());

      if (context.getCommandLine().hasOption("dryRun")) {
        liquibase.update(liquibaseContexts, new LabelExpression(labelFilter.toString()), new PrintWriter(System.out));
      } else {
        liquibase.update(liquibaseContexts, new LabelExpression(labelFilter.toString()));
      }

      liquibase.getLog().info("Metastore schema upgraded to version: " + toVersion);
    } catch (LiquibaseException e) {
      throw new HiveMetaException("Schema upgrade FAILED! Metastore state would be inconsistent!", e);
    }
  }

}