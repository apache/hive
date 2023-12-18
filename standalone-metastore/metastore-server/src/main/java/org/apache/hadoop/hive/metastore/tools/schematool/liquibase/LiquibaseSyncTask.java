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
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Responsible for creating the baseline when liquibase is run on the schema for the first time.
 * Reads the DB version from the old versioning table, and initializes liquibase with it.
 */
class LiquibaseSyncTask extends SchemaToolTask {


  private final boolean suppressNoSync;

  @Override
  protected Set<String> usedCommandLineArguments() {
    return null;
  }

  @Override
  protected void execute(TaskContext context) throws HiveMetaException {
    try {
      Liquibase liquibase = context.getLiquibase();
      Contexts liquibaseContexts = context.getLiquibaseContext();
      
      liquibase.getLog().info("Checking if liquibase is already in use");

      //Unfotunately I did not find any better way to check if the liquibase schema is already created or not
      List<ChangeSetStatus> statuses = liquibase.getChangeSetStatuses(liquibaseContexts, null, false)
          .stream()
          .sorted(Comparator.comparing(s -> Integer.parseInt(s.getChangeSet().getId())))
          .collect(Collectors.toList());
      if (statuses.stream().noneMatch(ChangeSetStatus::getPreviouslyRan)) {
        liquibase.getLog().info("Liquibase is not yet in use, syncing with changelog.");

        String currentVerison = null;
        try {
          currentVerison = context.getSchemaInfo().getSchemaVersion();
        } catch (Exception e) {
          if (suppressNoSync) {
            liquibase.getLog().warning("Could not read version from versioning table. Database sync is not possible " +
                "(very old or empty schema), but fail on sync is supressed.");
          } else {
            throw new HiveMetaException("Unable to determine the current database version, liquibase sync is not " +
                "possible.");
          }
        }
        syncSchema(liquibase, liquibaseContexts, statuses, currentVerison, context.getCommandLine().hasOption("dryRun"));
      } else {
        liquibase.getLog().info("Liquibase is already in use, no sync required.");
      }
      liquibase.getLog().info("Liquibase status check successfull.");
    } catch (LiquibaseException e) {
      throw new HiveMetaException("Unable to sync the database with the provided changelog", e);
    }
  }

  private void syncSchema(Liquibase liquibase, Contexts liquibaseContexts,
                          List<ChangeSetStatus> statuses, String currentVerison, boolean dryrun) throws LiquibaseException {
    liquibase.getLog().info("Current metastore version: " + currentVerison);
    StringBuilder logEntry = new StringBuilder();
    StringBuilder labelFilter = new StringBuilder();

    logEntry.append("Assuming that the following scripts are already applied:\n");
    for (int i = 0; i < statuses.size(); i++) {
      ChangeSetStatus status = statuses.get(i);
      Labels version = status.getChangeSet().getLabels();

      logEntry.append(status.getChangeSet().getFilePath());
      labelFilter.append(String.join(" OR ", version.getLabels()));

      if (version.getLabels().contains(currentVerison)) {
        break;
      }

      if (i < statuses.size() - 1) {
        labelFilter.append(" OR ");
        logEntry.append(",");
      }
    }
    liquibase.getLog().info(logEntry.toString());

    //Sync changelog up to the current version.
    if (dryrun) {
      liquibase.changeLogSync(liquibaseContexts,
          new LabelExpression(labelFilter.toString()), new PrintWriter(System.out));
    } else {
      liquibase.changeLogSync(liquibaseContexts, new LabelExpression(labelFilter.toString()));
    }

    liquibase.getLog().info("Changelog synced to verision: " + currentVerison);
  }

  LiquibaseSyncTask(boolean suppressNoSync) {
    this.suppressNoSync = suppressNoSync;
  }

}