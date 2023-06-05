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
import liquibase.Labels;
import liquibase.Liquibase;
import liquibase.changelog.ChangeSetStatus;
import liquibase.exception.LiquibaseException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.SchemaInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class LiquibaseValidationTask extends SchemaToolTask {

  @Override
  protected Set<String> usedCommandLineArguments() {
    return null;
  }

  @Override
  @SuppressWarnings({ "squid:S2201", "ResultOfMethodCallIgnored" })
  protected void execute(TaskContext context) throws HiveMetaException {
    Liquibase liquibase = context.getLiquibase();
    Contexts liquibaseContexts = context.getLiquibaseContext();

    liquibase.getLog().info("Validating liquibase changesets if match all requirements.");

    try {
      List<ChangeSetStatus> statuses = liquibase.getChangeSetStatuses(liquibaseContexts, null, false);

      for (int i = 1; i < statuses.size(); i++) {
        ChangeSetStatus status = statuses.get(i);
        Labels version = status.getChangeSet().getLabels();
        if (version == null || version.isEmpty()) {
          throw new HiveMetaException("All version scripts must have a label containing one or more valid version(s)! " +
              "The following script does not have any label: " + status.getChangeSet().getFilePath());
        }
        
        if (version.getLabels().stream().anyMatch(s -> !SchemaInfo.isValidVersion(version.toString()))) {
          throw new HiveMetaException("All version scripts must have a label containing one or more valid version(s)! " +
              "Valid versions are consist of major, minor, incremental version numbers and optionally can have qualifiers. " +
              "The label ( " + version + ") of the following script does not conform the required format: " +
              status.getChangeSet().getFilePath());
        }
        
        if (!NumberUtils.isParsable(status.getChangeSet().getId())) {
          throw new HiveMetaException("All changesets must have a vaild and unique integer id! The following changeset " +
              "does not have a valid integer id: " + status.getChangeSet().getFilePath());
        }
        if (StringUtils.isBlank(status.getChangeSet().getComments())) {
          throw new HiveMetaException("All changesets must have a vaild comment! The following changeset " +
              "does not have a valid comment: " + status.getChangeSet().getFilePath());
        }
      }
      //No need to check the result, Collectors.toMap will fail with IllegalStateException in case of duplicate key(s).
      String dbType = HiveSchemaHelper.DB_POSTGRES.equals(context.getCommandLine().getDbType())
          ? "postgresql"
          : context.getCommandLine().getDbType();
      statuses.stream()
          .filter(s -> CollectionUtils.isEmpty(s.getChangeSet().getDbmsSet()) || s.getChangeSet().getDbmsSet().contains(dbType))
          .collect(Collectors.toMap(s -> s.getChangeSet().getId(), s -> s));
    } catch (LiquibaseException e) {
      throw new HiveMetaException("Unable obtain the liquibase changelog.", e);
    } catch (IllegalStateException e) {
      if (e.getMessage().startsWith("Duplicate key")) {
        throw new HiveMetaException("All changesets must have a unique id. One or more changesets have the same id!");
      }
      throw new HiveMetaException("Unable obtain the liquibase changelog.", e);
    }
    liquibase.getLog().info("Validation of Liquibase changesets successfull.");
  }

}
