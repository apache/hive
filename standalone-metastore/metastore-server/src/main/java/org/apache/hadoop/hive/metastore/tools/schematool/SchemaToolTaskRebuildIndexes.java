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
package org.apache.hadoop.hive.metastore.tools.schematool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rebuilds all B-tree indexes in the HMS backend database.
 *
 * <p>Stops early if duplicate keys are found for any unique index and reports the affected
 * indexes.
 *
 * <p>With {@code --dryRun}, logs the rebuild DDL without executing it.
 */
class SchemaToolTaskRebuildIndexes extends SchemaToolTask {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskRebuildIndexes.class);

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    // No arguments needed.
  }

  @Override
  void execute() throws HiveMetaException {
    try (Connection conn = schemaTool.getConnectionToMetastore(false)) {
      IndexRebuilder rebuilder =
          IndexRebuilderFactory.create(schemaTool.getDbType(), conn, schemaTool);
      executeWithRebuilder(rebuilder);
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to close metastore connection", e);
    }
  }

  void executeWithRebuilder(IndexRebuilder rebuilder) throws HiveMetaException {
    List<IndexInfo> indexes = rebuilder.loadIndexes();
    if (indexes.isEmpty()) {
      LOG.info("No indexes found to rebuild.");
      return;
    }

    LOG.info("Found {} index(es) to rebuild.", indexes.size());

    List<IndexInfo> blocked = new ArrayList<>();
    for (IndexInfo index : indexes) {
      long dupes = rebuilder.findDuplicates(index);
      if (dupes > 0) {
        LOG.error("Cannot rebuild index \"{}\" on table \"{}\": {} duplicate row group(s) detected."
            + " Clean up duplicates first.", index.indexName(), index.tableName(), dupes);
        blocked.add(index);
      }
    }

    if (!blocked.isEmpty()) {
      String detail = blocked.stream()
          .map(i -> "\"" + i.indexName() + "\" on \"" + i.tableName() + "\"")
          .collect(Collectors.joining(", "));
      throw new HiveMetaException("Index rebuild blocked by duplicate data in "
          + blocked.size() + " index(es): " + detail + ". Remove duplicates and retry.");
    }

    for (IndexInfo index : indexes) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Rebuilding: {}", index);
        LOG.info(rebuilder.describeRebuildDDL(index));
      }
      if (!schemaTool.isDryRun()) {
        rebuilder.rebuildIndex(index);
        LOG.info("Done.");
      }
    }

    if (schemaTool.isDryRun()) {
      LOG.info("Dry run complete. No changes were made.");
    } else {
      LOG.info("Index rebuild complete.");
    }
  }
}
