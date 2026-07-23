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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SchemaToolTaskRebuildIndexes extends SchemaToolTask {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskRebuildIndexes.class);
  static final String REBUILD_INDEXES_FILE_PREFIX = "rebuild-indexes";

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
  }

  @Override
  void execute() throws HiveMetaException {
    String dbType = schemaTool.getDbType();
    String scriptDir = schemaTool.getMetaStoreSchemaInfo().getMetaStoreScriptDir();
    String scriptFile = REBUILD_INDEXES_FILE_PREFIX + "." + dbType + ".sql";
    File script = new File(scriptDir, scriptFile);

    if (!script.exists()) {
      throw new HiveMetaException(
          "-rebuildIndexes is not supported for -dbType " + dbType + ". "
              + "Expected script not found: " + script.getAbsolutePath());
    }

    if (schemaTool.isDryRun()) {
      try {
        LOG.info("Dry run: would execute {}", script.getAbsolutePath());
        LOG.info(new String(Files.readAllBytes(script.toPath())));
      } catch (IOException e) {
        throw new HiveMetaException("Failed to read rebuild-indexes script", e);
      }
      return;
    }

    LOG.info("Starting index rebuild using {}", scriptFile);
    try {
      schemaTool.execSql(scriptDir, scriptFile);
    } catch (IOException e) {
      throw new HiveMetaException("Index rebuild failed", e);
    }
    LOG.info("Index rebuild complete.");
  }
}