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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.DatabaseEvent;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;

public class FSDatabaseEvent implements DatabaseEvent {

  private final Path dbMetadataFile;
  private final FileSystem fileSystem;

  FSDatabaseEvent(HiveConf hiveConf, String dbDumpDirectory) {
    try {
      this.dbMetadataFile = new Path(dbDumpDirectory, EximUtil.METADATA_NAME);
      this.fileSystem = dbMetadataFile.getFileSystem(hiveConf);
    } catch (Exception e) {
      String message = "Error while identifying the filesystem for db "
          + "metadata file in " + dbDumpDirectory;
      throw new RuntimeException(message, e);
    }
  }

  @Override
  public Database dbInMetadata(String dbNameToOverride) throws SemanticException {
    try {
      MetaData rv = EximUtil.readMetaData(fileSystem, dbMetadataFile);
      Database dbObj = rv.getDatabase();
      if (dbObj == null) {
        throw new IllegalArgumentException(
            "_metadata file read did not contain a db object - invalid dump.");
      }

      // override the db name if provided in repl load command
      if (StringUtils.isNotBlank(dbNameToOverride)) {
        dbObj.setName(dbNameToOverride);
      }
      return dbObj;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  @Override
  public State toState() {
    return new FSDBState(dbMetadataFile.getParent().toString());
  }

  @Override
  public EventType eventType() {
    return EventType.Database;
  }

  static class FSDBState implements DatabaseEvent.State {
    final String dbDumpDirectory;

    FSDBState(String dbDumpDirectory) {
      this.dbDumpDirectory = dbDumpDirectory;
    }

    @Override
    public DatabaseEvent toEvent(HiveConf hiveConf) {
      return new FSDatabaseEvent(hiveConf, dbDumpDirectory);
    }
  }
}
