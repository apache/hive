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
package org.apache.hadoop.hive.ql.parse.repl.load.message;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.UpdatedMetaDataTracker;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

public interface MessageHandler {

  List<Task<? extends Serializable>> handle(Context withinContext) throws SemanticException;

  Set<ReadEntity> readEntities();

  Set<WriteEntity> writeEntities();

  UpdatedMetaDataTracker getUpdatedMetadata();

  class Context {
    public String dbName;
    public final String tableName, location;
    public final Task<? extends Serializable> precursor;
    public DumpMetaData dmd;
    final HiveConf hiveConf;
    final Hive db;
    final org.apache.hadoop.hive.ql.Context nestedContext;
    final Logger log;

    public Context(String dbName, String tableName, String location,
        Task<? extends Serializable> precursor, DumpMetaData dmd, HiveConf hiveConf,
        Hive db, org.apache.hadoop.hive.ql.Context nestedContext, Logger log) {
      this.dbName = dbName;
      this.tableName = tableName;
      this.location = location;
      this.precursor = precursor;
      this.dmd = dmd;
      this.hiveConf = hiveConf;
      this.db = db;
      this.nestedContext = nestedContext;
      this.log = log;
    }

    public Context(Context other, String dbName, String tableName) {
      this.dbName = dbName;
      this.tableName = tableName;
      this.location = other.location;
      this.precursor = other.precursor;
      this.dmd = other.dmd;
      this.hiveConf = other.hiveConf;
      this.db = other.db;
      this.nestedContext = other.nestedContext;
      this.log = other.log;
    }

    boolean isTableNameEmpty() {
      return StringUtils.isEmpty(tableName);
    }

    public boolean isDbNameEmpty() {
      return StringUtils.isEmpty(dbName);
    }

    ReplicationSpec eventOnlyReplicationSpec() throws SemanticException {
      String eventId = dmd.getEventTo().toString();
      return new ReplicationSpec(eventId, eventId);
    }
  }
}
