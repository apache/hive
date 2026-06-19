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
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.UpdatedMetaDataTracker;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

public interface MessageHandler {

  List<Task<?>> handle(Context withinContext) throws SemanticException;

  Set<ReadEntity> readEntities();

  Set<WriteEntity> writeEntities();

  UpdatedMetaDataTracker getUpdatedMetadata();

  class Context {
    public String location;
    public final String dbName;
    public final Task<?> precursor;
    public DumpMetaData dmd;
    final HiveConf hiveConf;
    final Hive db;
    final org.apache.hadoop.hive.ql.Context nestedContext;
    final Logger log;
    String dumpDirectory;
    private transient ReplicationMetricCollector metricCollector;

    public Context(String dbName, String location,
        Task<?> precursor, DumpMetaData dmd, HiveConf hiveConf,
        Hive db, org.apache.hadoop.hive.ql.Context nestedContext, Logger log) {
      this.dbName = dbName;
      this.location = location;
      this.precursor = precursor;
      this.dmd = dmd;
      this.hiveConf = hiveConf;
      this.db = db;
      this.nestedContext = nestedContext;
      this.log = log;
    }

    public Context(String dbName, String location,
                   Task<?> precursor, DumpMetaData dmd, HiveConf hiveConf,
                   Hive db, org.apache.hadoop.hive.ql.Context nestedContext, Logger log,
                   String dumpDirectory, ReplicationMetricCollector metricCollector) {
      this.dbName = dbName;
      this.location = location;
      this.precursor = precursor;
      this.dmd = dmd;
      this.hiveConf = hiveConf;
      this.db = db;
      this.nestedContext = nestedContext;
      this.log = log;
      this.dumpDirectory = dumpDirectory;
      this.metricCollector = metricCollector;
    }

    public Context(Context other, String dbName) {
      this.dbName = dbName;
      this.location = other.location;
      this.precursor = other.precursor;
      this.dmd = other.dmd;
      this.hiveConf = other.hiveConf;
      this.db = other.db;
      this.nestedContext = other.nestedContext;
      this.log = other.log;
    }

    public Context(Context other, String dbName, String dumpDirectory, ReplicationMetricCollector metricCollector) {
      this.dbName = dbName;
      this.location = other.location;
      this.precursor = other.precursor;
      this.dmd = other.dmd;
      this.hiveConf = other.hiveConf;
      this.db = other.db;
      this.nestedContext = other.nestedContext;
      this.log = other.log;
      this.dumpDirectory = dumpDirectory;
      this.metricCollector = metricCollector;
    }

    public boolean isDbNameEmpty() {
      return StringUtils.isEmpty(dbName);
    }

    /**
     * not sure why we have this, this should always be read from the _metadata file via the
     * {@link org.apache.hadoop.hive.ql.parse.repl.load.MetadataJson#readReplicationSpec}
     */
    ReplicationSpec eventOnlyReplicationSpec() throws SemanticException {
      String eventId = dmd.getEventTo().toString();
      return new ReplicationSpec(eventId, eventId);
    }

    public org.apache.hadoop.hive.ql.Context getNestedContext() {
      return nestedContext;
    }

    public String getDumpDirectory() {
      return dumpDirectory;
    }

    public ReplicationMetricCollector getMetricCollector() {
      return metricCollector;
    }

    public HiveTxnManager getTxnMgr() {
      return nestedContext.getHiveTxnManager();
    }

    public void setLocation(String location) {
      this.location = location;
    }
  }
}
