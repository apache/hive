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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.load;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.messaging.AddCheckConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddDefaultConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddForeignKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddNotNullConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPrimaryKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddUniqueConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.ConstraintEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddCheckConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddDefaultConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddForeignKeyHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddNotNullConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddPrimaryKeyHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddUniqueConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.MessageHandler;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.stripQuotes;

public class LoadConstraint {
  private static final Logger LOG = LoggerFactory.getLogger(LoadFunction.class);
  private Context context;
  private final ConstraintEvent event;
  private final String dbNameToLoadIn;
  private final TaskTracker tracker;
  private final MessageDeserializer deserializer = JSONMessageEncoder.getInstance().getDeserializer();
  String dumpDirectory;
  private transient ReplicationMetricCollector metricCollector;

  public LoadConstraint(Context context, ConstraintEvent event, String dbNameToLoadIn,
      TaskTracker existingTracker) {
    this.context = context;
    this.event = event;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.tracker = new TaskTracker(existingTracker);
  }

  public LoadConstraint(Context context, ConstraintEvent event, String dbNameToLoadIn,
                        TaskTracker existingTracker, String dumpDirectory,
                        ReplicationMetricCollector metricCollector) {
    this.context = context;
    this.event = event;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.tracker = new TaskTracker(existingTracker);
    this.dumpDirectory = dumpDirectory;
    this.metricCollector = metricCollector;
  }

  public TaskTracker tasks() throws IOException, SemanticException {
    URI fromURI = EximUtil
        .getValidatedURI(context.hiveConf, stripQuotes(event.rootDir().toUri().toString()));
    Path fromPath = new Path(fromURI.getScheme(), fromURI.getAuthority(), fromURI.getPath());

    try {
      FileSystem fs = FileSystem.get(fromPath.toUri(), context.hiveConf);
      JSONObject json = new JSONObject(EximUtil.readAsString(fs, fromPath));
      String pksString = json.getString("pks");
      String fksString = json.getString("fks");
      String uksString = json.getString("uks");
      String nnsString = json.getString("nns");
      String dksString = json.getString("dks");
      String cksString = json.getString("cks");
      List<Task<?>> tasks = new ArrayList<Task<?>>();

      if (StringUtils.isNotEmpty(StringUtils.trim(pksString)) && !isPrimaryKeysAlreadyLoaded(pksString)) {
        AddPrimaryKeyHandler pkHandler = new AddPrimaryKeyHandler();
        DumpMetaData pkDumpMetaData = new DumpMetaData(fromPath, DumpType.EVENT_ADD_PRIMARYKEY, Long.MAX_VALUE, Long.MAX_VALUE, null,
            context.hiveConf);
        pkDumpMetaData.setPayload(pksString);
        tasks.addAll(pkHandler.handle(
            new MessageHandler.Context(
                dbNameToLoadIn, fromPath.toString(), null, pkDumpMetaData, context.hiveConf,
                context.hiveDb, context.nestedContext, LOG, dumpDirectory, metricCollector)));
      }

      if (StringUtils.isNotEmpty(StringUtils.trim(uksString)) && !isUniqueConstraintsAlreadyLoaded(uksString)) {
        AddUniqueConstraintHandler ukHandler = new AddUniqueConstraintHandler();
        DumpMetaData ukDumpMetaData = new DumpMetaData(fromPath, DumpType.EVENT_ADD_UNIQUECONSTRAINT, Long.MAX_VALUE, Long.MAX_VALUE, null,
            context.hiveConf);
        ukDumpMetaData.setPayload(uksString);
        tasks.addAll(ukHandler.handle(
            new MessageHandler.Context(
                dbNameToLoadIn, fromPath.toString(), null, ukDumpMetaData, context.hiveConf,
                context.hiveDb, context.nestedContext, LOG, dumpDirectory, metricCollector)));
      }

      if (StringUtils.isNotEmpty(StringUtils.trim(nnsString)) && !isNotNullConstraintsAlreadyLoaded(nnsString)) {
        AddNotNullConstraintHandler nnHandler = new AddNotNullConstraintHandler();
        DumpMetaData nnDumpMetaData = new DumpMetaData(fromPath, DumpType.EVENT_ADD_NOTNULLCONSTRAINT, Long.MAX_VALUE, Long.MAX_VALUE, null,
            context.hiveConf);
        nnDumpMetaData.setPayload(nnsString);
        tasks.addAll(nnHandler.handle(
            new MessageHandler.Context(
                dbNameToLoadIn, fromPath.toString(), null, nnDumpMetaData, context.hiveConf,
                context.hiveDb, context.nestedContext, LOG, dumpDirectory, metricCollector)));
      }

      if (StringUtils.isNotEmpty(StringUtils.trim(dksString)) && !isDefaultConstraintsAlreadyLoaded(dksString)) {
        AddDefaultConstraintHandler dkHandler = new AddDefaultConstraintHandler();
        DumpMetaData dkDumpMetaData = new DumpMetaData(fromPath, DumpType.EVENT_ADD_DEFAULTCONSTRAINT, Long.MAX_VALUE, Long.MAX_VALUE, null,
            context.hiveConf);
        dkDumpMetaData.setPayload(dksString);
        tasks.addAll(dkHandler.handle(
            new MessageHandler.Context(
                dbNameToLoadIn, fromPath.toString(), null, dkDumpMetaData, context.hiveConf,
                context.hiveDb, context.nestedContext, LOG, dumpDirectory, metricCollector)));
      }

      if (StringUtils.isNotEmpty(StringUtils.trim(cksString))  && !isCheckConstraintsAlreadyLoaded(cksString)) {
        AddCheckConstraintHandler ckHandler = new AddCheckConstraintHandler();
        DumpMetaData dkDumpMetaData = new DumpMetaData(fromPath, DumpType.EVENT_ADD_CHECKCONSTRAINT, Long.MAX_VALUE, Long.MAX_VALUE, null,
            context.hiveConf);
        dkDumpMetaData.setPayload(cksString);
        tasks.addAll(ckHandler.handle(
            new MessageHandler.Context(
                dbNameToLoadIn, fromPath.toString(), null, dkDumpMetaData, context.hiveConf,
                context.hiveDb, context.nestedContext, LOG, dumpDirectory, metricCollector)));
      }

      if (StringUtils.isNotEmpty(StringUtils.trim(fksString)) && !isForeignKeysAlreadyLoaded(fksString)) {
        AddForeignKeyHandler fkHandler = new AddForeignKeyHandler();
        DumpMetaData fkDumpMetaData = new DumpMetaData(fromPath, DumpType.EVENT_ADD_FOREIGNKEY, Long.MAX_VALUE, Long.MAX_VALUE, null,
            context.hiveConf);
        fkDumpMetaData.setPayload(fksString);
        tasks.addAll(fkHandler.handle(
            new MessageHandler.Context(
                dbNameToLoadIn, fromPath.toString(), null, fkDumpMetaData, context.hiveConf,
                context.hiveDb, context.nestedContext, LOG, dumpDirectory, metricCollector)));
      }

      tasks.forEach(tracker::addTask);
      return tracker;
    } catch (Exception e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
    }
  }

  private boolean isPrimaryKeysAlreadyLoaded(String pksMsgString) throws Exception {
    AddPrimaryKeyMessage msg = deserializer.getAddPrimaryKeyMessage(pksMsgString);
    List<SQLPrimaryKey> pksInMsg = msg.getPrimaryKeys();
    if (pksInMsg.isEmpty()) {
      return true;
    }

    String dbName = StringUtils.isBlank(dbNameToLoadIn) ? pksInMsg.get(0).getTable_db() : dbNameToLoadIn;
    List<SQLPrimaryKey> pks;
    try {
      pks = context.hiveDb.getPrimaryKeyList(dbName, pksInMsg.get(0).getTable_name());
    } catch (NoSuchObjectException e) {
      return false;
    }
    return CollectionUtils.isNotEmpty(pks);
  }

  private boolean isForeignKeysAlreadyLoaded(String fksMsgString) throws Exception {
    AddForeignKeyMessage msg = deserializer.getAddForeignKeyMessage(fksMsgString);
    List<SQLForeignKey> fksInMsg = msg.getForeignKeys();
    if (fksInMsg.isEmpty()) {
      return true;
    }

    String dbName = StringUtils.isBlank(dbNameToLoadIn) ? fksInMsg.get(0).getFktable_db() : dbNameToLoadIn;
    List<SQLForeignKey> fks;
    try {
      fks = context.hiveDb.getForeignKeyList(dbName, fksInMsg.get(0).getFktable_name());
    } catch (NoSuchObjectException e) {
      return false;
    }
    return CollectionUtils.isNotEmpty(fks);
  }

  private boolean isUniqueConstraintsAlreadyLoaded(String uksMsgString) throws Exception {
    AddUniqueConstraintMessage msg = deserializer.getAddUniqueConstraintMessage(uksMsgString);
    List<SQLUniqueConstraint> uksInMsg = msg.getUniqueConstraints();
    if (uksInMsg.isEmpty()) {
      return true;
    }

    String dbName = StringUtils.isBlank(dbNameToLoadIn) ? uksInMsg.get(0).getTable_db() : dbNameToLoadIn;
    List<SQLUniqueConstraint> uks;
    try {
      uks = context.hiveDb.getUniqueConstraintList(dbName, uksInMsg.get(0).getTable_name());
    } catch (NoSuchObjectException e) {
      return false;
    }
    return CollectionUtils.isNotEmpty(uks);
  }

  private boolean isNotNullConstraintsAlreadyLoaded(String nnsMsgString) throws Exception {
    AddNotNullConstraintMessage msg = deserializer.getAddNotNullConstraintMessage(nnsMsgString);
    List<SQLNotNullConstraint> nnsInMsg = msg.getNotNullConstraints();
    if (nnsInMsg.isEmpty()) {
      return true;
    }

    String dbName = StringUtils.isBlank(dbNameToLoadIn) ? nnsInMsg.get(0).getTable_db() : dbNameToLoadIn;
    List<SQLNotNullConstraint> nns;
    try {
      nns = context.hiveDb.getNotNullConstraintList(dbName, nnsInMsg.get(0).getTable_name());
    } catch (NoSuchObjectException e) {
      return false;
    }
    return CollectionUtils.isNotEmpty(nns);
  }

  private boolean isCheckConstraintsAlreadyLoaded(String cksMsgString) throws Exception {
    AddCheckConstraintMessage msg = deserializer.getAddCheckConstraintMessage(cksMsgString);
    List<SQLCheckConstraint> cksInMsg = msg.getCheckConstraints();
    if (cksInMsg.isEmpty()) {
      return true;
    }

    String dbName = StringUtils.isBlank(dbNameToLoadIn) ? cksInMsg.get(0).getTable_db() : dbNameToLoadIn;
    List<SQLCheckConstraint> cks;
    try {
      cks = context.hiveDb.getCheckConstraintList(dbName, cksInMsg.get(0).getTable_name());
    } catch (NoSuchObjectException e) {
      return false;
    }
    return ((cks != null) && !cks.isEmpty());
  }

  private boolean isDefaultConstraintsAlreadyLoaded(String dksMsgString) throws Exception {
    AddDefaultConstraintMessage msg = deserializer.getAddDefaultConstraintMessage(dksMsgString);
    List<SQLDefaultConstraint> dksInMsg = msg.getDefaultConstraints();
    if (dksInMsg.isEmpty()) {
      return true;
    }

    String dbName = StringUtils.isBlank(dbNameToLoadIn) ? dksInMsg.get(0).getTable_db() : dbNameToLoadIn;
    List<SQLDefaultConstraint> dks;
    try {
      dks = context.hiveDb.getDefaultConstraintList(dbName, dksInMsg.get(0).getTable_name());
    } catch (NoSuchObjectException e) {
      return false;
    }
    return ((dks != null) && !dks.isEmpty());
  }

}
