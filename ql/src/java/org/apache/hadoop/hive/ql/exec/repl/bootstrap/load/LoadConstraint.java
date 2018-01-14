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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.ConstraintEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddForeignKeyHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddNotNullConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddPrimaryKeyHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddUniqueConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.MessageHandler;
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

  public LoadConstraint(Context context, ConstraintEvent event, String dbNameToLoadIn,
      TaskTracker existingTracker) {
    this.context = context;
    this.event = event;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.tracker = new TaskTracker(existingTracker);
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
      List<Task<? extends Serializable>> tasks = new ArrayList<Task<? extends Serializable>>();

      if (pksString != null && !pksString.isEmpty()) {
        AddPrimaryKeyHandler pkHandler = new AddPrimaryKeyHandler();
        DumpMetaData pkDumpMetaData = new DumpMetaData(fromPath, DumpType.EVENT_ADD_PRIMARYKEY, Long.MAX_VALUE, Long.MAX_VALUE, null,
            context.hiveConf);
        pkDumpMetaData.setPayload(pksString);
        tasks.addAll(pkHandler.handle(
            new MessageHandler.Context(
                dbNameToLoadIn, null, fromPath.toString(), null, pkDumpMetaData, context.hiveConf,
                context.hiveDb, null, LOG)));
      }

      if (uksString != null && !uksString.isEmpty()) {
        AddUniqueConstraintHandler ukHandler = new AddUniqueConstraintHandler();
        DumpMetaData ukDumpMetaData = new DumpMetaData(fromPath, DumpType.EVENT_ADD_UNIQUECONSTRAINT, Long.MAX_VALUE, Long.MAX_VALUE, null,
            context.hiveConf);
        ukDumpMetaData.setPayload(uksString);
        tasks.addAll(ukHandler.handle(
            new MessageHandler.Context(
                dbNameToLoadIn, null, fromPath.toString(), null, ukDumpMetaData, context.hiveConf,
                context.hiveDb, null, LOG)));
      }

      if (nnsString != null && !nnsString.isEmpty()) {
        AddNotNullConstraintHandler nnHandler = new AddNotNullConstraintHandler();
        DumpMetaData nnDumpMetaData = new DumpMetaData(fromPath, DumpType.EVENT_ADD_NOTNULLCONSTRAINT, Long.MAX_VALUE, Long.MAX_VALUE, null,
            context.hiveConf);
        nnDumpMetaData.setPayload(nnsString);
        tasks.addAll(nnHandler.handle(
            new MessageHandler.Context(
                dbNameToLoadIn, null, fromPath.toString(), null, nnDumpMetaData, context.hiveConf,
                context.hiveDb, null, LOG)));
      }

      if (fksString != null && !fksString.isEmpty()) {
        AddForeignKeyHandler fkHandler = new AddForeignKeyHandler();
        DumpMetaData fkDumpMetaData = new DumpMetaData(fromPath, DumpType.EVENT_ADD_FOREIGNKEY, Long.MAX_VALUE, Long.MAX_VALUE, null,
            context.hiveConf);
        fkDumpMetaData.setPayload(fksString);
        tasks.addAll(fkHandler.handle(
            new MessageHandler.Context(
                dbNameToLoadIn, null, fromPath.toString(), null, fkDumpMetaData, context.hiveConf,
                context.hiveDb, null, LOG)));
      }

      tasks.forEach(tracker::addTask);
      return tracker;
    } catch (Exception e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
    }
  }

}
