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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogWork;
import org.apache.hadoop.hive.ql.exec.repl.util.AddDependencyToLeaves;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.FunctionEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.util.DAGTraversal;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.message.CreateFunctionHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.MessageHandler;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.List;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.stripQuotes;

public class LoadFunction {
  private static final Logger LOG = LoggerFactory.getLogger(LoadFunction.class);
  private Context context;
  private ReplLogger replLogger;
  private final FunctionEvent event;
  private final String dbNameToLoadIn;
  private final TaskTracker tracker;
  String dumpDirectory;
  private final ReplicationMetricCollector metricCollector;

  public LoadFunction(Context context, ReplLogger replLogger, FunctionEvent event,
                      String dbNameToLoadIn, TaskTracker existingTracker, ReplicationMetricCollector metricCollector) {
    this.context = context;
    this.replLogger = replLogger;
    this.event = event;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.tracker = new TaskTracker(existingTracker);
    this.metricCollector = metricCollector;
  }

  public LoadFunction(Context context, ReplLogger replLogger, FunctionEvent event,
                      String dbNameToLoadIn, TaskTracker existingTracker,
                      String dumpDirectory, ReplicationMetricCollector metricCollector) {
    this.context = context;
    this.replLogger = replLogger;
    this.event = event;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.tracker = new TaskTracker(existingTracker);
    this.dumpDirectory = dumpDirectory;
    this.metricCollector = metricCollector;
  }

  private void createFunctionReplLogTask(List<Task<?>> functionTasks,
                                         String functionName) {
    ReplStateLogWork replLogWork = new ReplStateLogWork(replLogger, functionName, dumpDirectory, metricCollector);
    Task<ReplStateLogWork> replLogTask = TaskFactory.get(replLogWork, context.hiveConf);
    DAGTraversal.traverse(functionTasks, new AddDependencyToLeaves(replLogTask));
  }

  public TaskTracker tasks() throws IOException, SemanticException {
    URI fromURI = EximUtil
        .getValidatedURI(context.hiveConf, stripQuotes(event.rootDir().toUri().toString()));
    Path fromPath = new Path(fromURI.getScheme(), fromURI.getAuthority(), fromURI.getPath());

    try {
      if (isFunctionAlreadyLoaded(fromPath)) {
        return tracker;
      }
      CreateFunctionHandler handler = new CreateFunctionHandler();
      List<Task<?>> tasks = handler.handle(
          new MessageHandler.Context(
              dbNameToLoadIn, fromPath.toString(), null, null, context.hiveConf,
              context.hiveDb, context.nestedContext, LOG, dumpDirectory, metricCollector)
      );
      createFunctionReplLogTask(tasks, handler.getFunctionName());
      tasks.forEach(tracker::addTask);
      return tracker;
    } catch (Exception e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
    }
  }

  private boolean isFunctionAlreadyLoaded(Path funcDumpRoot) throws HiveException, IOException {
    Path metadataPath = new Path(funcDumpRoot, EximUtil.METADATA_NAME);
    FileSystem fs = FileSystem.get(metadataPath.toUri(), context.hiveConf);
    MetaData metadata = EximUtil.readMetaData(fs, metadataPath);
    Function function;
    try {
      String dbName = StringUtils.isBlank(dbNameToLoadIn) ? metadata.function.getDbName() : dbNameToLoadIn;
      function = context.hiveDb.getFunction(dbName, metadata.function.getFunctionName());
    } catch (HiveException e) {
      if (e.getCause() instanceof NoSuchObjectException) {
        return false;
      }
      throw e;
    }
    return (function != null);
  }

}
