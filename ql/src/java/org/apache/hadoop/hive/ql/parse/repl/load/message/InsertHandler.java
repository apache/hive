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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class InsertHandler extends AbstractMessageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(InsertHandler.class);

  @Override
  public List<Task<?>> handle(Context withinContext)
      throws SemanticException {
    try {
      FileSystem fs =
          FileSystem.get(new Path(withinContext.location).toUri(), withinContext.hiveConf);
      MetaData metaData =
          EximUtil.readMetaData(fs, new Path(withinContext.location, EximUtil.METADATA_NAME));
      ReplicationSpec replicationSpec = metaData.getReplicationSpec();
      if (replicationSpec.isNoop()) {
        return Collections.emptyList();
      }
    } catch (Exception e) {
      LOG.error("failed to load insert event", e);
      throw new SemanticException(e);
    }

    InsertMessage insertMessage = deserializer.getInsertMessage(withinContext.dmd.getPayload());
    String actualDbName =
        withinContext.isDbNameEmpty() ? insertMessage.getDB() : withinContext.dbName;
    Context currentContext = new Context(withinContext, actualDbName,
                                         withinContext.getDumpDirectory(), withinContext.getMetricCollector());

    // Piggybacking in Import logic for now
    TableHandler tableHandler = new TableHandler();
    List<Task<?>> tasks = tableHandler.handle(currentContext);
    readEntitySet.addAll(tableHandler.readEntities());
    writeEntitySet.addAll(tableHandler.writeEntities());
    getUpdatedMetadata().copyUpdatedMetadata(tableHandler.getUpdatedMetadata());
    return tasks;
  }
}
