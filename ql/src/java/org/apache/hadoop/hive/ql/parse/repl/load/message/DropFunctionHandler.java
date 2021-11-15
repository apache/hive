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

import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.function.drop.DropFunctionDesc;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class DropFunctionHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context)
      throws SemanticException {
    DropFunctionMessage msg = deserializer.getDropFunctionMessage(context.dmd.getPayload());
    String actualDbName = context.isDbNameEmpty() ? msg.getDB() : context.dbName;
    String qualifiedFunctionName =
        FunctionUtils.qualifyFunctionName(msg.getFunctionName(), actualDbName);
    // When the load is invoked via Scheduler's executor route, the function resources will not be
    // there in classpath. Processing drop function event tries to unregister the function resulting
    // in ClassNotFoundException being thrown in such case.
    // Obtaining FunctionInfo object from FunctionRegistry will add the function's resources URLs to UDFClassLoader.
    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(qualifiedFunctionName);
    DropFunctionDesc desc = new DropFunctionDesc(
            qualifiedFunctionName, false, context.eventOnlyReplicationSpec());
    Task<DDLWork> dropFunctionTask =
        TaskFactory.get(new DDLWork(readEntitySet, writeEntitySet, desc, true,
                context.getDumpDirectory(), context.getMetricCollector()), context.hiveConf);
    context.log.debug(
        "Added drop function task : {}:{}", dropFunctionTask.getId(), desc.getName()
    );
    updatedMetadata.set(context.dmd.getEventTo().toString(), actualDbName, null, null);
    return Collections.singletonList(dropFunctionTask);
  }
}
