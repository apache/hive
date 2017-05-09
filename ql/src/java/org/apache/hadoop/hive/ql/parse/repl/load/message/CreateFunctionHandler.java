
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
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.plan.CreateFunctionDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class CreateFunctionHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context)
      throws SemanticException {
    try {
      FileSystem fs = FileSystem.get(new Path(context.location).toUri(), context.hiveConf);
      MetaData metadata;
      try {
        metadata = EximUtil.readMetaData(fs, new Path(context.location, EximUtil.METADATA_NAME));
      } catch (IOException e) {
        throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
      }

      String dbName = context.isDbNameEmpty() ? metadata.function.getDbName() : context.dbName;
      CreateFunctionDesc desc = new CreateFunctionDesc(
          FunctionUtils.qualifyFunctionName(metadata.function.getFunctionName(), dbName), false,
          metadata.function.getClassName(), metadata.function.getResourceUris()
      );

      Task<FunctionWork> task = TaskFactory.get(new FunctionWork(desc), context.hiveConf);
      context.log.debug("Added create function task : {}:{},{}", task.getId(),
          metadata.function.getFunctionName(), metadata.function.getClassName());
      databasesUpdated.put(dbName, context.dmd.getEventTo());
      return Collections.singletonList(task);
    } catch (Exception e) {
      throw (e instanceof SemanticException)
          ? (SemanticException) e
          : new SemanticException("Error reading message members", e);
    }
  }
}