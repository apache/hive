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

package org.apache.hadoop.hive.ql.ddl.function.reload;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of reloading the functions.
 */
public class ReloadFunctionsOperation extends DDLOperation<ReloadFunctionsDesc> {
  public ReloadFunctionsOperation(DDLOperationContext context, ReloadFunctionsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    try {
      Hive.get(false).reloadFunctions();
      return 0;
    } catch (Exception e) {
      context.getTask().setException(e);
      LOG.error("Failed to reload functions", e);
      return 1;
    }
  }
}
