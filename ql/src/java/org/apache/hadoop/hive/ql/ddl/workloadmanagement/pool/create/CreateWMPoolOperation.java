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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.pool.create;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Operation process of creating a workload management pool.
 */
public class CreateWMPoolOperation extends DDLOperation<CreateWMPoolDesc> {
  public CreateWMPoolOperation(DDLOperationContext context, CreateWMPoolDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException, IOException {
    WMPool pool = new WMPool(desc.getResourcePlanName(), desc.getPoolPath());
    pool.setAllocFraction(desc.getAllocFraction());
    pool.setQueryParallelism(desc.getQueryParallelism());
    if (desc.getSchedulingPolicy() != null) {
      if (!MetaStoreUtils.isValidSchedulingPolicy(desc.getSchedulingPolicy())) {
        throw new SemanticException("Invalid scheduling policy " + desc.getSchedulingPolicy());
      }
      pool.setSchedulingPolicy(desc.getSchedulingPolicy());
    }

    context.getDb().createWMPool(pool);

    return 0;
  }
}
