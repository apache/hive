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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.mapping.create;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of creating a workload management mapping.
 */
public class CreateWMMappingOperation extends DDLOperation<CreateWMMappingDesc> {
  public CreateWMMappingOperation(DDLOperationContext context, CreateWMMappingDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException, IOException {
    WMMapping mapping = new WMMapping(desc.getResourcePlanName(), desc.getEntityType(), desc.getEntityName());
    mapping.setPoolPath(desc.getPoolPath());
    if (desc.getOrdering() != null) {
      mapping.setOrdering(desc.getOrdering());
    }

    context.getDb().createOrUpdateWMMapping(mapping, false);

    return 0;
  }
}
