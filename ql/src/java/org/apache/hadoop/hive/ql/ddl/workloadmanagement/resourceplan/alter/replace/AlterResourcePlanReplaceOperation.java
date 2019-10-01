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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.alter.replace;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlanStatus;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.alter.AbstractAlterResourcePlanStatusOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of replacing a resource plan.
 */
public class AlterResourcePlanReplaceOperation
    extends AbstractAlterResourcePlanStatusOperation<AlterResourcePlanReplaceDesc> {
  // Note: the resource plan operations are going to be annotated with namespace based on the config
  //       inside Hive.java. We don't want HS2 to be aware of namespaces beyond that, or to even see
  //       that there exist other namespaces, because one HS2 always operates inside just one and we
  //       don't want this complexity to bleed everywhere. Therefore, this code doesn't care about
  //       namespaces - Hive.java will transparently scope everything. That's the idea anyway.
  public AlterResourcePlanReplaceOperation(DDLOperationContext context, AlterResourcePlanReplaceDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException, IOException {
    WMNullableResourcePlan resourcePlan = new WMNullableResourcePlan();
    if (desc.getDestinationResourcePlanName() == null) {
      resourcePlan.setStatus(WMResourcePlanStatus.ACTIVE);
    } else {
      resourcePlan.setName(desc.getDestinationResourcePlanName());
    }

    WMFullResourcePlan appliedResourcePlan = context.getDb().alterResourcePlan(desc.getResourcePlanName(), resourcePlan,
        false, false, true);

    handleWMServiceChangeIfNeeded(appliedResourcePlan, false, false, true);
    return 0;
  }
}
