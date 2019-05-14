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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlanStatus;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Operation process of altering a resource plan.
 */
public class AlterResourcePlanOperation extends DDLOperation {
  private final AlterResourcePlanDesc desc;

  // Note: the resource plan operations are going to be annotated with namespace based on the config
  //       inside Hive.java. We don't want HS2 to be aware of namespaces beyond that, or to even see
  //       that there exist other namespaces, because one HS2 always operates inside just one and we
  //       don't want this complexity to bleed everywhere. Therefore, this code doesn't care about
  //       namespaces - Hive.java will transparently scope everything. That's the idea anyway.
  public AlterResourcePlanOperation(DDLOperationContext context, AlterResourcePlanDesc desc) {
    super(context);
    this.desc = desc;
  }

  @Override
  public int execute() throws HiveException, IOException {
    if (desc.shouldValidate()) {
      WMValidateResourcePlanResponse result = context.getDb().validateResourcePlan(desc.getPlanName());
      try (DataOutputStream out = DDLUtils.getOutputStream(new Path(desc.getResFile()), context)) {
        context.getFormatter().showErrors(out, result);
      } catch (IOException e) {
        throw new HiveException(e);
      }
      return 0;
    }

    WMNullableResourcePlan resourcePlan = desc.getResourcePlan();
    WMFullResourcePlan appliedResourcePlan = context.getDb().alterResourcePlan(desc.getPlanName(), resourcePlan,
        desc.isEnableActivate(), desc.isForceDeactivate(), desc.isReplace());

    boolean isActivate = resourcePlan.getStatus() != null && resourcePlan.getStatus() == WMResourcePlanStatus.ACTIVE;
    boolean mustHaveAppliedChange = isActivate || desc.isForceDeactivate();
    if (!mustHaveAppliedChange && !desc.isReplace()) {
      return 0; // The modification cannot affect an active plan.
    }
    if (appliedResourcePlan == null && !mustHaveAppliedChange) {
      return 0; // Replacing an inactive plan.
    }

    WorkloadManager wm = WorkloadManager.getInstance();
    boolean isInTest = HiveConf.getBoolVar(context.getConf(), ConfVars.HIVE_IN_TEST);
    if (wm == null && isInTest) {
      return 0; // Skip for tests if WM is not present.
    }

    if ((appliedResourcePlan == null) != desc.isForceDeactivate()) {
      throw new HiveException("Cannot get a resource plan to apply; or non-null plan on disable");
      // TODO: shut down HS2?
    }
    assert appliedResourcePlan == null || appliedResourcePlan.getPlan().getStatus() == WMResourcePlanStatus.ACTIVE;

    handleWorkloadManagementServiceChange(wm, isActivate, appliedResourcePlan);

    return 0;
  }

  private int handleWorkloadManagementServiceChange(WorkloadManager wm, boolean isActivate,
      WMFullResourcePlan appliedResourcePlan) throws HiveException {
    String name = null;
    if (isActivate) {
      name = appliedResourcePlan.getPlan().getName();
      LOG.info("Activating a new resource plan " + name + ": " + appliedResourcePlan);
    } else {
      LOG.info("Disabling workload management");
    }

    if (wm != null) {
      // Note: as per our current constraints, the behavior of two parallel activates is
      //       undefined; although only one will succeed and the other will receive exception.
      //       We need proper (semi-)transactional modifications to support this without hacks.
      ListenableFuture<Boolean> future = wm.updateResourcePlanAsync(appliedResourcePlan);
      boolean isOk = false;
      try {
        // Note: we may add an async option in future. For now, let the task fail for the user.
        future.get();
        isOk = true;
        if (isActivate) {
          LOG.info("Successfully activated resource plan " + name);
        } else {
          LOG.info("Successfully disabled workload management");
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new HiveException(e);
      } finally {
        if (!isOk) {
          if (isActivate) {
            LOG.error("Failed to activate resource plan " + name);
          } else {
            LOG.error("Failed to disable workload management");
          }
          // TODO: shut down HS2?
        }
      }
    }

    TezSessionPoolManager pm = TezSessionPoolManager.getInstance();
    if (pm != null) {
      Collection<String> appliedTriggers = pm.updateTriggers(appliedResourcePlan);
      LOG.info("Updated tez session pool manager with active resource plan: {} appliedTriggers: {}", name,
          appliedTriggers);
    }

    return 0;
  }
}
