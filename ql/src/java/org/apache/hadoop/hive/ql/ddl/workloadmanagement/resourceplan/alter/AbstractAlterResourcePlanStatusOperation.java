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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.alter;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlanStatus;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Abstract ancestor of the enable / disable Resource Plan operations.
 */
public abstract class AbstractAlterResourcePlanStatusOperation<T extends DDLDesc> extends DDLOperation<T> {

  public AbstractAlterResourcePlanStatusOperation(DDLOperationContext context, T desc) {
    super(context, desc);
  }

  protected void handleWMServiceChangeIfNeeded(WMFullResourcePlan appliedResourcePlan, boolean isActivate,
      boolean isForceDeactivate, boolean replace) throws HiveException {
    boolean mustHaveAppliedChange = isActivate || isForceDeactivate;
    if (!mustHaveAppliedChange && !replace) {
      return; // The modification cannot affect an active plan.
    }
    if (appliedResourcePlan == null && !mustHaveAppliedChange) {
      return; // Replacing an inactive plan.
    }

    WorkloadManager wm = WorkloadManager.getInstance();
    boolean isInTest = HiveConf.getBoolVar(context.getConf(), ConfVars.HIVE_IN_TEST);
    if (wm == null && isInTest) {
      return; // Skip for tests if WM is not present.
    }

    if ((appliedResourcePlan == null) != isForceDeactivate) {
      throw new HiveException("Cannot get a resource plan to apply; or non-null plan on disable");
      // TODO: shut down HS2?
    }
    assert appliedResourcePlan == null || appliedResourcePlan.getPlan().getStatus() == WMResourcePlanStatus.ACTIVE;

    handleWMServiceChange(wm, isActivate, appliedResourcePlan);
  }

  private int handleWMServiceChange(WorkloadManager wm, boolean isActivate,
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
