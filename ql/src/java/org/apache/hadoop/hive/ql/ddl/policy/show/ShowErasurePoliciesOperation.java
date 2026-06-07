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

package org.apache.hadoop.hive.ql.ddl.policy.show;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyVersion;
import org.apache.hadoop.hive.metastore.api.PolicyInfo;
import org.apache.hadoop.hive.metastore.api.PolicyVersionStatus;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.IOUtils;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Operation process of showing erasure policies. Emits one row per policy with
 * its name, the label of the currently {@code ACTIVE} version, and a compact
 * {@code label(STATUS)} view of every version. The raw {@code .erp} source is
 * deliberately not listed here (it is multi-line and would corrupt the row/column
 * layout); use {@code DESCRIBE ERASURE POLICY} to read a policy's source.
 */
public class ShowErasurePoliciesOperation extends DDLOperation<ShowErasurePoliciesDesc> {
  public ShowErasurePoliciesOperation(DDLOperationContext context, ShowErasurePoliciesDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    final Hive db = context.getDb();
    final List<PolicyInfo> policies = db.getAllErasurePolicyNames();
    LOG.info("Found {} policies.", policies.size());

    final List<ErasurePolicySummary> summaries = new ArrayList<>(policies.size());
    for (PolicyInfo policy : policies) {
      summaries.add(summarize(db, policy.getName()));
    }

    // write the results in the file
    DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context);
    try {
      ShowErasurePoliciesFormatter formatter = ShowErasurePoliciesFormatter.getFormatter(context.getConf());
      formatter.showErasurePolicies(outStream, summaries);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "show policies");
    } finally {
      IOUtils.closeStream(outStream);
    }

    return 0;
  }

  /**
   * Collapse a policy's version journal into a single listing row: the active
   * version label (or {@code -}) and an oldest-first {@code label(STATUS)} list.
   */
  private static ErasurePolicySummary summarize(Hive db, String policyName) throws HiveException {
    final List<ErasurePolicyVersion> versions = db.listErasurePolicyVersions(policyName);
    if (versions == null || versions.isEmpty()) {
      return new ErasurePolicySummary(policyName, "-", "-");
    }
    versions.sort(Comparator.comparingLong(ErasurePolicyVersion::getVersionId));

    String active = "-";
    final StringBuilder summary = new StringBuilder();
    for (ErasurePolicyVersion v : versions) {
      if (summary.length() > 0) {
        summary.append(", ");
      }
      summary.append(v.getVersionLabel()).append('(').append(v.getStatus()).append(')');
      if (v.getStatus() == PolicyVersionStatus.ACTIVE) {
        active = v.getVersionLabel();
      }
    }
    return new ErasurePolicySummary(policyName, active, summary.toString());
  }
}
